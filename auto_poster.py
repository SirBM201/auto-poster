import os
import sys
import logging
import time
import datetime
from typing import List, Dict, Any, Optional

import boto3
from botocore.exceptions import NoCredentialsError
import requests
from dotenv import load_dotenv

# --- Optional YouTube imports (used only if available) ---
try:
    from google.oauth2.credentials import Credentials
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaFileUpload

    YT_LIBS_AVAILABLE = True
except ImportError:
    YT_LIBS_AVAILABLE = False

# =========================================================
# LOGGING
# =========================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# =========================================================
# HELPERS
# =========================================================

def str_to_bool(value: str) -> bool:
    return str(value).lower() in ("1", "true", "yes", "y", "on")


def list_mp4_objects(s3_client, bucket: str, prefix: str) -> List[Dict[str, Any]]:
    """Return all .mp4 objects under a prefix, sorted by LastModified (newest first)."""
    objs: List[Dict[str, Any]] = []
    paginator = s3_client.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        contents = page.get("Contents", [])
        for obj in contents:
            key = obj["Key"]
            if key.lower().endswith(".mp4"):
                objs.append(obj)

    # newest first
    objs.sort(key=lambda o: o["LastModified"], reverse=True)
    return objs


def choose_today_or_latest(objs: List[Dict[str, Any]], today_str: str) -> Optional[Dict[str, Any]]:
    """Prefer today's file if present; otherwise return newest."""
    if not objs:
        return None

    for obj in objs:
        key = obj["Key"]
        if today_str in key:
            return obj

    # fallback: newest
    return objs[0]


def ensure_dir(path: str) -> None:
    if path:
        os.makedirs(path, exist_ok=True)


def download_s3_object(s3_client, bucket: str, key: str, local_path: str) -> None:
    ensure_dir(os.path.dirname(local_path))
    if os.path.exists(local_path):
        logging.info(f"Removed existing local file before download: {local_path}")
        os.remove(local_path)
    logging.info(f"Downloading S3 object '{key}' to '{local_path}'")
    s3_client.download_file(bucket, key, local_path)
    logging.info("Download complete")


def parse_metadata_from_key(key: str) -> Dict[str, str]:
    """Parse title, caption, and date from S3 key/filename."""
    filename = key.split("/")[-1]
    name_no_ext = os.path.splitext(filename)[0]

    # Expected pattern: 2025-11-30 - The Royal Accident - Episode 01
    date_part = name_no_ext[:10]
    title_part = name_no_ext
    if len(name_no_ext) > 13 and name_no_ext[10:13] == " - ":
        title_part = name_no_ext[13:]

    title = title_part
    date = date_part
    caption = f"{title} | {date}"

    return {"title": title, "caption": caption, "date": date}


def generate_presigned_url(s3_client, bucket: str, key: str, expires_in: int = 3600) -> str:
    url = s3_client.generate_presigned_url(
        "get_object", Params={"Bucket": bucket, "Key": key}, ExpiresIn=expires_in
    )
    return url


def retry_operation(name: str, func, max_retries: int, delay_seconds: int) -> bool:
    """Generic retry wrapper for a platform operation."""
    for attempt in range(1, max_retries + 1):
        try:
            success = func()
            if success:
                return True
            logging.warning(f"[{name}] Attempt {attempt} failed (returned False).")
        except Exception as e:
            logging.error(f"[{name}] Attempt {attempt} raised error: {e}")

        if attempt < max_retries:
            logging.info(f"[{name}] Retrying in {delay_seconds} seconds...")
            time.sleep(delay_seconds)

    logging.error(f"[{name}] All {max_retries} attempts failed.")
    return False


# =========================================================
# YOUTUBE
# =========================================================

def get_youtube_service() -> Optional[Any]:
    if not YT_LIBS_AVAILABLE:
        logging.error("YouTube libraries are not installed. Skipping YouTube upload.")
        return None

    cred_file = "credentials.json"
    if not os.path.exists(cred_file):
        logging.error("YouTube credentials.json not found. Run youtube_auth.py first.")
        return None

    scopes = ["https://www.googleapis.com/auth/youtube.upload"]
    creds = Credentials.from_authorized_user_file(cred_file, scopes=scopes)
    service = build("youtube", "v3", credentials=creds)
    return service


def upload_to_youtube(local_path: str, title: str, description: str) -> bool:
    service = get_youtube_service()
    if service is None:
        return False

    logging.info("Uploading to YouTube...")

    media = MediaFileUpload(local_path, chunksize=-1, resumable=True)

    body = {
        "snippet": {
            "title": title,
            "description": description,
            "categoryId": "22",  # People & Blogs (generic)
        },
        "status": {"privacyStatus": "public"},
    }

    request = service.videos().insert(
        part="snippet,status",
        body=body,
        media_body=media,
    )

    response = request.execute()
    video_id = response.get("id")
    logging.info(f"YouTube upload success — video ID: {video_id}")
    return True


# =========================================================
# FACEBOOK
# =========================================================

def upload_to_facebook_video(page_id: str, access_token: str, local_path: str, caption: str) -> bool:
    url = f"https://graph.facebook.com/v20.0/{page_id}/videos"
    logging.info("Uploading to Facebook Page...")

    with open(local_path, "rb") as f:
        files = {"source": f}
        data = {"access_token": access_token, "description": caption}
        resp = requests.post(url, files=files, data=data, timeout=600)

    if resp.status_code != 200:
        logging.error(
            f"Facebook failed — status={resp.status_code}, response={resp.text}"
        )
        return False

    logging.info(f"Facebook upload success — response: {resp.text}")
    return True


# =========================================================
# INSTAGRAM
# =========================================================

def instagram_reels_upload(
    ig_user_id: str,
    ig_access_token: str,
    video_url: str,
    caption: str,
    max_wait_seconds: int = 600,
) -> bool:
    """Upload a short Reel via pre-signed S3 URL."""
    create_url = f"https://graph.facebook.com/v20.0/{ig_user_id}/media"
    params = {
        "media_type": "REELS",
        "video_url": video_url,
        "caption": caption,
        "access_token": ig_access_token,
        "share_to_feed": "true",
    }

    logging.info("[Instagram REELS] Creating media container...")
    resp = requests.post(create_url, data=params, timeout=600)
    if resp.status_code != 200:
        logging.error(
            f"[Instagram REELS] Failed to create media container — status={resp.status_code}, response={resp.text}"
        )
        return False

    data = resp.json()
    creation_id = data.get("id")
    logging.info(f"[Instagram REELS] Container created — ID: {creation_id}")

    status_url = f"https://graph.facebook.com/v20.0/{creation_id}"
    start_time = time.time()

    while True:
        elapsed = time.time() - start_time
        if elapsed > max_wait_seconds:
            logging.error(
                f"[Instagram REELS] Media still not ready after {max_wait_seconds} seconds. Giving up."
            )
            return False

        resp = requests.get(
            status_url,
            params={"fields": "status_code,status", "access_token": ig_access_token},
            timeout=600,
        )

        if resp.status_code != 200:
            logging.error(
                f"[Instagram REELS] Status check failed — status={resp.status_code}, response={resp.text}"
            )
            time.sleep(5)
            continue

        status_data = resp.json()
        status_code = status_data.get("status_code")
        status_msg = status_data.get("status")

        logging.info(
            f"[Instagram REELS] Status ({int(elapsed)}s): status_code={status_code}, status={status_msg}"
        )

        if status_code == "FINISHED":
            break
        elif status_code == "ERROR":
            logging.error("[Instagram REELS] status_code=ERROR. Aborting.")
            return False

        time.sleep(5)

    publish_url = f"https://graph.facebook.com/v20.0/{ig_user_id}/media_publish"
    publish_params = {
        "creation_id": creation_id,
        "access_token": ig_access_token,
    }

    logging.info("[Instagram REELS] Publishing media...")
    publish_resp = requests.post(publish_url, data=publish_params, timeout=600)

    if publish_resp.status_code != 200:
        logging.error(
            f"[Instagram REELS] Publish failed — status={publish_resp.status_code}, response={publish_resp.text}"
        )
        return False

    logging.info(f"[Instagram REELS] PUBLISH success — response: {publish_resp.text}")
    return True


def instagram_feed_video_upload(
    ig_user_id: str,
    ig_access_token: str,
    video_url: str,
    caption: str,
    max_wait_seconds: int = 600,
) -> bool:
    """
    Upload a longer feed video (e.g. 4-minute) via pre-signed S3 URL.

    Uses media_type=VIDEO so it posts as a regular feed video instead of a Reel.
    """
    create_url = f"https://graph.facebook.com/v20.0/{ig_user_id}/media"
    params = {
        "media_type": "VIDEO",
        "video_url": video_url,
        "caption": caption,
        "access_token": ig_access_token,
    }

    logging.info("[Instagram FEED] Creating media container...")
    resp = requests.post(create_url, data=params, timeout=600)
    if resp.status_code != 200:
        logging.error(
            f"[Instagram FEED] Failed to create media container — status={resp.status_code}, response={resp.text}"
        )
        return False

    data = resp.json()
    creation_id = data.get("id")
    logging.info(f"[Instagram FEED] Container created — ID: {creation_id}")

    status_url = f"https://graph.facebook.com/v20.0/{creation_id}"
    start_time = time.time()

    while True:
        elapsed = time.time() - start_time
        if elapsed > max_wait_seconds:
            logging.error(
                f"[Instagram FEED] Media still not ready after {max_wait_seconds} seconds. Giving up."
            )
            return False

        resp = requests.get(
            status_url,
            params={"fields": "status_code,status", "access_token": ig_access_token},
            timeout=600,
        )

        if resp.status_code != 200:
            logging.error(
                f"[Instagram FEED] Status check failed — status={resp.status_code}, response={resp.text}"
            )
            time.sleep(5)
            continue

        status_data = resp.json()
        status_code = status_data.get("status_code")
        status_msg = status_data.get("status")

        logging.info(
            f"[Instagram FEED] Status ({int(elapsed)}s): status_code={status_code}, status={status_msg}"
        )

        if status_code == "FINISHED":
            break
        elif status_code == "ERROR":
            logging.error("[Instagram FEED] status_code=ERROR. Aborting.")
            return False

        time.sleep(5)

    publish_url = f"https://graph.facebook.com/v20.0/{ig_user_id}/media_publish"
    publish_params = {
        "creation_id": creation_id,
        "access_token": ig_access_token,
    }

    logging.info("[Instagram FEED] Publishing media...")
    publish_resp = requests.post(publish_url, data=publish_params, timeout=600)

    if publish_resp.status_code != 200:
        logging.error(
            f"[Instagram FEED] Publish failed — status={publish_resp.status_code}, response={publish_resp.text}"
        )
        return False

    logging.info(f"[Instagram FEED] PUBLISH success — response: {publish_resp.text}")
    return True


# =========================================================
# WHATSAPP / WEBHOOK ALERT
# =========================================================

def send_whatsapp_summary(webhook_url: str, summary_text: str) -> None:
    if not webhook_url:
        return
    try:
        resp = requests.post(webhook_url, json={"text": summary_text}, timeout=30)
        if 200 <= resp.status_code < 300:
            logging.info("WhatsApp / webhook alert sent successfully.")
        else:
            logging.error(
                f"WhatsApp / webhook alert failed — status={resp.status_code}, response={resp.text}"
            )
    except Exception as e:
        logging.error(f"Error sending WhatsApp / webhook alert: {e}")


# =========================================================
# SLOT PROCESSING
# =========================================================

def process_slot(
    slot_display_name: str,
    slot_short_name: str,
    prefix: str,
    kind: str,
    s3_client,
    bucket: str,
    today_str: str,
    youtube_enabled: bool,
    facebook_enabled: bool,
    instagram_enabled: bool,
    fb_page_id: str,
    fb_token: str,
    ig_user_id: str,
    ig_token: str,
    retry_enabled: bool,
    max_retries: int,
    retry_delay_seconds: int,
    ig_max_wait_seconds: int,
) -> str:
    """
    Returns status string:
      - "SUCCESS"
      - "SKIPPED (reason)"
      - "FAILED (reason)"
    """
    logging.info("=" * 67)
    logging.info(f"Starting slot: {slot_display_name}")

    # 1) List objects
    objs = list_mp4_objects(s3_client, bucket, prefix)
    logging.info(f"[] Looking for objects under prefix '{prefix}'")

    if not objs:
        logging.warning(
            f"[{slot_display_name}] No .mp4 files found in prefix '{prefix}'"
        )
        return "SKIPPED (no video)"

    # 2) Choose today's or latest
    chosen = choose_today_or_latest(objs, today_str)
    if chosen is None:
        logging.warning(f"[{slot_display_name}] No suitable object found.")
        return "SKIPPED (no suitable object)"

    key = chosen["Key"]
    if today_str in key:
        logging.info(f"[{slot_display_name}] Using today's file: {key}")
    else:
        logging.info(
            f"[{slot_display_name}] Today's file not found. Using latest file instead: {key}"
        )

    # 3) Download
    filename = key.split("/")[-1]
    local_path = os.path.join("videos", filename)
    download_s3_object(s3_client, bucket, key, local_path)

    # 4) Metadata
    meta = parse_metadata_from_key(key)
    title = meta["title"]
    caption = meta["caption"]

    logging.info(f"Metadata from filename: {meta}")

    all_ok = True
    reasons = []

    # 5) YouTube
    if youtube_enabled:
        logging.info(f"[{slot_display_name}] YouTube upload is ENABLED for this slot.")

        def yt_func():
            return upload_to_youtube(local_path, title, caption)

        if retry_enabled:
            yt_success = retry_operation(
                f"YouTube ({slot_short_name})",
                yt_func,
                max_retries,
                retry_delay_seconds,
            )
        else:
            yt_success = yt_func()

        if not yt_success:
            all_ok = False
            reasons.append("YouTube failed")
    else:
        logging.info(f"[{slot_display_name}] YouTube upload is DISABLED for this slot.")

    # 6) Facebook
    if facebook_enabled:
        if not fb_page_id or not fb_token:
            logging.error(
                f"[{slot_display_name}] FB_PAGE_ID or META_ACCESS_TOKEN not set. Skipping Facebook."
            )
            all_ok = False
            reasons.append("Facebook config missing")
        else:

            def fb_func():
                return upload_to_facebook_video(
                    fb_page_id, fb_token, local_path, caption
                )

            if retry_enabled:
                fb_success = retry_operation(
                    f"Facebook ({slot_short_name})",
                    fb_func,
                    max_retries,
                    retry_delay_seconds,
                )
            else:
                fb_success = fb_func()

            if not fb_success:
                all_ok = False
                reasons.append("Facebook failed")
    else:
        logging.info(
            f"[{slot_display_name}] Facebook upload is DISABLED for this slot."
        )

    # 7) Instagram
    if instagram_enabled:
        if not ig_user_id or not ig_token:
            logging.error(
                f"[{slot_display_name}] IG_USER_ID or IG_ACCESS_TOKEN not set. Skipping Instagram."
            )
            all_ok = False
            reasons.append("Instagram config missing")
        else:

            def ig_func():
                presigned_url = generate_presigned_url(
                    s3_client, bucket, key, expires_in=3600
                )
                logging.info("Pre-signed URL generated.")

                if kind == "short":
                    # Reels for short videos
                    return instagram_reels_upload(
                        ig_user_id,
                        ig_token,
                        presigned_url,
                        caption,
                        max_wait_seconds=ig_max_wait_seconds,
                    )
                else:
                    # Feed video for 4-minute standard videos
                    return instagram_feed_video_upload(
                        ig_user_id,
                        ig_token,
                        presigned_url,
                        caption,
                        max_wait_seconds=ig_max_wait_seconds,
                    )

            if retry_enabled:
                ig_success = retry_operation(
                    f"Instagram ({slot_short_name})",
                    ig_func,
                    max_retries,
                    retry_delay_seconds,
                )
            else:
                ig_success = ig_func()

            if not ig_success:
                all_ok = False
                reasons.append("Instagram failed")
    else:
        logging.info(
            f"[{slot_display_name}] Instagram upload is DISABLED for this slot."
        )

    # 8) Final status for this slot
    if all_ok:
        logging.info(f"[{slot_display_name}] All enabled uploads succeeded.")
        # We keep video in S3 for now.
        return "SUCCESS"
    else:
        reason_str = ", ".join(reasons) if reasons else "one or more uploads failed"
        logging.warning(
            f"[{slot_display_name}] One or more uploads failed ({reason_str}). Keeping source file in S3."
        )
        return f"FAILED ({reason_str})"


# =========================================================
# MAIN
# =========================================================

def main():
    load_dotenv()

    today_str = datetime.date.today().strftime("%Y-%m-%d")

    # --- AWS / S3 env ---
    aws_region = os.getenv("AWS_REGION_NAME", "us-east-1")
    bucket = os.getenv("S3_BUCKET_NAME")

    if not bucket:
        logging.error("S3_BUCKET_NAME is not set. Exiting.")
        sys.exit(1)

    try:
        s3_client = boto3.client("s3", region_name=aws_region)
    except NoCredentialsError:
        logging.error("AWS credentials not found. Exiting.")
        sys.exit(1)

    # --- META / IG / FB env ---
    meta_access_token = os.getenv("META_ACCESS_TOKEN")
    fb_page_id = os.getenv("FB_PAGE_ID") or os.getenv("META_PAGE_ID")
    ig_access_token = os.getenv("IG_ACCESS_TOKEN")
    ig_user_id = os.getenv("IG_USER_ID")

    # --- Feature toggles (default: ENABLED for YT + FB + IG) ---
    youtube_enabled = str_to_bool(os.getenv("YOUTUBE_ENABLED", "true"))
    facebook_enabled = str_to_bool(os.getenv("FACEBOOK_ENABLED", "true"))
    instagram_enabled = str_to_bool(os.getenv("INSTAGRAM_ENABLED", "true"))

    retry_enabled = str_to_bool(os.getenv("RETRY_ENABLED", "true"))
    max_retries = int(os.getenv("MAX_RETRIES", "2"))
    retry_delay_seconds = int(os.getenv("RETRY_DELAY_SECONDS", "60"))
    ig_max_wait_seconds = int(os.getenv("IG_MAX_WAIT_SECONDS", "600"))  # 10 mins

    # --- WhatsApp webhook (optional) ---
    whatsapp_webhook_url = os.getenv("WHATSAPP_WEBHOOK_URL", "").strip()

    # --- Slots configuration ---
    slots = [
        {
            "display_name": "reel_9am (short)",
            "short_name": "reel_9am",
            "prefix": "reels n shorts/9am content/",
            "kind": "short",
        },
        {
            "display_name": "reel_4pm (short)",
            "short_name": "reel_4pm",
            "prefix": "reels n shorts/4pm content/",
            "kind": "short",
        },
        {
            "display_name": "std_9_30am (standard)",
            "short_name": "std_9_30am",
            "prefix": "standard videos/9:30am content/",
            "kind": "standard",
        },
        {
            "display_name": "std_4_30pm (standard)",
            "short_name": "std_4_30pm",
            "prefix": "standard videos/4:30pm content/",
            "kind": "standard",
        },
    ]

    # --- SLOT_FILTER logic ---
    slot_filter = os.getenv("SLOT_FILTER", "").strip().lower()
    if slot_filter:
        filtered = [s for s in slots if s["short_name"].lower() == slot_filter]
        if not filtered:
            available = ", ".join(s["short_name"] for s in slots)
            logging.error(
                f"SLOT_FILTER='{slot_filter}' did not match any slot. "
                f"Available short names: {available}"
            )
            sys.exit(1)
        slots_to_run = filtered
        logging.info(
            f"SLOT_FILTER is set to '{slot_filter}'. Only this slot will be processed."
        )
    else:
        slots_to_run = slots
        logging.info("SLOT_FILTER not set. All slots will be processed.")

    slot_results: Dict[str, str] = {}

    for slot in slots_to_run:
        status = process_slot(
            slot_display_name=slot["display_name"],
            slot_short_name=slot["short_name"],
            prefix=slot["prefix"],
            kind=slot["kind"],
            s3_client=s3_client,
            bucket=bucket,
            today_str=today_str,
            youtube_enabled=youtube_enabled,
            facebook_enabled=facebook_enabled,
            instagram_enabled=instagram_enabled,
            fb_page_id=fb_page_id,
            fb_token=meta_access_token,
            ig_user_id=ig_user_id,
            ig_token=ig_access_token,
            retry_enabled=retry_enabled,
            max_retries=max_retries,
            retry_delay_seconds=retry_delay_seconds,
            ig_max_wait_seconds=ig_max_wait_seconds,
        )
        slot_results[slot["short_name"]] = status

    # --- Final summary ---
    logging.info("=" * 67)
    logging.info("RUN SUMMARY:")
    for short_name, status in slot_results.items():
        logging.info(f"  {short_name}: {status}")

    # --- WhatsApp / webhook alert ---
    if whatsapp_webhook_url:
        summary_lines = [f"{k}: {v}" for k, v in slot_results.items()]
        summary_text = "Auto Poster Run Summary:\n" + "\n".join(summary_lines)
        send_whatsapp_summary(whatsapp_webhook_url, summary_text)


if __name__ == "__main__":
    main()
