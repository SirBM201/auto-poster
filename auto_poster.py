import os
import logging
import time
from datetime import date
from typing import Optional, List, Dict

import boto3
import requests
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# ========================
# LOAD .env FIRST
# ========================
load_dotenv()  # Loads variables from .env into the environment

# ========================
# LOGGING CONFIG
# ========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ========================
# FEATURE TOGGLES
# ========================
YOUTUBE_ENABLED = True         # YouTube upload ENABLED
FACEBOOK_ENABLED = True        # Facebook Page upload ENABLED
INSTAGRAM_ENABLED = True       # Instagram upload ENABLED

# Automatic retry settings (for all platforms)
RETRY_ENABLED = True
MAX_RETRIES = 2                # 1st attempt + 1 retry
RETRY_DELAY_SECONDS = 60       # wait between retries

# ========================
# ENV CONFIG
# ========================

# AWS
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION_NAME = os.getenv("AWS_REGION_NAME", "us-east-1")

# If S3_BUCKET_NAME is not set, default to your known bucket
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME") or "fair-video-source"

# YouTube
YOUTUBE_CLIENT_SECRETS_FILE = os.getenv("YOUTUBE_CLIENT_SECRETS_FILE", "client_secret.json")
YOUTUBE_CREDENTIALS_FILE = os.getenv("YOUTUBE_CREDENTIALS_FILE", "youtube_token.json")
YOUTUBE_MADE_FOR_KIDS = os.getenv("YOUTUBE_MADE_FOR_KIDS", "false").lower() == "true"

# Instagram / Facebook

# Try IG_ACCESS_TOKEN first, else fall back to META_ACCESS_TOKEN (from get_token.py)
IG_ACCESS_TOKEN = os.getenv("IG_ACCESS_TOKEN") or os.getenv("META_ACCESS_TOKEN")

# Try several possible variable names for your IG User ID (whichever exists in .env)
IG_USER_ID = (
    os.getenv("IG_USER_ID")
    or os.getenv("INSTAGRAM_USER_ID")
    or os.getenv("IG_BUSINESS_ID")
    or os.getenv("IG_ACCOUNT_ID")
    or os.getenv("META_IG_USER_ID")
)

# Page token: explicit FB_PAGE_ACCESS_TOKEN or fallback to META_ACCESS_TOKEN
FB_PAGE_ACCESS_TOKEN = os.getenv("FB_PAGE_ACCESS_TOKEN") or os.getenv("META_ACCESS_TOKEN")
FB_PAGE_ID = os.getenv("FB_PAGE_ID")

# WhatsApp alert webhook (e.g. Make.com webhook URL)
WHATSAPP_WEBHOOK_URL = os.getenv("WHATSAPP_WEBHOOK_URL")

# Debug logs (helpful to confirm wiring)
logger.info(f"IG_ACCESS_TOKEN loaded: {'YES' if IG_ACCESS_TOKEN else 'NO'}")
logger.info(f"IG_USER_ID loaded: {IG_USER_ID if IG_USER_ID else 'MISSING'}")
logger.info(f"FB_PAGE_ID loaded: {FB_PAGE_ID if FB_PAGE_ID else 'MISSING'}")
logger.info(f"S3_BUCKET_NAME: {S3_BUCKET_NAME}")


# ========================
# S3 HELPERS
# ========================
def get_s3_client():
    """
    Create an S3 client.

    - If AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY are set (from .env), use them.
    - Otherwise, fall back to default AWS credentials (shared config).
    """
    if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
        session = boto3.Session(
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION_NAME,
        )
        return session.client("s3")
    else:
        return boto3.client("s3", region_name=AWS_REGION_NAME)


def list_mp4_objects(s3_client, bucket: str, prefix: str) -> List[dict]:
    """
    List all .mp4 objects under a given prefix.
    """
    logger.info(f"[] Looking for objects under prefix '{prefix}'")
    paginator = s3_client.get_paginator("list_objects_v2")

    objs = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            key = item["Key"]
            if key.lower().endswith(".mp4"):
                objs.append(item)

    return objs


def pick_todays_or_latest(objs: List[dict], today_str: str, slot_name: str) -> Optional[dict]:
    """
    From the list of S3 objects:
    - Try to pick today's file: key contains `"{today_str} - "` and endswith .mp4
    - Else pick the most recently modified object.
    """
    if not objs:
        return None

    # Try today's file
    for obj in objs:
        key = obj["Key"]
        if f"{today_str} - " in key and key.lower().endswith(".mp4"):
            logger.info(f"[{slot_name}] Found today's file: {key}")
            return obj

    # Fallback: choose the latest by LastModified
    latest = max(objs, key=lambda x: x["LastModified"])
    logger.info(
        f"[{slot_name}] Today's file not found. Using latest file instead: {latest['Key']}"
    )
    return latest


def download_s3_object(s3_client, bucket: str, key: str, local_dir: str = "videos") -> str:
    """
    Download S3 object to local /videos folder and return local path.
    """
    os.makedirs(local_dir, exist_ok=True)
    filename = os.path.basename(key)
    local_path = os.path.join(local_dir, filename)

    # Remove local file first if exists
    if os.path.exists(local_path):
        logger.info(f"Removed existing local file before download: {local_path}")
        os.remove(local_path)

    logger.info(f"Downloading S3 object '{key}' to '{local_path}'")
    s3_client.download_file(bucket, key, local_path)
    logger.info("Download complete")
    return local_path


def generate_s3_presigned_url(s3_client, bucket: str, key: str, expires_in: int = 3600) -> str:
    """
    Generate a pre-signed URL for S3 object.
    """
    logger.info("Generating pre-signed S3 URL for Instagram...")
    try:
        url = s3_client.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket, "Key": key},
            ExpiresIn=expires_in,
        )
        logger.info("Pre-signed URL generated.")
        return url
    except ClientError as e:
        logger.error(f"Error generating pre-signed URL: {e}")
        raise


# ========================
# METADATA PARSING
# ========================
def parse_metadata_from_filename(filename: str) -> dict:
    """
    Expected filename format:
      'YYYY-MM-DD - Title goes here.mp4'
    Returns dict: { 'title': ..., 'caption': ..., 'date': ... }
    """
    base = os.path.basename(filename)
    if base.lower().endswith(".mp4"):
        base = base[:-4]  # strip .mp4

    # Split on ' - ' once
    try:
        date_str, title = base.split(" - ", 1)
    except ValueError:
        # Fallback: no date, just use full name
        date_str = date.today().strftime("%Y-%m-%d")
        title = base

    caption = f"{title} | {date_str}"

    metadata = {
        "title": title,
        "caption": caption,
        "date": date_str,
    }
    logger.info(f"Metadata from filename: {metadata}")
    return metadata


# ========================
# RETRY WRAPPER
# ========================
def run_with_retries(
    name: str,
    slot_name: str,
    func,
    max_retries: int = MAX_RETRIES,
    delay_seconds: int = RETRY_DELAY_SECONDS,
) -> bool:
    """
    Generic retry wrapper for platform uploads.
    """
    if not RETRY_ENABLED:
        return func()

    attempt = 1
    while attempt <= max_retries:
        ok = func()
        if ok:
            if attempt > 1:
                logger.info(f"[{slot_name}] {name} succeeded on retry {attempt}.")
            return True

        if attempt == max_retries:
            logger.warning(f"[{slot_name}] {name} failed after {max_retries} attempts.")
            return False

        logger.warning(
            f"[{slot_name}] {name} failed on attempt {attempt}. "
            f"Retrying in {delay_seconds}s..."
        )
        time.sleep(delay_seconds)
        attempt += 1

    return False


# ========================
# YOUTUBE UPLOAD
# ========================
def get_youtube_service():
    """
    Build an authenticated YouTube Data API client.

    Requires:
      - google-api-python-client
      - google-auth-oauthlib
      - google-auth-httplib2
    And files:
      - client_secret.json (or YOUTUBE_CLIENT_SECRETS_FILE)
      - youtube_token.json (or YOUTUBE_CREDENTIALS_FILE)
    """
    try:
        from googleapiclient.discovery import build
        from google_auth_oauthlib.flow import InstalledAppFlow
        from google.auth.transport.requests import Request
        import google.oauth2.credentials
    except ImportError as e:
        raise RuntimeError(
            "YouTube libraries not installed. Run:\n"
            "  pip install google-api-python-client google-auth-oauthlib google-auth-httplib2"
        ) from e

    scopes = ["https://www.googleapis.com/auth/youtube.upload"]
    creds = None

    if os.path.exists(YOUTUBE_CREDENTIALS_FILE):
        import google.oauth2.credentials

        creds = google.oauth2.credentials.Credentials.from_authorized_user_file(
            YOUTUBE_CREDENTIALS_FILE, scopes
        )

    if not creds or not creds.valid:
        from google.auth.exceptions import RefreshError

        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
            except RefreshError as e:
                logger.warning(f"Failed to refresh YouTube credentials: {e}")
                creds = None

        if not creds:
            flow = InstalledAppFlow.from_client_secrets_file(
                YOUTUBE_CLIENT_SECRETS_FILE, scopes
            )
            # For first-time setup this will prompt you once in console/browser
            creds = flow.run_console()

        with open(YOUTUBE_CREDENTIALS_FILE, "w") as token:
            token.write(creds.to_json())

    from googleapiclient.discovery import build as build_service

    return build_service("youtube", "v3", credentials=creds)


def upload_to_youtube(video_path: str, title: str, description: str, slot_name: str) -> bool:
    """
    Upload a video to YouTube channel.
    """
    if not YOUTUBE_ENABLED:
        logger.info(f"[{slot_name}] YouTube upload is DISABLED by config.")
        return False

    try:
        service = get_youtube_service()
    except Exception as e:
        logger.error(f"[{slot_name}] Failed to initialise YouTube client: {e}")
        return False

    try:
        from googleapiclient.http import MediaFileUpload
        from googleapiclient.errors import HttpError
    except ImportError as e:
        logger.error(
            f"[{slot_name}] google-api-python-client not installed. "
            "Run: pip install google-api-python-client"
        )
        return False

    body = {
        "snippet": {
            "title": title,
            "description": description,
            "categoryId": "22",  # People & Blogs (adjust if you like)
        },
        "status": {
            "privacyStatus": "public",
            "selfDeclaredMadeForKids": bool(YOUTUBE_MADE_FOR_KIDS),
        },
    }

    media = MediaFileUpload(
        video_path, chunksize=-1, resumable=True, mimetype="video/*"
    )

    logger.info(f"[{slot_name}] Uploading to YouTube...")
    try:
        request = service.videos().insert(
            part="snippet,status",
            body=body,
            media_body=media,
        )

        response = None
        while response is None:
            status, response = request.next_chunk()
            if status:
                pct = int(status.progress() * 100)
                logger.info(f"[{slot_name}] YouTube upload progress: {pct}%")

        video_id = response.get("id")
        logger.info(f"[{slot_name}] YouTube upload success — ID: {video_id}")
        return True

    except HttpError as e:
        logger.error(f"[{slot_name}] YouTube upload HTTP error: {e}")
        return False
    except Exception as e:
        logger.error(f"[{slot_name}] YouTube upload exception: {e}")
        return False


# ========================
# FACEBOOK UPLOAD
# ========================
def upload_to_facebook_page(video_path: str, title: str, description: str, slot_name: str) -> bool:
    """
    Uploads video to Facebook Page.
    """
    if not FACEBOOK_ENABLED:
        logger.info(f"[{slot_name}] Facebook upload is DISABLED by config.")
        return False

    if not FB_PAGE_ACCESS_TOKEN or not FB_PAGE_ID:
        logger.error(f"[{slot_name}] FB_PAGE_ACCESS_TOKEN or FB_PAGE_ID not set. Skipping Facebook.")
        return False

    url = f"https://graph.facebook.com/v21.0/{FB_PAGE_ID}/videos"
    params = {
        "title": title,
        "description": description,
        "access_token": FB_PAGE_ACCESS_TOKEN,
    }

    logger.info(f"[{slot_name}] Uploading to Facebook Page ID {FB_PAGE_ID}...")
    try:
        with open(video_path, "rb") as f:
            files = {"source": f}
            resp = requests.post(url, params=params, files=files, timeout=600)
        data = resp.json()

        if resp.status_code != 200 or "error" in data:
            logger.error(
                f"[{slot_name}] Facebook failed — full error: {data}"
            )
            return False

        video_id = data.get("id")
        logger.info(f"[{slot_name}] Facebook upload success — Video ID: {video_id}")
        return True

    except Exception as e:
        logger.error(f"[{slot_name}] Facebook upload exception: {e}")
        return False


# ========================
# INSTAGRAM HELPERS
# ========================
def create_ig_container(
    video_url: str,
    caption: str,
    access_token: str,
    ig_user_id: str,
    share_to_feed: bool = True,
) -> str:
    """
    Creates an Instagram Reels media container.
    """
    if not INSTAGRAM_ENABLED:
        raise RuntimeError("Instagram is disabled by config (INSTAGRAM_ENABLED = False).")

    endpoint = f"https://graph.facebook.com/v21.0/{ig_user_id}/media"
    payload = {
        "media_type": "REELS",
        "video_url": video_url,
        "caption": caption,
        "share_to_feed": "true" if share_to_feed else "false",
        "access_token": access_token,
    }

    resp = requests.post(endpoint, data=payload, timeout=60)
    data = resp.json()

    if resp.status_code != 200 or "error" in data:
        raise RuntimeError(f"Error creating IG container: {data}")

    return data["id"]


def publish_ig_media(creation_id: str, access_token: str, ig_user_id: str) -> str:
    """
    Publish a previously created IG media container.
    """
    endpoint = f"https://graph.facebook.com/v21.0/{ig_user_id}/media_publish"
    payload = {
        "creation_id": creation_id,
        "access_token": access_token,
    }

    resp = requests.post(endpoint, data=payload, timeout=60)
    data = resp.json()

    if resp.status_code != 200 or "error" in data:
        raise RuntimeError(f"Error publishing IG media: {data}")

    return data["id"]


def poll_ig_container_status(ig_creation_id: str, access_token: str, slot_name: str) -> str:
    """
    Polls the IG container until it's FINISHED, ERROR, or we hit our max wait time.
    Returns the final status_code from IG (e.g., FINISHED, ERROR, TIMEOUT).
    """

    # 10 minutes total wait time: 120 checks × 5 seconds = 600 seconds
    max_checks = 120
    delay_seconds = 5

    for i in range(max_checks):
        elapsed = i * delay_seconds

        status_url = (
            f"https://graph.facebook.com/v21.0/{ig_creation_id}"
            f"?fields=status_code,status&access_token={access_token}"
        )

        try:
            resp = requests.get(status_url, timeout=30)
            data = resp.json()
        except Exception as e:
            logger.error(f"[{slot_name}] Error querying IG status: {e}")
            time.sleep(delay_seconds)
            continue

        status_code = data.get("status_code")
        status_msg = data.get("status")

        logger.info(
            f"IG status check ({elapsed}s): status_code={status_code}, status={status_msg}"
        )

        if status_code == "FINISHED":
            return "FINISHED"

        if status_code == "ERROR":
            logger.error(
                f"[{slot_name}] Instagram reported ERROR for container {ig_creation_id}: "
                f"{status_msg}"
            )
            return "ERROR"

        # Otherwise it's typically IN_PROGRESS; keep waiting
        time.sleep(delay_seconds)

    total_seconds = max_checks * delay_seconds
    logger.error(
        f"[{slot_name}] Instagram media still not ready after {total_seconds} seconds "
        f"({total_seconds // 60} minutes). Giving up."
    )
    return "TIMEOUT"


def upload_to_instagram_reels(
    s3_client,
    bucket: str,
    key: str,
    caption: str,
    slot_name: str,
) -> bool:
    """
    Full IG flow: presigned URL -> container -> poll -> publish.
    """
    if not INSTAGRAM_ENABLED:
        logger.info(f"[{slot_name}] Instagram upload is DISABLED by config.")
        return False

    if not IG_ACCESS_TOKEN or not IG_USER_ID:
        logger.error(f"[{slot_name}] IG_ACCESS_TOKEN or IG_USER_ID not set. Skipping Instagram.")
        return False

    presigned_url = generate_s3_presigned_url(s3_client, bucket, key)
    logger.info(f"[{slot_name}] Creating Instagram Reels media container...")
    try:
        ig_creation_id = create_ig_container(
            video_url=presigned_url,
            caption=caption,
            access_token=IG_ACCESS_TOKEN,
            ig_user_id=IG_USER_ID,
        )
        logger.info(f"Instagram container created — ID: {ig_creation_id}")

        status_code = poll_ig_container_status(
            ig_creation_id=ig_creation_id,
            access_token=IG_ACCESS_TOKEN,
            slot_name=slot_name,
        )

        if status_code == "FINISHED":
            logger.info("Publishing Instagram media...")
            ig_media_id = publish_ig_media(
                creation_id=ig_creation_id,
                access_token=IG_ACCESS_TOKEN,
                ig_user_id=IG_USER_ID,
            )
            logger.info(f"Instagram PUBLISH success — ID: {ig_media_id}")
            return True
        elif status_code == "ERROR":
            logger.warning(f"[{slot_name}] Instagram upload failed — container reported ERROR.")
            return False
        else:  # TIMEOUT
            logger.warning(
                f"[{slot_name}] Instagram upload failed — container timed out "
                f"(still IN_PROGRESS after 10 minutes)."
            )
            return False

    except Exception as e:
        logger.error(f"[{slot_name}] Instagram exception: {e}")
        return False


# ========================
# S3 DELETE
# ========================
def delete_from_s3(s3_client, bucket: str, key: str, slot_name: str) -> None:
    try:
        s3_client.delete_object(Bucket=bucket, Key=key)
        logger.info(f"[{slot_name}] Deleted source file from S3: {key}")
    except ClientError as e:
        logger.error(f"[{slot_name}] Failed to delete from S3: {e}")


# ========================
# WHATSAPP ALERT VIA WEBHOOK
# ========================
def send_whatsapp_alert(message: str) -> None:
    """
    Sends a summary message to a webhook that you connect to WhatsApp
    (e.g., Make.com scenario calling WhatsApp Cloud API or Twilio).

    .env:
      WHATSAPP_WEBHOOK_URL=https://hook.make.com/your-scenario-id
    """
    if not WHATSAPP_WEBHOOK_URL:
        logger.info("WHATSAPP_WEBHOOK_URL not set; skipping WhatsApp alert.")
        return

    try:
        resp = requests.post(
            WHATSAPP_WEBHOOK_URL,
            json={"text": message},
            timeout=10,
        )
        if resp.status_code >= 200 and resp.status_code < 300:
            logger.info("WhatsApp alert sent via webhook.")
        else:
            logger.error(
                f"WhatsApp webhook returned status {resp.status_code}: {resp.text}"
            )
    except Exception as e:
        logger.error(f"Failed to send WhatsApp alert: {e}")


# ========================
# SLOT PROCESSOR
# ========================
def process_slot(
    slot_name: str,
    prefix: str,
    s3_client,
    bucket: str,
    today_str: str,
) -> Dict[str, str]:
    """
    Process a single slot.
    Returns a dict summarising platform results.
    """
    logger.info("====================================================================")
    logger.info(f"Starting slot: {slot_name}")

    result = {
        "slot": slot_name,
        "video_key": None,
        "youtube": "NOT_RUN",
        "facebook": "NOT_RUN",
        "instagram": "NOT_RUN",
    }

    objs = list_mp4_objects(s3_client, bucket, prefix)

    if not objs:
        logger.warning(f"[{slot_name}] No .mp4 files found in prefix '{prefix}'")
        logger.warning(f"[{slot_name}] Skipping slot — no video.")
        result["youtube"] = "SKIPPED_NO_VIDEO"
        result["facebook"] = "SKIPPED_NO_VIDEO"
        result["instagram"] = "SKIPPED_NO_VIDEO"
        return result

    obj = pick_todays_or_latest(objs, today_str, slot_name)
    if not obj:
        logger.warning(f"[{slot_name}] No suitable video found. Skipping.")
        result["youtube"] = "SKIPPED_NO_MATCH"
        result["facebook"] = "SKIPPED_NO_MATCH"
        result["instagram"] = "SKIPPED_NO_MATCH"
        return result

    key = obj["Key"]
    result["video_key"] = key

    local_path = download_s3_object(s3_client, bucket, key)
    meta = parse_metadata_from_filename(os.path.basename(local_path))

    title = meta["title"]
    caption = meta["caption"]

    # ----------------------
    # YOUTUBE
    # ----------------------
    if YOUTUBE_ENABLED:
        def yt_task():
            return upload_to_youtube(local_path, title, caption, slot_name)

        youtube_ok = run_with_retries("YouTube", slot_name, yt_task)
        result["youtube"] = "SUCCESS" if youtube_ok else "FAILED"
    else:
        logger.info(f"[{slot_name}] YouTube upload is DISABLED for now. Skipping YouTube for this slot.")
        result["youtube"] = "DISABLED"
        youtube_ok = False

    # ----------------------
    # FACEBOOK
    # ----------------------
    if FACEBOOK_ENABLED:
        def fb_task():
            return upload_to_facebook_page(local_path, title, caption, slot_name)

        fb_ok = run_with_retries("Facebook", slot_name, fb_task)
        result["facebook"] = "SUCCESS" if fb_ok else "FAILED"
    else:
        logger.info(f"[{slot_name}] Facebook upload is DISABLED for now.")
        result["facebook"] = "DISABLED"
        fb_ok = False

    # ----------------------
    # INSTAGRAM
    # ----------------------
    if INSTAGRAM_ENABLED:
        def ig_task():
            return upload_to_instagram_reels(s3_client, bucket, key, caption, slot_name)

        ig_ok = run_with_retries("Instagram", slot_name, ig_task)
        result["instagram"] = "SUCCESS" if ig_ok else "FAILED"
    else:
        logger.info(f"[{slot_name}] Instagram upload is DISABLED by config.")
        result["instagram"] = "DISABLED"
        ig_ok = False

    # ----------------------
    # S3 DELETION DECISION
    # ----------------------
    # Delete only if ALL enabled platforms succeeded
    all_required_ok = True
    if YOUTUBE_ENABLED and result["youtube"] != "SUCCESS":
        all_required_ok = False
    if FACEBOOK_ENABLED and result["facebook"] != "SUCCESS":
        all_required_ok = False
    if INSTAGRAM_ENABLED and result["instagram"] != "SUCCESS":
        all_required_ok = False

    if all_required_ok:
        logger.info(f"[{slot_name}] All required uploads succeeded. Deleting source file in S3.")
        delete_from_s3(s3_client, bucket, key, slot_name)
    else:
        logger.warning(f"[{slot_name}] One or more uploads failed. Keeping source file in S3.")

    return result


# ========================
# MAIN
# ========================
def format_summary(results: List[Dict[str, str]]) -> str:
    lines = []
    lines.append("Auto-Poster Summary")
    lines.append("====================")
    for r in results:
        lines.append(f"- {r['slot']}:")
        lines.append(f"    YouTube : {r['youtube']}")
        lines.append(f"    Facebook: {r['facebook']}")
        lines.append(f"    Instagram: {r['instagram']}")
        if r.get("video_key"):
            lines.append(f"    Video: {r['video_key']}")
        else:
            lines.append("    Video: (none)")
    return "\n".join(lines)


def main():
    s3_client = get_s3_client()
    today_str = date.today().strftime("%Y-%m-%d")

    # Slot configuration matches your prefixes
    slots = [
        {
            "name": "reel_9am (short)",
            "prefix": "reels n shorts/9am content/",
        },
        {
            "name": "reel_4pm (short)",
            "prefix": "reels n shorts/4pm content/",
        },
        {
            "name": "std_9_30am (standard)",
            "prefix": "standard videos/9:30am content/",
        },
        {
            "name": "std_4_30pm (standard)",
            "prefix": "standard videos/4:30pm content/",
        },
    ]

    results = []
    for slot in slots:
        result = process_slot(
            slot_name=slot["name"],
            prefix=slot["prefix"],
            s3_client=s3_client,
            bucket=S3_BUCKET_NAME,
            today_str=today_str,
        )
        results.append(result)

    # Print summary in logs
    summary_text = format_summary(results)
    logger.info("====================================================================")
    logger.info("\n" + summary_text)

    # Send WhatsApp alert via webhook
    send_whatsapp_alert(summary_text)


if __name__ == "__main__":
    main()
