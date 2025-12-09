#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
AUTO POSTING ENGINE v2.5  — BM SAFE VERSION (ALL PLATFORMS + CLEANUP)
---------------------------------------------------------------------
- One script, multiple slots (reel_9am, reel_4pm, std_9_30am, std_4_30pm)
- S3 → Download
- Filename → Metadata (title/caption/date)
- YouTube upload (short + long)
- Facebook Page upload
- Instagram:
    - Shorts (slots type=short)  → Reels
    - Long (slots type=standard) → Feed video
- TikTok upload (short + up to 60 minutes, via tiktok_poster.py)
- After SUCCESS on all enabled platforms:
    - Copy S3 object to posted/<original_key>
    - Delete original object
- Slot filter via SLOT_FILTER env (for GitHub Actions cron)
- Cleanup posted/ objects older than 48 hours on each run

ENV RULES:
----------
CORE (REQUIRED for script to run):
    AWS_ACCESS_KEY_ID
    AWS_SECRET_ACCESS_KEY
    AWS_REGION_NAME
    S3_BUCKET_NAME

PLATFORM (OPTIONAL, enable each only when complete):

    YOUTUBE (direct from .env):
        YT_CLIENT_ID
        YT_CLIENT_SECRET
        YT_REFRESH_TOKEN

    FACEBOOK:
        META_ACCESS_TOKEN
        FB_PAGE_ID

    INSTAGRAM:
        IG_ACCESS_TOKEN
        IG_USER_ID   (or INSTAGRAM_USER_ID)

    TIKTOK:
        TIKTOK_ACCESS_TOKEN           (used by tiktok_poster.py)
        TIKTOK_ENABLED=true|false     (toggle in this engine)

OPTIONAL:
    SLACK_WEBHOOK_URL
    WHATSAPP_WEBHOOK_URL
    SLOT_FILTER   (e.g., reel_9am)
"""

import os
import re
import time
import logging
from datetime import date, datetime, timezone, timedelta

import boto3
from botocore.exceptions import ClientError
import requests
from dotenv import load_dotenv

from fb_chunk_upload import fb_chunk_upload  # Facebook Page upload helper
from tiktok_poster import post_video_to_tiktok, TikTokError  # TikTok uploader

# ------------------------------------------------
# Load .env (LOCAL ONLY — in GitHub, secrets are injected directly)
# ------------------------------------------------
load_dotenv()

# ------------------------------------------------
# Logging configuration
# ------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
LOGGER = logging.getLogger("auto_poster")

# ------------------------------------------------
# YouTube robustness settings
# ------------------------------------------------
MAX_YT_RETRIES = 3          # how many times to retry YouTube upload
YT_RETRY_SLEEP = 20         # seconds to wait between retries

# ------------------------------------------------
# Environment helpers
# ------------------------------------------------
def env(key: str, default: str = None):
    """Simple env getter with optional default."""
    val = os.getenv(key)
    return val if val is not None else default


# Core AWS/S3 envs (required)
AWS_ACCESS_KEY_ID = env("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = env("AWS_SECRET_ACCESS_KEY")
AWS_REGION_NAME = env("AWS_REGION_NAME")
S3_BUCKET_NAME = env("S3_BUCKET_NAME")

# YouTube (optional - direct from .env)
YT_CLIENT_ID = env("YT_CLIENT_ID")
YT_CLIENT_SECRET = env("YT_CLIENT_SECRET")
YT_REFRESH_TOKEN = env("YT_REFRESH_TOKEN")

# Facebook (optional)
META_ACCESS_TOKEN = env("META_ACCESS_TOKEN")
FB_PAGE_ID = env("FB_PAGE_ID")

# Instagram (optional)
IG_ACCESS_TOKEN = env("IG_ACCESS_TOKEN")
IG_USER_ID = env("IG_USER_ID") or env("INSTAGRAM_USER_ID")

# TikTok (via tiktok_poster.py)
TIKTOK_ACCESS_TOKEN = env("TIKTOK_ACCESS_TOKEN")
TIKTOK_ENABLED_FLAG = env("TIKTOK_ENABLED", "false").strip().lower() == "true"

# Optional alerts (reserved for future use)
SLACK_WEBHOOK_URL = env("SLACK_WEBHOOK_URL")
WHATSAPP_WEBHOOK_URL = env("WHATSAPP_WEBHOOK_URL")

# Slot filter
SLOT_FILTER = env("SLOT_FILTER")  # e.g. "reel_9am"


def validate_core_env_or_exit():
    """Ensure AWS/S3 core variables exist. Exit if missing."""
    missing = []
    if not AWS_ACCESS_KEY_ID:
        missing.append("AWS_ACCESS_KEY_ID")
    if not AWS_SECRET_ACCESS_KEY:
        missing.append("AWS_SECRET_ACCESS_KEY")
    if not AWS_REGION_NAME:
        missing.append("AWS_REGION_NAME")
    if not S3_BUCKET_NAME:
        missing.append("S3_BUCKET_NAME")

    if missing:
        LOGGER.error("Missing required AWS/S3 env vars: %s", ", ".join(missing))
        raise SystemExit("Core AWS/S3 env vars missing. Exiting.")


def platform_status():
    """Determine which platforms are globally enabled based on envs."""
    yt_enabled = bool(YT_CLIENT_ID and YT_CLIENT_SECRET and YT_REFRESH_TOKEN)
    fb_enabled = bool(META_ACCESS_TOKEN and FB_PAGE_ID)
    ig_enabled = bool(IG_ACCESS_TOKEN and IG_USER_ID)
    tiktok_enabled = bool(TIKTOK_ACCESS_TOKEN and TIKTOK_ENABLED_FLAG)

    LOGGER.info("PLATFORM STATUS:")
    LOGGER.info("  YouTube:   %s", "ENABLED" if yt_enabled else "DISABLED (missing env)")
    LOGGER.info("  Facebook:  %s", "ENABLED" if fb_enabled else "DISABLED (missing env)")
    LOGGER.info("  Instagram: %s", "ENABLED" if ig_enabled else "DISABLED (missing env)")
    LOGGER.info(
        "  TikTok:    %s",
        "ENABLED" if tiktok_enabled else "DISABLED (no token or TIKTOK_ENABLED flag is false)",
    )

    return yt_enabled, fb_enabled, ig_enabled, tiktok_enabled


# ------------------------------------------------
# SLOTS (Plan A)
# ------------------------------------------------
SLOTS = [
    {
        "name": "reel_9am",
        "type": "short",
        "prefix": "reels n shorts/9am content/",
        "post_youtube": True,
        "post_facebook": True,
        "post_instagram": True,  # Reels
        "post_tiktok": True,     # Short to TikTok
    },
    {
        "name": "reel_4pm",
        "type": "short",
        "prefix": "reels n shorts/4pm content/",
        "post_youtube": True,
        "post_facebook": True,
        "post_instagram": True,  # Reels
        "post_tiktok": True,
    },
    {
        "name": "std_9_30am",
        "type": "standard",
        "prefix": "standard videos/9:30am content/",
        "post_youtube": True,
        "post_facebook": True,
        "post_instagram": True,  # Long → IG feed video
        "post_tiktok": True,
    },
    {
        "name": "std_4_30pm",
        "type": "standard",
        "prefix": "standard videos/4:30pm content/",
        "post_youtube": True,
        "post_facebook": True,
        "post_instagram": True,
        "post_tiktok": True,
    },
]


# ------------------------------------------------
# S3 helpers
# ------------------------------------------------
def build_s3_client():
    return boto3.client(
        "s3",
        region_name=AWS_REGION_NAME,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )


def get_latest_video_key(prefix: str):
    """
    Returns the latest .mp4 key under the prefix that contains TODAY's date
    (YYYY-MM-DD) in the filename. If none match, returns None.
    """
    s3 = build_s3_client()
    resp = s3.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=prefix)

    if "Contents" not in resp:
        LOGGER.warning("No objects found under prefix '%s'", prefix)
        return None

    today_str = str(date.today())  # 'YYYY-MM-DD'
    all_mp4s = [obj["Key"] for obj in resp["Contents"] if obj["Key"].lower().endswith(".mp4")]

    # Filter only files that have today's date in the basename
    mp4s_today = [
        key for key in all_mp4s
        if today_str in os.path.basename(key)
    ]

    if not mp4s_today:
        LOGGER.warning(
            "No .mp4 files for TODAY (%s) found under prefix '%s'. "
            "Existing or future-dated files are left untouched.",
            today_str,
            prefix,
        )
        return None

    mp4s_today.sort()
    latest_key = mp4s_today[-1]
    return latest_key


def download_s3_object(key: str):
    os.makedirs("videos", exist_ok=True)
    local_path = os.path.join("videos", os.path.basename(key))

    if os.path.exists(local_path):
        os.remove(local_path)

    LOGGER.info("Downloading S3 object '%s' to '%s'", key, local_path)
    s3 = build_s3_client()
    s3.download_file(S3_BUCKET_NAME, key, local_path)
    LOGGER.info("Download complete")
    return local_path


def archive_s3_object(key: str):
    """
    Safer archive:
    - Copy original object to posted/<original_key>
    - Delete original key
    Example:
      key = 'reels n shorts/9am content/2025-12-09 Heir vs. Beast.mp4'
      -> 'posted/reels n shorts/9am content/2025-12-09 Heir vs. Beast.mp4'
    """
    s3 = build_s3_client()
    dst_key = f"posted/{key}"

    LOGGER.info("Archiving S3 object: %s -> %s", key, dst_key)

    # Copy
    s3.copy_object(
        Bucket=S3_BUCKET_NAME,
        CopySource={"Bucket": S3_BUCKET_NAME, "Key": key},
        Key=dst_key,
    )

    # Delete original
    s3.delete_object(Bucket=S3_BUCKET_NAME, Key=key)
    LOGGER.info("Archive complete, original removed: %s", key)


# ------------------------------------------------
# Metadata extraction
# ------------------------------------------------
def parse_metadata_from_key(key: str):
    base = os.path.basename(key)
    date_match = re.search(r"\d{4}-\d{2}-\d{2}", base)
    if date_match:
        fdate = date_match.group(0)
    else:
        fdate = str(date.today())

    title = re.sub(r"\.mp4$", "", base)
    caption = f"{title} | {fdate}"

    return {
        "title": title,
        "caption": caption,
        "date": fdate,
    }


# ------------------------------------------------
# YouTube uploader (robust with retry + safe failure)
# ------------------------------------------------
def upload_to_youtube(file_path: str, meta: dict):
    """
    YouTube uploader with retry and safe failure.

    Expects these env vars:
      - YT_CLIENT_ID
      - YT_CLIENT_SECRET
      - YT_REFRESH_TOKEN

    Returns:
        True  -> upload succeeded
        False -> upload failed after retries or env/credentials issue
    """
    from google.oauth2.credentials import Credentials
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaFileUpload

    LOGGER.info("Uploading to YouTube...")

    if not (YT_CLIENT_ID and YT_CLIENT_SECRET and YT_REFRESH_TOKEN):
        LOGGER.error("YouTube env missing — cannot upload.")
        return False

    # Build credentials from refresh token
    creds = Credentials(
        None,
        refresh_token=YT_REFRESH_TOKEN,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=YT_CLIENT_ID,
        client_secret=YT_CLIENT_SECRET,
        scopes=["https://www.googleapis.com/auth/youtube.upload"],
    )

    try:
        youtube = build("youtube", "v3", credentials=creds)
    except Exception as e:
        LOGGER.error("YouTube: failed to build API client: %s", e, exc_info=True)
        return False

    body = {
        "snippet": {
            "title": meta["title"],
            "description": meta["caption"],
            "categoryId": "24",  # Entertainment
        },
        "status": {
            "privacyStatus": "public",
        },
    }

    media = MediaFileUpload(file_path, chunksize=-1, resumable=True)
    request = youtube.videos().insert(
        part="snippet,status",
        body=body,
        media_body=media,
    )

    response = None

    for attempt in range(1, MAX_YT_RETRIES + 1):
        LOGGER.info("YouTube: upload attempt %d/%d", attempt, MAX_YT_RETRIES)
        try:
            while response is None:
                status, response = request.next_chunk()
                if status is not None:
                    LOGGER.info("YouTube: upload progress %.2f%%", status.progress() * 100.0)

            if response and "id" in response:
                LOGGER.info("YouTube upload success — video ID: %s", response["id"])
                return True

            LOGGER.error("YouTube upload failed. Response: %s", response)
            return False

        except Exception as e:
            LOGGER.error(
                "YouTube: upload attempt %d failed: %s",
                attempt,
                e,
                exc_info=True,
            )

            if attempt >= MAX_YT_RETRIES:
                LOGGER.error(
                    "YouTube: giving up after %d attempts. "
                    "Continuing with Facebook / Instagram / TikTok.",
                    MAX_YT_RETRIES,
                )
                return False

            LOGGER.info(
                "YouTube: will retry in %d seconds (attempt %d/%d)...",
                YT_RETRY_SLEEP,
                attempt + 1,
                MAX_YT_RETRIES,
            )
            time.sleep(YT_RETRY_SLEEP)

    # Should not reach here, but keep safe
    return False


# ------------------------------------------------
# Facebook: Page upload (uses fb_chunk_upload helper)
# ------------------------------------------------
def upload_to_facebook(file_path: str, meta: dict, slot_name: str):
    if not META_ACCESS_TOKEN or not FB_PAGE_ID:
        LOGGER.info("[%s] Facebook disabled globally (env missing). Skipping.", slot_name)
        return False

    try:
        LOGGER.info("[%s] Uploading to Facebook Page via fb_chunk_upload...", slot_name)
        ok = fb_chunk_upload(
            file_path=file_path,
            page_id=FB_PAGE_ID,
            access_token=META_ACCESS_TOKEN,
            caption=meta["caption"],
        )
    except Exception as e:
        LOGGER.error("[%s] Facebook upload ERROR: %s", slot_name, e, exc_info=True)
        return False

    if ok:
        LOGGER.info("[%s] Facebook upload SUCCESS", slot_name)
        return True

    LOGGER.error("[%s] Facebook upload FAILED", slot_name)
    return False


# ------------------------------------------------
# Instagram: Reels (short only)
# ------------------------------------------------
def upload_to_instagram_reels(s3_key: str, meta: dict, slot: dict):
    """
    Uploads a short video as a Reel via pre-signed S3 URL.
    Uses: IG_ACCESS_TOKEN, IG_USER_ID
    Safe failure: Any exception returns False and does not crash script.
    """
    if slot["type"] != "short":
        LOGGER.info("[%s] Instagram Reels disabled for standard videos.", slot["name"])
        return False

    if not IG_ACCESS_TOKEN or not IG_USER_ID:
        LOGGER.info("[%s] Instagram disabled globally (env missing). Skipping.", slot["name"])
        return False

    try:
        s3 = build_s3_client()
        presigned_url = s3.generate_presigned_url(
            ClientMethod="get_object",
            Params={"Bucket": S3_BUCKET_NAME, "Key": s3_key},
            ExpiresIn=7200,
        )

        LOGGER.info("[%s] [Instagram REELS] Creating media container...", slot["name"])
        create_url = f"https://graph.facebook.com/v18.0/{IG_USER_ID}/media"

        payload = {
            "media_type": "REELS",
            "video_url": presigned_url,
            "caption": meta["caption"],
            "access_token": IG_ACCESS_TOKEN,
        }

        r = requests.post(create_url, data=payload)
        if r.status_code != 200:
            LOGGER.error("[%s] IG Reels container create FAILED: %s", slot["name"], r.text)
            return False

        cid = r.json().get("id")
        LOGGER.info("[%s] IG Reels container created — ID: %s", slot["name"], cid)

        # Poll status (up to ~1 minute)
        status_data = {}
        for sec in [0, 5, 11, 17, 23, 29, 35, 41, 47, 53]:
            time.sleep(6)
            status_url = f"https://graph.facebook.com/v18.0/{cid}"
            params = {
                "fields": "status_code,status",
                "access_token": IG_ACCESS_TOKEN,
            }
            sr = requests.get(status_url, params=params)
            try:
                status_data = sr.json()
            except Exception:
                status_data = {}
            LOGGER.info(
                "[%s] [Instagram REELS] Status (%ss): status_code=%s, status=%s",
                slot["name"],
                sec,
                status_data.get("status_code"),
                status_data.get("status"),
            )
            if status_data.get("status_code") == "FINISHED":
                break

        if status_data.get("status_code") != "FINISHED":
            LOGGER.error("[%s] IG Reels media not ready after polling. Giving up.", slot["name"])
            return False

        # Publish
        pub_url = f"https://graph.facebook.com/v18.0/{IG_USER_ID}/media_publish"
        pub_payload = {
            "creation_id": cid,
            "access_token": IG_ACCESS_TOKEN,
        }
        pr = requests.post(pub_url, data=pub_payload)
        if pr.status_code == 200:
            LOGGER.info("[%s] IG REELS PUBLISH success — %s", slot["name"], pr.text)
            return True

        LOGGER.error("[%s] IG REELS PUBLISH failed — %s", slot["name"], pr.text)
        return False

    except Exception as e:
        LOGGER.error("[%s] Instagram Reels upload ERROR: %s", slot["name"], e, exc_info=True)
        return False


# ------------------------------------------------
# Instagram: Feed video (long / standard slots)
# ------------------------------------------------
def upload_to_instagram_video(s3_key: str, meta: dict, slot: dict):
    """
    Uploads a long video as a normal Instagram video post via pre-signed S3 URL.
    Uses: IG_ACCESS_TOKEN, IG_USER_ID
    Safe failure: Any exception returns False and does not crash script.
    """
    if slot["type"] != "standard":
        LOGGER.info("[%s] Instagram long video is only used for standard slots.", slot["name"])
        return False

    if not IG_ACCESS_TOKEN or not IG_USER_ID:
        LOGGER.info("[%s] Instagram disabled globally (env missing). Skipping.", slot["name"])
        return False

    try:
        s3 = build_s3_client()
        presigned_url = s3.generate_presigned_url(
            ClientMethod="get_object",
            Params={"Bucket": S3_BUCKET_NAME, "Key": s3_key},
            ExpiresIn=7200,
        )

        LOGGER.info("[%s] [Instagram VIDEO] Creating media container...", slot["name"])
        create_url = f"https://graph.facebook.com/v18.0/{IG_USER_ID}/media"

        payload = {
            "media_type": "VIDEO",  # feed video
            "video_url": presigned_url,
            "caption": meta["caption"],
            "access_token": IG_ACCESS_TOKEN,
        }

        r = requests.post(create_url, data=payload)
        if r.status_code != 200:
            LOGGER.error("[%s] IG video container create FAILED: %s", slot["name"], r.text)
            return False

        cid = r.json().get("id")
        LOGGER.info("[%s] IG video container created — ID: %s", slot["name"], cid)

        # Poll status (up to ~1–2 minutes)
        status_data = {}
        for sec in [0, 7, 15, 23, 31, 39, 47, 55, 63, 71]:
            time.sleep(8)
            status_url = f"https://graph.facebook.com/v18.0/{cid}"
            params = {
                "fields": "status_code,status",
                "access_token": IG_ACCESS_TOKEN,
            }
            sr = requests.get(status_url, params=params)
            try:
                status_data = sr.json()
            except Exception:
                status_data = {}
            LOGGER.info(
                "[%s] [Instagram VIDEO] Status (%ss): status_code=%s, status=%s",
                slot["name"],
                sec,
                status_data.get("status_code"),
                status_data.get("status"),
            )
            if status_data.get("status_code") == "FINISHED":
                break

        if status_data.get("status_code") != "FINISHED":
            LOGGER.error("[%s] IG video media not ready after polling. Giving up.", slot["name"])
            return False

        # Publish
        pub_url = f"https://graph.facebook.com/v18.0/{IG_USER_ID}/media_publish"
        pub_payload = {
            "creation_id": cid,
            "access_token": IG_ACCESS_TOKEN,
        }
        pr = requests.post(pub_url, data=pub_payload)
        if pr.status_code == 200:
            LOGGER.info("[%s] IG VIDEO PUBLISH success — %s", slot["name"], pr.text)
            return True

        LOGGER.error("[%s] IG VIDEO PUBLISH failed — %s", slot["name"], pr.text)
        return False

    except Exception as e:
        LOGGER.error("[%s] Instagram VIDEO upload ERROR: %s", slot["name"], e, exc_info=True)
        return False


# ------------------------------------------------
# TikTok uploader wrapper (short + long)
# ------------------------------------------------
def upload_to_tiktok(local_path: str, meta: dict, slot_name: str):
    """
    Uploads a video file to TikTok using tiktok_poster.post_video_to_tiktok.
    Works for both short and standard videos (up to your account's max duration).
    Safe failure: Any exception returns False and does not crash script.
    """
    if not (TIKTOK_ACCESS_TOKEN and TIKTOK_ENABLED_FLAG):
        LOGGER.info("[%s] TikTok disabled globally (no token or TIKTOK_ENABLED=false).", slot_name)
        return False

    # Basic caption: title + date + hashtag
    caption = f"{meta['title']} | {meta['date']} #cre8studio"

    try:
        LOGGER.info("[%s] Uploading to TikTok...", slot_name)
        publish_id, status = post_video_to_tiktok(local_path, caption)
        LOGGER.info("[%s] TikTok upload SUCCESS — publish_id=%s status=%s", slot_name, publish_id, status)
        return True
    except TikTokError as e:
        LOGGER.error("[%s] TikTok upload FAILED: %s", slot_name, e)
        return False
    except Exception as e:
        LOGGER.error("[%s] TikTok unexpected error: %s", slot_name, e, exc_info=True)
        return False


# ------------------------------------------------
# Cleanup logic for posted/ objects older than X hours
# ------------------------------------------------
def cleanup_posted_objects(max_age_hours: int = 48):
    """
    Deletes objects under 'posted/' that are older than max_age_hours.
    Runs at the end of the script.
    """
    s3 = build_s3_client()
    cutoff = datetime.now(timezone.utc) - timedelta(hours=max_age_hours)
    prefix = "posted/"
    LOGGER.info("Cleaning up posted objects older than %d hours...", max_age_hours)

    continuation_token = None
    deleted = 0

    while True:
        kwargs = {"Bucket": S3_BUCKET_NAME, "Prefix": prefix}
        if continuation_token:
            kwargs["ContinuationToken"] = continuation_token

        resp = s3.list_objects_v2(**kwargs)
        contents = resp.get("Contents", [])

        for obj in contents:
            key = obj["Key"]
            last_modified = obj["LastModified"]  # timezone-aware UTC datetime
            if last_modified < cutoff:
                try:
                    s3.delete_object(Bucket=S3_BUCKET_NAME, Key=key)
                    deleted += 1
                    LOGGER.info("Deleted old posted object: %s", key)
                except Exception as e:
                    LOGGER.error("Failed to delete posted object '%s': %s", key, e, exc_info=True)

        if not resp.get("IsTruncated"):
            break

        continuation_token = resp.get("NextContinuationToken")

    LOGGER.info("Cleanup complete. Deleted %d old posted objects.", deleted)


# ------------------------------------------------
# Slot executor
# ------------------------------------------------
def run_slot(slot: dict, yt_enabled: bool, fb_enabled: bool, ig_enabled: bool, tiktok_enabled: bool):
    name = slot["name"]
    prefix = slot["prefix"]
    LOGGER.info("=" * 65)
    LOGGER.info("Starting slot: %s (%s)", name, slot["type"])

    s3_key = get_latest_video_key(prefix)
    if not s3_key:
        LOGGER.warning("[%s] No TODAY file found. Slot skipped.", name)
        return "SKIPPED"

    LOGGER.info("[%s] Using video: %s", name, s3_key)
    local_path = download_s3_object(s3_key)
    meta = parse_metadata_from_key(s3_key)

    all_ok = True

    # YouTube
    if slot.get("post_youtube") and yt_enabled:
        yt_ok = upload_to_youtube(local_path, meta)
        if not yt_ok:
            all_ok = False
    elif slot.get("post_youtube"):
        LOGGER.info("[%s] YouTube is disabled globally (env missing).", name)

    # Facebook
    if slot.get("post_facebook") and fb_enabled:
        fb_ok = upload_to_facebook(local_path, meta, name)
        if not fb_ok:
            all_ok = False
    elif slot.get("post_facebook"):
        LOGGER.info("[%s] Facebook is disabled globally (env missing).", name)

    # Instagram
    if slot.get("post_instagram") and ig_enabled:
        if slot["type"] == "short":
            ig_ok = upload_to_instagram_reels(s3_key, meta, slot)
        else:
            ig_ok = upload_to_instagram_video(s3_key, meta, slot)
        if not ig_ok:
            all_ok = False
    elif slot.get("post_instagram"):
        LOGGER.info("[%s] Instagram is disabled globally (env missing).", name)

    # TikTok
    if slot.get("post_tiktok") and tiktok_enabled:
        tt_ok = upload_to_tiktok(local_path, meta, name)
        if not tt_ok:
            all_ok = False
    elif slot.get("post_tiktok"):
        LOGGER.info("[%s] TikTok is disabled globally (env missing token or flag).", name)

    # Archive S3 object IF and ONLY IF all enabled platforms succeeded
    if all_ok:
        try:
            archive_s3_object(s3_key)
        except Exception as e:
            LOGGER.error("[%s] Failed to archive S3 object '%s': %s", name, s3_key, e, exc_info=True)
            all_ok = False

    status = "SUCCESS" if all_ok else "FAILED"
    return status


# ------------------------------------------------
# MAIN
# ------------------------------------------------
if __name__ == "__main__":
    # 1) Check core AWS envs
    validate_core_env_or_exit()

    LOGGER.info("Core AWS/S3 environment present.")
    if SLOT_FILTER:
        LOGGER.info("SLOT_FILTER active → will only run slot: %s", SLOT_FILTER)
    else:
        LOGGER.info("SLOT_FILTER not set. All slots will be processed.")

    # 2) Determine platform status
    yt_enabled, fb_enabled, ig_enabled, tiktok_enabled = platform_status()

    # 3) Run slots
    run_summary = {}

    for slot in SLOTS:
        if SLOT_FILTER and slot["name"] != SLOT_FILTER:
            continue

        status = run_slot(slot, yt_enabled, fb_enabled, ig_enabled, tiktok_enabled)
        run_summary[slot["name"]] = status

    # 4) Summary
    LOGGER.info("=" * 65)
    LOGGER.info("RUN SUMMARY:")
    for slot_name, status in run_summary.items():
        LOGGER.info("  %s: %s", slot_name, status)

    # 5) Cleanup posted/ older than 48 hours
    cleanup_posted_objects(max_age_hours=48)
