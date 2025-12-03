import os
import sys
import logging
from pathlib import Path

import requests
from dotenv import load_dotenv

# ---------------------------------------------------------
# 1. Setup logging and load .env
# ---------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Load .env from the same folder as this script
load_dotenv()

BASE_URL = "https://open.tiktokapis.com/v2"
TIKTOK_ACCESS_TOKEN = os.getenv("TIKTOK_ACCESS_TOKEN")


class TikTokError(Exception):
    """Custom exception for TikTok API errors."""
    pass


def _auth_headers_json():
    """
    Build headers for TikTok API calls that send/receive JSON.
    """
    if not TIKTOK_ACCESS_TOKEN:
        raise TikTokError(
            "TIKTOK_ACCESS_TOKEN is missing. "
            "Please check your .env file (same folder as this script)."
        )

    return {
        "Authorization": f"Bearer {TIKTOK_ACCESS_TOKEN}",
        "Content-Type": "application/json; charset=UTF-8",
    }


def get_creator_info():
    """
    Test call: fetch creator info.
    This is the first thing we do to confirm that the token is valid.
    """
    url = f"{BASE_URL}/post/publish/creator_info/query/"
    resp = requests.post(url, headers=_auth_headers_json())

    try:
        data = resp.json()
    except Exception:
        raise TikTokError(f"creator_info non-JSON response: {resp.text[:300]}")

    error = data.get("error", {})
    if error.get("code") != "ok":
        raise TikTokError(f"creator_info error: {data}")

    return data["data"]


def init_direct_post(video_path: str, title: str):
    """
    Step 1: ask TikTok for an upload URL + publish_id.
    """
    path = Path(video_path)
    if not path.is_file():
        raise TikTokError(f"Video file not found: {video_path}")

    file_size = path.stat().st_size

    payload = {
        "post_info": {
            "title": title[:150],  # caption max ~150 chars
            "privacy_level": "SELF_ONLY",
            "disable_duet": False,
            "disable_comment": False,
            "disable_stitch": False,
            "video_cover_timestamp_ms": 1000,
        },
        "source_info": {
            "source": "FILE_UPLOAD",
            "video_size": file_size,
            "chunk_size": file_size,   # single chunk upload
            "total_chunk_count": 1
        },
    }

    url = f"{BASE_URL}/post/publish/video/init/"
    resp = requests.post(url, headers=_auth_headers_json(), json=payload)

    try:
        data = resp.json()
    except Exception:
        raise TikTokError(f"init_direct_post non-JSON response: {resp.text[:300]}")

    error = data.get("error", {})
    if error.get("code") != "ok":
        raise TikTokError(f"init_direct_post error: {data}")

    publish_id = data["data"]["publish_id"]
    upload_url = data["data"]["upload_url"]
    return publish_id, upload_url, file_size


def upload_video_file(upload_url: str, video_path: str, file_size: int):
    """
    Step 2: upload the .mp4 file to TikTok using the upload_url.
    """
    logging.info("Uploading file to TikTok...")

    with open(video_path, "rb") as f:
        headers = {
            "Content-Type": "video/mp4",
            "Content-Range": f"bytes 0-{file_size - 1}/{file_size}",
        }
        resp = requests.put(upload_url, headers=headers, data=f)

    if not resp.ok:
        raise TikTokError(
            f"Upload failed: HTTP {resp.status_code} - {resp.text[:300]}"
        )

    logging.info("Upload completed with HTTP %s", resp.status_code)


def fetch_publish_status(publish_id: str):
    """
    Step 3: check publish status.
    """
    url = f"{BASE_URL}/post/publish/status/fetch/"
    payload = {"publish_id": publish_id}

    resp = requests.post(url, headers=_auth_headers_json(), json=payload)

    try:
        data = resp.json()
    except Exception:
        raise TikTokError(f"status non-JSON response: {resp.text[:300]}")

    error = data.get("error", {})
    if error.get("code") != "ok":
        raise TikTokError(f"status error: {data}")

    return data["data"]


def post_video_to_tiktok(video_path: str, title: str):
    """
    High-level helper: creator_info -> init -> upload -> status.
    """
    logging.info("=== TikTok Direct Post ===")

    # First confirm token & creator info
    info = get_creator_info()
    logging.info(
        "Creator OK. Posting as @%s (nickname: %s). Max duration: %ss",
        info.get("creator_username"),
        info.get("creator_nickname"),
        info.get("max_video_post_duration_sec"),
    )

    # Init upload
    publish_id, upload_url, file_size = init_direct_post(video_path, title)
    logging.info("publish_id: %s", publish_id)

    # Upload file
    upload_video_file(upload_url, video_path, file_size)

    # Initial status check
    status = fetch_publish_status(publish_id)
    logging.info("Initial status: %s", status.get("status"))

    return publish_id, status


if __name__ == "__main__":
    # Show what token we loaded (only first few chars, for debugging)
    if TIKTOK_ACCESS_TOKEN:
        print(f"TIKTOK_ACCESS_TOKEN loaded (first 12 chars): {TIKTOK_ACCESS_TOKEN[:12]}...")
    else:
        print("ERROR: TIKTOK_ACCESS_TOKEN not found. Check your .env file.")
        sys.exit(1)

    # If no args, just test the token and exit
    if len(sys.argv) < 3:
        print("\nNo video path/caption given. We'll just test the token with creator_info().\n")
        try:
            info = get_creator_info()
            print("Creator info test OK.")
            print("Username:", info.get("creator_username"))
            print("Nickname:", info.get("creator_nickname"))
        except TikTokError as e:
            logging.error("TikTokError during creator_info test: %s", e)
            sys.exit(1)
        sys.exit(0)

    # If args given, we BOTH test creator_info and then upload video
    video_path = sys.argv[1]
    caption = sys.argv[2]

    try:
        publish_id, status = post_video_to_tiktok(video_path, caption)
        print("\nPublish ID:", publish_id)
        print("Status:", status)
    except TikTokError as e:
        logging.error("TikTokError during upload: %s", e)
        sys.exit(1)
