#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
fb_chunk_upload.py
------------------
Chunked video upload helper for Facebook Page videos.

Uses the official 3-phase flow:
  1) upload_phase=start   -> get upload_session_id + first offsets
  2) upload_phase=transfer (loop) -> send chunks using offsets
  3) upload_phase=finish  -> publish the video with caption

This module is called from auto_poster.py like:

    fb_chunk_upload(
        file_path=local_path,
        page_id=FB_PAGE_ID,
        access_token=META_ACCESS_TOKEN,
        caption=meta["caption"],
    )
"""

import os
import logging
import requests


LOGGER = logging.getLogger("fb_chunk_upload")


def fb_chunk_upload(file_path: str, page_id: str, access_token: str, caption: str | None = None) -> bool:
    """
    Upload a video file to a Facebook Page using chunked upload.

    :param file_path: Path to local video file
    :param page_id:   Page ID (e.g. "320422494483049")
    :param access_token: Long-lived Page access token
    :param caption:   Optional caption/description for the video
    :return: True on success, False on any error
    """
    if not os.path.exists(file_path):
        LOGGER.error("fb_chunk_upload: file not found: %s", file_path)
        return False

    file_size = os.path.getsize(file_path)
    LOGGER.info("fb_chunk_upload: file=%s size=%d bytes", file_path, file_size)

    # IMPORTANT: use graph-video endpoint for large uploads
    base_url = f"https://graph-video.facebook.com/v18.0/{page_id}/videos"

    # ----------------------------------------------------
    # 1) START phase — create upload session
    # ----------------------------------------------------
    start_data = {
        "upload_phase": "start",
        "file_size": file_size,
        "access_token": access_token,
    }

    start_resp = requests.post(base_url, data=start_data)
    if start_resp.status_code != 200:
        LOGGER.error("fb_chunk_upload: START failed (%s) — %s", start_resp.status_code, start_resp.text)
        return False

    start_json = start_resp.json()
    upload_session_id = start_json.get("upload_session_id")
    video_id = start_json.get("video_id")
    start_offset = int(start_json.get("start_offset", "0"))
    end_offset = int(start_json.get("end_offset", "0"))

    LOGGER.info(
        "fb_chunk_upload: START ok — session_id=%s video_id=%s first chunk [%s, %s]",
        upload_session_id,
        video_id,
        start_offset,
        end_offset,
    )

    # ----------------------------------------------------
    # 2) TRANSFER phase — loop sending chunks
    # ----------------------------------------------------
    with open(file_path, "rb") as f:
        while True:
            # If start_offset == end_offset, uploading is done
            if start_offset == end_offset:
                break

            chunk_len = end_offset - start_offset
            f.seek(start_offset)
            chunk = f.read(chunk_len)

            if not chunk:
                LOGGER.error(
                    "fb_chunk_upload: Empty chunk read at offsets [%s, %s]. Aborting.",
                    start_offset,
                    end_offset,
                )
                return False

            LOGGER.info(
                "fb_chunk_upload: TRANSFER chunk [%s, %s] (%d bytes)",
                start_offset,
                end_offset,
                len(chunk),
            )

            files = {
                "video_file_chunk": ("chunk.mp4", chunk, "video/mp4"),
            }
            transfer_data = {
                "upload_phase": "transfer",
                "upload_session_id": upload_session_id,
                "start_offset": start_offset,
                "access_token": access_token,
            }

            transfer_resp = requests.post(base_url, data=transfer_data, files=files)
            if transfer_resp.status_code != 200:
                LOGGER.error(
                    "fb_chunk_upload: TRANSFER failed (%s) — %s",
                    transfer_resp.status_code,
                    transfer_resp.text,
                )
                return False

            transfer_json = transfer_resp.json()
            start_offset = int(transfer_json.get("start_offset", "0"))
            end_offset = int(transfer_json.get("end_offset", "0"))

    LOGGER.info("fb_chunk_upload: all chunks transferred successfully.")

    # ----------------------------------------------------
    # 3) FINISH phase — publish the video
    # ----------------------------------------------------
    finish_data = {
        "upload_phase": "finish",
        "upload_session_id": upload_session_id,
        "access_token": access_token,
    }

    # Facebook uses 'description' for the body text under the video.
    # We can also set 'title' (shortened caption).
    if caption:
        finish_data["description"] = caption
        finish_data["title"] = caption[:100]

    finish_resp = requests.post(base_url, data=finish_data)
    if finish_resp.status_code != 200:
        LOGGER.error(
            "fb_chunk_upload: FINISH failed (%s) — %s",
            finish_resp.status_code,
            finish_resp.text,
        )
        return False

    LOGGER.info("fb_chunk_upload: FINISH ok — %s", finish_resp.text)
    return True
