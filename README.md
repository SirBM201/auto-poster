# Auto Social Video Poster

A powerful automation system that uploads daily videos from S3 to Instagram, Facebook Pages, YouTube, and X/Twitter.

---

## 🚀 Overview

This project automates:

* Video retrieval from **Amazon S3**
* Local download + metadata extraction
* Uploading videos to:

  * **Instagram Reels** (Graph API)
  * **Facebook Page Videos**
  * **YouTube Channels**
  * **X/Twitter** video tweets
* Intelligent retry system
* Clean logs + slot summary

---

## 🖼️ System Architecture

![Architecture Diagram](https://raw.githubusercontent.com/github/explore/main/topics/automation/automation.png)

---

## 🔧 Folder Structure

```
ANOTHER/
│── auto_poster.py            # Main automation engine
│── get_token.py              # Gets Meta access tokens
│── youtube_auth.py           # YouTube OAuth flow
│── .gitignore
│── README.md                 # This file
│── config/
│     └── x.json              # Supported config for X/Twitter
│── src/
│     └── post_to_x.py        # X/Twitter upload engine
│── logs/                     # Log files (ignored in Git)
│── videos/                   # Local video storage
```

---

## ⚙️ Environment Setup

Create a `.env` file in the root directory:

```
AWS_ACCESS_KEY_ID=YOUR_KEY
AWS_SECRET_ACCESS_KEY=YOUR_SECRET
AWS_REGION_NAME=us-east-1
S3_BUCKET_NAME=fair-video-source

# Meta / Instagram
IG_ACCESS_TOKEN=YOUR_IG_TOKEN
IG_USER_ID=YOUR_IG_USER_ID

# Facebook
META_ACCESS_TOKEN=YOUR_PAGE_ACCESS_TOKEN
FB_PAGE_ID=YOUR_FACEBOOK_PAGE_ID

# YouTube
YOUTUBE_CLIENT_ID=your_client_id
youtube_client_secret=your_client_secret
YOUTUBE_REDIRECT_URI=http://localhost:8080/
```

⚠️ Do NOT commit `.env` to GitHub.

---

## ▶️ Running the Poster

```
python auto_poster.py
```

You will see logs like:

* Selected slot
* Download status
* Metadata extracted
* Instagram upload progress
* Retry attempts
* Final status summary

---

## 📱 Instagram Upload Flow

![Instagram Logo](https://raw.githubusercontent.com/github/explore/main/topics/instagram/instagram.png)

* Pre-signed S3 URL generated
* Reels container created
* Long polling (up to 10 minutes)
* Auto publish when ready

---

## 📺 YouTube Setup

![YouTube Logo](https://raw.githubusercontent.com/github/explore/main/topics/youtube/youtube.png)

Authorize YouTube once:

```
python youtube_auth.py
```

This generates:

* `credentials.json` (DO NOT commit)
* YouTube token

---

## 📘 Facebook Setup

![Facebook Logo](https://raw.githubusercontent.com/github/explore/main/topics/facebook/facebook.png)

Get your Page Access Token:

```
python get_token.py
```

Select the Page you want, copy the token, and place it in `.env`.

---

## 🕊️ X/Twitter Posting

![Twitter Logo](https://raw.githubusercontent.com/github/explore/main/topics/twitter/twitter.png)

Configure posting behavior in:

```
config/x.json
```

Example:

```json
{
  "enabled": true,
  "caption_template": "{title}\n\n#Faith #DailyWord",
  "video_category": "tweet_video",
  "max_retries": 3
}
```

---

## 📦 Deployment Options

* GitHub Actions (cron automation)
* Koyeb
* Railway
* AWS Lambda + EventBridge

You can request deployment help when ready.

---

## 📊 Final Slot Summary

At the end of every run, you see:

```
RESULT SUMMARY:
  reel_9am: SUCCESS
  reel_4pm: SUCCESS
  std_9_30am: SKIPPED (no video)
  std_4_30pm: FAILED (IG timeout)
```

---

## 🛡️ Safety Rules

❌ Do NOT commit the following:

```
.env
client_secret.json
credentials.json
youtube_token.json
videos/*
logs/*
```

---

## 👑 Author

** Shotayo Moses - BM**

Just tell me: **"BM wants the upgrade"**.
