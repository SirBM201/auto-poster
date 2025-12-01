# get_token.py
import os
import requests
from dotenv import load_dotenv

load_dotenv()

GRAPH_VERSION = "v21.0"
USER_TOKEN = os.getenv("META_ACCESS_TOKEN")

if not USER_TOKEN:
    print("❌ META_ACCESS_TOKEN not found in .env")
    raise SystemExit(1)

print(f"🔑 Using META_ACCESS_TOKEN: {USER_TOKEN[:10]}...")

base_url = f"https://graph.facebook.com/{GRAPH_VERSION}"

# 1) Show which user this token belongs to
print("\n👤 Checking token owner (/me)...")
me_resp = requests.get(
    f"{base_url}/me",
    params={"fields": "id,name", "access_token": USER_TOKEN},
)
print("Status:", me_resp.status_code, me_resp.text)

# 2) List pages and page access tokens
print("\n📄 Fetching your Facebook Pages (/me/accounts)...")
pages_resp = requests.get(
    f"{base_url}/me/accounts",
    params={"fields": "id,name,access_token", "access_token": USER_TOKEN},
)
if pages_resp.status_code != 200:
    print("❌ Error fetching pages:", pages_resp.status_code, "—", pages_resp.text)
    raise SystemExit(1)

data = pages_resp.json()
pages = data.get("data", [])

if not pages:
    print("⚠️ No pages found for this user/token.")
    raise SystemExit(0)

print("\n✅ Pages accessible with this token:")
for p in pages:
    pid = p.get("id")
    name = p.get("name")
    ptoken = p.get("access_token", "")
    print(f" • {name} — {pid}")
    print(f"   Page access token (copy into .env as META_ACCESS_TOKEN):")
    print(f"   {ptoken}\n")

print("👉 Next step:")
print("  1. Choose the page you want (e.g. Sir-BM).")
print("  2. Copy its page access token above.")
print("  3. Open your .env file and replace META_ACCESS_TOKEN=... with that page token.")
print("  4. Save .env and run:  python auto_poster.py")
