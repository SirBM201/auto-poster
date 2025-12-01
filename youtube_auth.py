# youtube_auth.py
from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = ["https://www.googleapis.com/auth/youtube.upload"]

def main():
    # Uses the client_secret.json you downloaded from Google Cloud
    flow = InstalledAppFlow.from_client_secrets_file(
        "client_secret.json",
        SCOPES
    )

    # Opens a browser window for you to log in and approve
    creds = flow.run_local_server(port=8080)

    # Save the tokens for auto_poster.py to use
    with open("credentials.json", "w", encoding="utf-8") as f:
        f.write(creds.to_json())

    print("✅ New YouTube credentials saved to credentials.json")

if __name__ == "__main__":
    main()
