"""
Google Photos Downloader
Downloads all photos and videos from your Google Photos library.
"""

import json
import os
import sys
import time
from pathlib import Path

import requests
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = ["https://www.googleapis.com/auth/photoslibrary.readonly"]
CLIENT_SECRET_FILE = "client_secret.json"
TOKEN_FILE = "token.json"
DOWNLOAD_DIR = "photos"
API_BASE = "https://photoslibrary.googleapis.com/v1"


def authenticate():
    """Authenticate with Google Photos API and return credentials."""
    creds = None

    if os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_FILE, SCOPES)
            creds = flow.run_local_server(port=0)

        with open(TOKEN_FILE, "w") as f:
            f.write(creds.to_json())

    return creds


def get_headers(creds):
    return {"Authorization": f"Bearer {creds.token}"}


def list_media_items(creds, page_token=None):
    """List media items from Google Photos, one page at a time."""
    params = {"pageSize": 100}
    if page_token:
        params["pageToken"] = page_token

    resp = requests.get(
        f"{API_BASE}/mediaItems",
        headers=get_headers(creds),
        params=params,
        timeout=30,
    )
    if not resp.ok:
        print(f"  API Error {resp.status_code}: {resp.text}")
    resp.raise_for_status()
    return resp.json()


def download_item(item, download_dir):
    """Download a single media item (photo or video)."""
    filename = item["filename"]
    filepath = download_dir / filename

    # Skip if already downloaded
    if filepath.exists():
        return False

    base_url = item["baseUrl"]
    media_metadata = item.get("mediaMetadata", {})

    # Append download parameters for full resolution
    if "video" in media_metadata:
        download_url = f"{base_url}=dv"  # full video
    else:
        download_url = f"{base_url}=d"  # full resolution photo

    resp = requests.get(download_url, timeout=120)
    resp.raise_for_status()

    filepath.write_bytes(resp.content)
    return True


def main():
    print("=== Google Photos Downloader ===\n")

    # Authenticate
    print("Authenticating...")
    creds = authenticate()
    print("Authenticated successfully!\n")

    # Create download directory
    download_dir = Path(DOWNLOAD_DIR)
    download_dir.mkdir(exist_ok=True)

    # Count existing files
    existing = len(list(download_dir.iterdir()))
    if existing > 0:
        print(f"Found {existing} existing files in {DOWNLOAD_DIR}/, will skip duplicates.\n")

    # Download all media items
    total_downloaded = 0
    total_skipped = 0
    total_errors = 0
    page_token = None
    page_num = 0

    while True:
        page_num += 1
        print(f"Fetching page {page_num}...")

        try:
            data = list_media_items(creds, page_token)
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 401:
                print("Token expired, refreshing...")
                creds.refresh(Request())
                with open(TOKEN_FILE, "w") as f:
                    f.write(creds.to_json())
                continue
            raise

        items = data.get("mediaItems", [])
        if not items:
            print("No more items found.")
            break

        print(f"  Found {len(items)} items on this page.")

        for item in items:
            filename = item.get("filename", "unknown")
            try:
                downloaded = download_item(item, download_dir)
                if downloaded:
                    total_downloaded += 1
                    print(f"  Downloaded: {filename} ({total_downloaded} new)")
                else:
                    total_skipped += 1
            except Exception as e:
                total_errors += 1
                print(f"  ERROR downloading {filename}: {e}")

        page_token = data.get("nextPageToken")
        if not page_token:
            break

        # Small delay to avoid rate limiting
        time.sleep(0.5)

    print(f"\n=== Done ===")
    print(f"Downloaded: {total_downloaded}")
    print(f"Skipped (already existed): {total_skipped}")
    print(f"Errors: {total_errors}")


if __name__ == "__main__":
    main()
