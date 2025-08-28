# try_new_updates.py
import requests
import time
import json
import csv
import os
from datetime import datetime, timezone
from google.cloud import storage

# --------------------------
# CONFIG
# --------------------------
ES_ENDPOINT    = "https://mi-reporting.es.us-west-2.aws.found.io"
INDEX_NAME     = "enquiry"

OUTPUT_CSV     = "updating_urls.csv"
GCS_BUCKET     = "csv-updater-output"
POLL_INTERVAL  = 10
BATCH_SIZE     = 200

URL_FIELD_CANDIDATES = ["ReportUrl", "reportUrl", "url"]

TOKEN_FILE = "token.txt"  # ✅ matches what token_with_report writes

# --------------------------
# Helpers
# --------------------------
def read_bearer_token():
    """Read the latest bearer token from file written by token_with_report.py"""
    if not os.path.exists(TOKEN_FILE):
        raise FileNotFoundError(f"{TOKEN_FILE} not found. Run token_with_report.py first.")
    with open(TOKEN_FILE, "r") as f:
        token = f.read().strip()
    return token

def make_bearer_header():
    token = read_bearer_token()
    return {"Authorization": f"Bearer {token}"}

def append_urls(urls):
    with open(OUTPUT_CSV, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        for u in urls:
            w.writerow([u])
    upload_to_gcs(OUTPUT_CSV, GCS_BUCKET, OUTPUT_CSV)

def upload_to_gcs(local_path: str, bucket_name: str, blob_name: str):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(local_path)
    print(f"☁️ Uploaded {local_path} to gs://{bucket_name}/{blob_name}")

# --------------------------
# Poll once
# --------------------------
def poll_once(last_seen_iso: str):
    url = f"{ES_ENDPOINT}/{INDEX_NAME}/_search"
    headers = {
        **make_bearer_header(),  # ✅ Bearer token here
        "Content-Type": "application/json"
    }

    body = {
        "size": BATCH_SIZE,
        "track_total_hits": False,
        "query": {"range": {"StartTime": {"gt": last_seen_iso}}},
        "sort": [{"StartTime": "asc"}],
        "_source": True
    }

    resp = requests.post(url, headers=headers, data=json.dumps(body), timeout=90)
    if resp.status_code != 200:
        raise RuntimeError(f"Search failed {resp.status_code}: {resp.text[:500]}")

    data = resp.json()
    hits = data.get("hits", {}).get("hits", [])
    urls = []
    max_seen = last_seen_iso

    for h in hits:
        src = h.get("_source", {})
        start_time = src.get("StartTime") or src.get("startTime")
        if not start_time:
            continue

        report_url = next((src.get(c) for c in URL_FIELD_CANDIDATES if src.get(c)), "")
        if report_url:
            urls.append(report_url)

        if start_time > max_seen:
            max_seen = start_time

    if urls:
        append_urls(urls)

    return max_seen, len(urls)

# --------------------------
# Main loop
# --------------------------
def listen_changes():
    last_seen = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    print(f"🔎 Starting poller, last_seen StartTime = {last_seen}")

    while True:
        try:
            last_seen, n = poll_once(last_seen)
            if n:
                print(f"📥 Appended {n} new URLs; new last_seen={last_seen}")
            else:
                print("… no new docs")
        except Exception as e:
            print(f"⚠️ Poll error: {e}")
        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    listen_changes()
