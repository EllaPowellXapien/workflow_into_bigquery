import requests
import base64
import time
import json
from datetime import datetime, timezone
from google.cloud import storage

# --------------------------
# CONFIG
# --------------------------
ES_ENDPOINT    = "https://mi-reporting.es.us-west-2.aws.found.io"
INDEX_NAME     = "enquiry"

OUTPUT_BUCKET  = "csv-updater-output"
OUTPUT_FILE    = "updating_urls.csv"

POLL_INTERVAL  = 10       # seconds between polls
BATCH_SIZE     = 200      # docs per request

# API key credentials
API_KEY_ID     = "Iz_amZgBx3uIKls0fAk0"
API_KEY_SECRET = "-3TYR1tUJ5Sg0Bva2VArwQ"

# Candidate URL fields inside documents
URL_FIELD_CANDIDATES = ["ReportUrl", "reportUrl", "url"]

MAX_POLLS = 1  # Limit for CI

def make_api_key_header():
    """Build ApiKey auth header for Elasticsearch."""
    token = f"{API_KEY_ID}:{API_KEY_SECRET}"
    b64 = base64.b64encode(token.encode()).decode()
    return {"Authorization": f"ApiKey {b64}"}

def pick_first(src: dict, candidates):
    """Pick the first non-null field from candidates."""
    for c in candidates:
        if c in src and src[c] is not None:
            return src[c]
    return None

def append_urls_to_gcs(urls):
    """Append URLs to CSV stored in GCS."""
    client = storage.Client()
    bucket = client.bucket(OUTPUT_BUCKET)
    blob = bucket.blob(OUTPUT_FILE)
    try:
        existing = blob.download_as_text().splitlines()
    except Exception:
        existing = ["url"]  # header row if file doesn’t exist yet
    writer_rows = existing + urls
    blob.upload_from_string("\n".join(writer_rows))
    print(f"☁️ Appended {len(urls)} URLs to gs://{OUTPUT_BUCKET}/{OUTPUT_FILE}")

def poll_once(last_seen_iso: str):
    url = f"{ES_ENDPOINT}/{INDEX_NAME}/_search"
    headers = {
        **make_api_key_header(),
        "Content-Type": "application/json"
    }
    body = {
        "size": BATCH_SIZE,
        "track_total_hits": False,
        "query": {
            "range": {
                "StartTime": {"gt": last_seen_iso}
            }
        },
        "sort": [
            {"StartTime": "asc"}
        ],
        "_source": True
    }
    print(f"🔎 Sending ES query with last_seen={last_seen_iso}")
    resp = requests.post(url, headers=headers, data=json.dumps(body), timeout=90)
    if resp.status_code != 200:
        raise RuntimeError(f"Search failed {resp.status_code}: {resp.text[:300]}")
    data = resp.json()
    hits = data.get("hits", {}).get("hits", [])
    urls = []
    max_seen = last_seen_iso
    for h in hits:
        src = h.get("_source", {})
        start_time = src.get("StartTime") or src.get("startTime")
        if not start_time:
            continue
        report_url = pick_first(src, URL_FIELD_CANDIDATES)
        if report_url:
            urls.append(report_url)
        if start_time > max_seen:
            max_seen = start_time
    if urls:
        append_urls_to_gcs(urls)
    return max_seen, len(urls)

def listen_changes(max_polls=MAX_POLLS):
    last_seen = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    print(f"🚀 Starting poller, last_seen StartTime = {last_seen}")
    for _ in range(max_polls):
        try:
            last_seen, n = poll_once(last_seen)
            if n:
                print(f"📥 Appended {n} new URLs; new last_seen={last_seen}")
            else:
                print("… no new docs this run")
        except Exception as e:
            print(f"⚠️ Poll error: {e}")
        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    listen_changes()
