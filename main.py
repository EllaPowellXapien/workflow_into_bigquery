import requests
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

BATCH_SIZE     = 200
URL_FIELD_CANDIDATES = ["ReportUrl", "reportUrl", "url"]

# --------------------------
# Helpers
# --------------------------
def read_bearer_token():
    """Download latest bearer token from GCS"""
    client = storage.Client()
    bucket = client.bucket("xapien-token-store")
    blob = bucket.blob("token.txt")
    return blob.download_as_text().strip()

def make_bearer_header():
    return {"Authorization": f"Bearer {read_bearer_token()}"}

def pick_first(src: dict, candidates):
    for c in candidates:
        if c in src and src[c] is not None:
            return src[c]
    return None

def ensure_csv_exists():
    client = storage.Client()
    bucket = client.bucket(OUTPUT_BUCKET)
    blob = bucket.blob(OUTPUT_FILE)
    if not blob.exists():
        blob.upload_from_string("url\n")
        print(f"☁️ Created empty CSV at gs://{OUTPUT_BUCKET}/{OUTPUT_FILE}")

def append_urls_to_gcs(urls):
    client = storage.Client()
    bucket = client.bucket(OUTPUT_BUCKET)
    blob = bucket.blob(OUTPUT_FILE)

    try:
        existing = blob.download_as_text().splitlines()
    except Exception:
        existing = ["url"]

    updated = existing + urls
    blob.upload_from_string("\n".join(updated))
    print(f"📥 Appended {len(urls)} URLs to gs://{OUTPUT_BUCKET}/{OUTPUT_FILE}")

def poll_once(last_seen_iso: str):
    url = f"{ES_ENDPOINT}/{INDEX_NAME}/_search"
    headers = {**make_bearer_header(), "Content-Type": "application/json"}
    body = {
        "size": BATCH_SIZE,
        "track_total_hits": False,
        "query": {"range": {"StartTime": {"gt": last_seen_iso}}},
        "sort": [{"StartTime": "asc"}],
        "_source": True,
    }

    resp = requests.post(url, headers=headers, data=json.dumps(body), timeout=30)
    if resp.status_code != 200:
        raise RuntimeError(f"Search failed {resp.status_code}: {resp.text[:500]}")

    data = resp.json()
    hits = data.get("hits", {}).get("hits", [])
    urls, max_seen = [], last_seen_iso

    for h in hits:
        src = h.get("_source", {})
        start_time = src.get("StartTime") or src.get("startTime")
        if not start_time:
            continue
        url = pick_first(src, URL_FIELD_CANDIDATES)
        if url:
            urls.append(url)
        if start_time > max_seen:
            max_seen = start_time

    if urls:
        append_urls_to_gcs(urls)
    else:
        print("… no new docs")

    return max_seen, len(urls)

# --------------------------
# Cloud Function Entry
# --------------------------
def main(request):
    print("🚀 Cloud Function csv_updater triggered")

    ensure_csv_exists()
    last_seen = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")

    try:
        last_seen, n = poll_once(last_seen)
        return f" Done, appended {n} URLs (last_seen={last_seen})"
    except Exception as e:
        print(f" Error: {e}")
        return f" Failed: {e}", 500
