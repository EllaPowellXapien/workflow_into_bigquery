import requests
import json
from datetime import datetime, timezone
from google.cloud import storage
print("script started")

# --------------------------
# CONFIG
# --------------------------
ES_ENDPOINT    = "https://mi-reporting.es.us-west-2.aws.found.io"
INDEX_NAME     = "enquiry"

OUTPUT_BUCKET  = "csv-updater-output"
OUTPUT_FILE    = "updating_urls.csv"

BATCH_SIZE     = 200      # docs per request
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

def append_urls_to_gcs(urls):
    """Append URLs to CSV stored in GCS"""
    client = storage.Client()
    bucket = client.bucket(OUTPUT_BUCKET)
    blob = bucket.blob(OUTPUT_FILE)

    # Download current CSV (if exists)
    try:
        existing = blob.download_as_text().splitlines()
    except Exception:
        existing = ["url"]  # start with header

    # Append new rows
    writer_rows = existing + [u for u in urls]

    # Upload back to GCS
    blob.upload_from_string("\n".join(writer_rows))
    print(f"☁️ Appended {len(urls)} URLs to gs://{OUTPUT_BUCKET}/{OUTPUT_FILE}")


# --------------------------
# Poll once for new docs
# --------------------------
def poll_once(last_seen_iso: str):
    url = f"{ES_ENDPOINT}/{INDEX_NAME}/_search"
    headers = {
        **make_bearer_header(),
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

        report_url = pick_first(src, URL_FIELD_CANDIDATES) or ""
        if report_url:
            urls.append(report_url)

        if start_time > max_seen:
            max_seen = start_time

    if urls:
        append_urls_to_gcs(urls)

    return max_seen, len(urls)


# --------------------------
# Main (single run for Cloud Run Job)
# --------------------------
def main():
    last_seen = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    print(f"🔎 Running single poll, last_seen StartTime = {last_seen}")

    try:
        last_seen, n = poll_once(last_seen)
        if n:
            print(f"📥 Appended {n} new URLs; new last_seen={last_seen}")
        else:
            print("… no new docs")
    except Exception as e:
        print(f"⚠️ Poll error: {e}")


# --------------------------
# Entry
# --------------------------
if __name__ == "__main__":
    main()
