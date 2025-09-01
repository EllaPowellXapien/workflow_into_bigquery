import requests
import base64
import time
import json
import csv
from datetime import datetime, timezone

# --------------------------
# CONFIG
# --------------------------
ES_ENDPOINT    = "https://mi-reporting.es.us-west-2.aws.found.io"
INDEX_NAME     = "enquiry"

API_KEY_ID     = "Iz_amZgBx3uIKls0fAk0"
API_KEY_SECRET = "-3TYR1tUJ5Sg0Bva2VArwQ"

OUTPUT_CSV     = "updating_urls.csv"
BATCH_SIZE     = 200

URL_FIELD_CANDIDATES = ["ReportUrl", "reportUrl", "url"]

# --------------------------
# Helpers
# --------------------------
def make_api_key_header():
    token = f"{API_KEY_ID}:{API_KEY_SECRET}"
    b64 = base64.b64encode(token.encode()).decode()
    return {"Authorization": f"ApiKey {b64}"}

def pick_first(src: dict, candidates):
    for c in candidates:
        if c in src and src[c]:
            return src[c]
    return None

def append_urls(urls):
    with open(OUTPUT_CSV, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        for u in urls:
            w.writerow([u])

# --------------------------
# Poll once for new docs
# --------------------------
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
        append_urls(urls)

    return max_seen, len(urls)

# --------------------------
# Run once
# --------------------------
def main():
    last_seen = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    print(f"🔎 Running single poll, last_seen StartTime = {last_seen}")

    try:
        last_seen, n = poll_once(last_seen)
        if n:
            print(f"📥 Appended {n} new URLs; new last_seen={last_seen}")
        else:
            print("… no new docs this run")
    except Exception as e:
        print(f"⚠️ Poll error: {e}")

# --------------------------
# Entry
# --------------------------
if __name__ == "__main__":
    main()
