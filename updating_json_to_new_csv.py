import csv
import logging
import os
import json
import time
from google.cloud import storage

# --- CONFIGURATION ---
WATCH_FOLDER = os.path.join("all_downloads", "reports")
OUTPUT_BUCKET = "csv-updater-output"
OUTPUT_FILE = "updating_scripts_summary.csv"
PROCESSED_LOG = "processed_files.log"

# --- LOGGING ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("log.txt"), logging.StreamHandler()]
)

CSV_FIELDS = [
    "UserEmail", "Subject", "SubjectType", "RelevantSourceGroups", "NonRelevantSourceGroups",
    "TotalReadingTime_Relevant_Minutes", "TotalReadingTime_NonRelevant_Minutes",
    "TotalWatchlistHits", "ConfirmedWatchlistHits",
    "ConfirmedSanctions", "ConfirmedWatchlists", "ConfirmedPeps",
    "ReviewSanctions", "ReviewWatchlists", "ReviewPeps",
    "DiscardedSanctions", "DiscardedWatchlists", "DiscardedPeps",
    "DetectedLanguages"
]

# --- HELPERS ---
def load_from_gcs():
    """Download CSV content from GCS if exists."""
    client = storage.Client()
    bucket = client.bucket(OUTPUT_BUCKET)
    blob = bucket.blob(OUTPUT_FILE)
    try:
        return blob.download_as_text().splitlines()
    except Exception:
        return []

def save_to_gcs(lines):
    """Upload CSV content to GCS."""
    client = storage.Client()
    bucket = client.bucket(OUTPUT_BUCKET)
    blob = bucket.blob(OUTPUT_FILE)
    blob.upload_from_string("\n".join(lines), content_type="text/csv")
    logging.info(f"☁️ Updated gs://{OUTPUT_BUCKET}/{OUTPUT_FILE}")

def append_to_gcs(row_dict):
    """Append a row to the CSV stored in GCS."""
    existing = load_from_gcs()
    if existing:
        reader = csv.reader(existing)
        headers = next(reader)
    else:
        headers = CSV_FIELDS
        existing = [",".join(headers)]

    writer = csv.DictWriter(open(os.devnull, "w"), fieldnames=headers)  # dummy
    row_line = ",".join(str(row_dict.get(h, "")) for h in headers)
    existing.append(row_line)

    save_to_gcs(existing)

def safe_get(dct, keys, default=None):
    for key in keys:
        if isinstance(dct, dict):
            dct = dct.get(key, default)
        else:
            return default
    return dct if dct is not None else default

def extract_json_data(json_path):
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            logging.error(f"Invalid top-level JSON in {json_path}")
            return
    except Exception as e:
        logging.error(f"Failed to read/parse {json_path}: {e}")
        return

    try:
        # Metadata
        user_email = safe_get(data, ["ReportMetadata", "UserEmail"], "Unknown")
        subject = safe_get(data, ["ReportMetadata", "Subject"], "Unknown")
        subject_type = safe_get(data, ["ReportMetadata", "SubjectType"], "Unknown")

        # Groups
        relevant_groups = safe_get(data, ["ReportSourcing", "RelevantSourceGroups"], [])
        non_relevant_groups = safe_get(data, ["ReportSourcing", "NonRelevantSourceGroups"], [])

        def calc_time(groups):
            total = 0
            for group in groups:
                for stack in group.get("SourceStacks", []):
                    for source in stack.get("Sources", []):
                        minutes = safe_get(source, ["ReadingTime", "Minutes"], 3)
                        if not isinstance(minutes, (int, float)):
                            minutes = 3
                        total += minutes
            return total

        total_rel = calc_time(relevant_groups)
        total_nonrel = calc_time(non_relevant_groups)

        langs = set()
        for g in relevant_groups + non_relevant_groups:
            for stack in g.get("SourceStacks", []):
                for source in stack.get("Sources", []):
                    lang = source.get("DetectedLanguage")
                    if lang:
                        langs.add(lang)

        def count_hits(path):
            section = safe_get(data, path, {})
            if isinstance(section, list):
                return len(section)
            if isinstance(section, dict):
                return sum(len(v) for v in section.values() if isinstance(v, list))
            return 0

        confirmed_sanctions = count_hits(["ScreeningSection", "Sanctions", "Confirmed"])
        confirmed_watchlists = count_hits(["ScreeningSection", "Watchlists", "Confirmed"])
        confirmed_peps = count_hits(["ScreeningSection", "Peps", "Confirmed"])
        confirmed_hits = confirmed_sanctions + confirmed_watchlists + confirmed_peps

        review_sanctions = count_hits(["ScreeningSection", "Sanctions", "Unconfirmed"])
        review_watchlists = count_hits(["ScreeningSection", "Watchlists", "Unconfirmed"])
        review_peps = count_hits(["ScreeningSection", "Peps", "Unconfirmed"])

        discarded_sanctions = count_hits(["ScreeningSection", "DisregardedPersonListings", "Sanctions"])
        discarded_watchlists = count_hits(["ScreeningSection", "DisregardedPersonListings", "WatchLists"])
        discarded_peps = count_hits(["ScreeningSection", "DisregardedPersonListings", "Peps"])

        total_hits = confirmed_hits + review_sanctions + review_watchlists + review_peps + discarded_sanctions + discarded_watchlists + discarded_peps

        append_to_gcs({
            "UserEmail": user_email,
            "Subject": subject,
            "SubjectType": subject_type,
            "RelevantSourceGroups": len(relevant_groups),
            "NonRelevantSourceGroups": len(non_relevant_groups),
            "TotalReadingTime_Relevant_Minutes": total_rel,
            "TotalReadingTime_NonRelevant_Minutes": total_nonrel,
            "TotalWatchlistHits": total_hits,
            "ConfirmedWatchlistHits": confirmed_hits,
            "ConfirmedSanctions": confirmed_sanctions,
            "ConfirmedWatchlists": confirmed_watchlists,
            "ConfirmedPeps": confirmed_peps,
            "ReviewSanctions": review_sanctions,
            "ReviewWatchlists": review_watchlists,
            "ReviewPeps": review_peps,
            "DiscardedSanctions": discarded_sanctions,
            "DiscardedWatchlists": discarded_watchlists,
            "DiscardedPeps": discarded_peps,
            "DetectedLanguages": ", ".join(sorted(langs)) if langs else "Unknown"
        })
        logging.info(f"Processed: {json_path}")
    except Exception as e:
        logging.error(f" Failed to process {json_path}: {e}")

def monitor_folder():
    logging.info(f" Watching folder: {WATCH_FOLDER}")
    while True:
        try:
            for file in os.listdir(WATCH_FOLDER):
                if file.endswith(".json"):
                    full_path = os.path.join(WATCH_FOLDER, file)
                    extract_json_data(full_path)
        except Exception as e:
            logging.error(f"Error during folder monitoring: {e}")
        time.sleep(5)

# --- RUN ---
if __name__ == "__main__":
    monitor_folder()
