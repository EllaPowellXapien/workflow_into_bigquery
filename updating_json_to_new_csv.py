import csv
import logging
import os
import json
import time

# --- CONFIGURATION ---
WATCH_FOLDER = os.path.join("all_downloads", "reports")
CSV_OUTPUT_PATH = "updating_scripts_summary.csv"
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

def load_processed_files():
    if not os.path.exists(PROCESSED_LOG):
        return set()
    with open(PROCESSED_LOG, "r", encoding="utf-8") as f:
        return set(line.strip() for line in f)

def save_processed_file(filename):
    with open(PROCESSED_LOG, "a", encoding="utf-8") as f:
        f.write(f"{filename}\n")

def validate_csv_headers():
    if os.path.exists(CSV_OUTPUT_PATH):
        with open(CSV_OUTPUT_PATH, "r", encoding="utf-8") as csvfile:
            first_line = csvfile.readline().strip()
            existing_headers = first_line.split(",")
        if existing_headers != CSV_FIELDS:
            logging.warning("Incorrect CSV headers detected. Deleting and recreating file.")
            os.remove(CSV_OUTPUT_PATH)

def extract_json_data(json_path):
    def safe_get(dct, keys, default=None):
        """Safely navigate nested dictionaries."""
        for key in keys:
            if isinstance(dct, dict):
                dct = dct.get(key, default)
            else:
                return default
        return dct if dct is not None else default

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

        # Sourcing groups
        relevant_groups = safe_get(data, ["ReportSourcing", "RelevantSourceGroups"], [])
        non_relevant_groups = safe_get(data, ["ReportSourcing", "NonRelevantSourceGroups"], [])

        def calculate_reading_time(groups):
            total = 0
            for group in groups:
                for stack in group.get("SourceStacks", []):
                    for source in stack.get("Sources", []):
                        minutes = safe_get(source, ["ReadingTime", "Minutes"], 3)
                        if not isinstance(minutes, (int, float)):
                            minutes = 3
                        total += minutes
            return total

        total_reading_time_relevant = calculate_reading_time(relevant_groups)
        total_reading_time_non_relevant = calculate_reading_time(non_relevant_groups)

        # Detected languages
        detected_languages = set()
        for group in relevant_groups + non_relevant_groups:
            for stack in group.get("SourceStacks", []):
                for source in stack.get("Sources", []):
                    lang = source.get("DetectedLanguage")
                    if lang:
                        detected_languages.add(lang)

        # Count helper
        def count_hits(path):
            section = safe_get(data, path, {})
            if isinstance(section, list):
                return len(section)
            if isinstance(section, dict):
                return sum(len(v) for v in section.values() if isinstance(v, list))
            return 0

        # Watchlist hit categories
        confirmed_sanctions = count_hits(["ScreeningSection", "Sanctions", "Confirmed"])
        confirmed_watchlists = count_hits(["ScreeningSection", "Watchlists", "Confirmed"])
        confirmed_peps = count_hits(["ScreeningSection", "Peps", "Confirmed"])
        confirmed_watchlist_hits = confirmed_sanctions + confirmed_watchlists + confirmed_peps

        review_sanctions = (
            count_hits(["ScreeningSection", "Sanctions", "Unconfirmed"]) +
            count_hits(["ScreeningSection", "PossiblePersonListings", "Sanctions"])
        )
        review_watchlists = (
            count_hits(["ScreeningSection", "Watchlists", "Unconfirmed"]) +
            count_hits(["ScreeningSection", "PossiblePersonListings", "WatchLists"])
        )
        review_peps = (
            count_hits(["ScreeningSection", "Peps", "Unconfirmed"]) +
            count_hits(["ScreeningSection", "PossiblePersonListings", "Peps"])
        )

        discarded_sanctions = count_hits(["ScreeningSection", "DisregardedPersonListings", "Sanctions"])
        discarded_watchlists = count_hits(["ScreeningSection", "DisregardedPersonListings", "WatchLists"])
        discarded_peps = count_hits(["ScreeningSection", "DisregardedPersonListings", "Peps"])

        total_watchlist_hits = (
            confirmed_watchlist_hits +
            review_sanctions + review_watchlists + review_peps +
            discarded_sanctions + discarded_watchlists + discarded_peps
        )

        # Write to CSV
        file_exists = os.path.isfile(CSV_OUTPUT_PATH)
        with open(CSV_OUTPUT_PATH, "a", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=CSV_FIELDS)
            if not file_exists:
                writer.writeheader()
            writer.writerow({
                "UserEmail": user_email,
                "Subject": subject,
                "SubjectType": subject_type,
                "RelevantSourceGroups": len(relevant_groups),
                "NonRelevantSourceGroups": len(non_relevant_groups),
                "TotalReadingTime_Relevant_Minutes": total_reading_time_relevant,
                "TotalReadingTime_NonRelevant_Minutes": total_reading_time_non_relevant,
                "TotalWatchlistHits": total_watchlist_hits,
                "ConfirmedWatchlistHits": confirmed_watchlist_hits,
                "ConfirmedSanctions": confirmed_sanctions,
                "ConfirmedWatchlists": confirmed_watchlists,
                "ConfirmedPeps": confirmed_peps,
                "ReviewSanctions": review_sanctions,
                "ReviewWatchlists": review_watchlists,
                "ReviewPeps": review_peps,
                "DiscardedSanctions": discarded_sanctions,
                "DiscardedWatchlists": discarded_watchlists,
                "DiscardedPeps": discarded_peps,
                "DetectedLanguages": ", ".join(sorted(detected_languages)) if detected_languages else "Unknown"
            })
        logging.info(f"✅ Processed: {json_path}")
    except Exception as e:
        logging.error(f"❌ Failed to process {json_path}: {e}")

def monitor_folder():
    validate_csv_headers()
    processed = load_processed_files()

    logging.info(f"👀 Watching folder: {WATCH_FOLDER}")
    while True:
        try:
            for file in os.listdir(WATCH_FOLDER):
                if file.endswith(".json") and file not in processed:
                    full_path = os.path.join(WATCH_FOLDER, file)
                    extract_json_data(full_path)
                    save_processed_file(file)
                    processed.add(file)
        except Exception as e:
            logging.error(f"Error during folder monitoring: {e}")
        time.sleep(5)

# --- RUN ---
if __name__ == "__main__":
    monitor_folder()
