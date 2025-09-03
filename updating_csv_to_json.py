# updating_csv_to_json.py

import asyncio
import aiohttp
import csv
import hashlib
import json
import logging
import os
import random
import re
import sqlite3
import sys
import time
import threading
from argparse import ArgumentParser
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

# ====================== CONFIG ======================

CSV_INPUT_DEFAULT        = "updating_urls.csv"
URL_COLUMN_INDEX_DEFAULT = 0
BASE_URL_DEFAULT         = "https://api.identity.di-demo.xapien.com/reports/"
OUTPUT_ROOT_DEFAULT      = "all_downloads"
TOKEN_FILE_DEFAULT       = "token.txt"

MAX_CONCURRENCY_DEFAULT  = 24
REQUEST_TIMEOUT_S        = 60
MAX_RETRIES              = 5
BACKOFF_BASE_S           = 1.5
JITTER_S                 = 0.25
APPLY_AUTH_TO_REPORT_URL = False

MAX_POLLS = 1  # Limit for CI

# ====================== LOGGING ======================

LOG_PATH = "log.txt"
logging.basicConfig(
    filename=LOG_PATH,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ... [rest of the file unchanged until CSV watcher] ...

def poll_csv_for_updates(csv_path: Path, con: sqlite3.Connection, col_idx=0, interval=10, max_polls=MAX_POLLS):
    def loop():
        for _ in range(max_polls):
            try:
                added = seed_jobs_from_csv(con, csv_path, col_idx)
                if added > 0:
                    logging.info(f"[WATCHER] Added {added} new job(s) from CSV.")
            except Exception as e:
                logging.error(f"[WATCHER ERROR] {e}")
            time.sleep(interval)
    threading.Thread(target=loop, daemon=True).start()

async def worker_pool(root, con, token_mgr, base_url, max_conc, max_polls=MAX_POLLS):
    timeout = aiohttp.ClientTimeout(total=None)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        sem = asyncio.Semaphore(max_conc)
        async def run_one(g, u):
            async with sem:
                db_inc_try(con, g)
                await process_one(root, g, u, session, token_mgr, con, base_url)
        for _ in range(max_polls):
            batch = db_next_batch(con, max_conc * 4)
            if not batch:
                await asyncio.sleep(5)
                continue
            tasks = [asyncio.create_task(run_one(g, u)) for (g, u, _t) in batch]
            await asyncio.gather(*tasks)
            done, pend, fail = db_counts(con)
            logging.info(f"Progress: done={done}, pending={pend}, failed={fail}")

def main():
    args = parse_args()
    root = Path(args.out)
    ensure_dirs(root)
    token_mgr = FileTokenManager(Path(args.token_file))
    con = db_init(root / "updating_downloads.db")
    poll_csv_for_updates(Path(args.csv), con, args.col, interval=10, max_polls=MAX_POLLS)
    try:
        asyncio.run(worker_pool(root, con, token_mgr, args.base, args.concurrency, max_polls=MAX_POLLS))
    except KeyboardInterrupt:
        logging.warning("Interrupted by user.")
    finally:
        done, pend, fail = db_counts(con)
        logging.info(f"SUMMARY: done={done}, pending={pend}, failed={fail}")

if __name__ == "__main__":
    main()
