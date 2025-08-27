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

# ====================== LOGGING ======================

LOG_PATH = "log.txt"
logging.basicConfig(
    filename=LOG_PATH,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
_console = logging.StreamHandler(sys.stdout)
_console.setLevel(logging.INFO)
_console.setFormatter(logging.Formatter("%(message)s"))
logging.getLogger().addHandler(_console)

# ====================== HELPERS ======================

SAFE_EXT_RE = re.compile(r"\.([A-Za-z0-9]{1,5})(?:\?|#|$)")

def hash_name(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

def guess_ext(url: str) -> str:
    m = SAFE_EXT_RE.search(url)
    return f".{m.group(1)}".lower() if m else ".json"

def ensure_dirs(root: Path):
    (root / "reports").mkdir(parents=True, exist_ok=True)
    (root / "errors").mkdir(parents=True, exist_ok=True)

def save_error_json(root: Path, guid: str, message: str, step: str, status: Optional[int] = None, body: Optional[str] = None):
    path = root / "errors" / f"{guid or ('err_'+hash_name(message))}.json"
    payload = {
        "error": message,
        "step": step,
        "status_code": status,
        "body": (body[:1000] if body else None),
        "time": time.time(),
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
    logging.warning(f"[ERROR] {guid} @ {step}: {message} (saved {path})")

# ====================== DB ======================

def db_init(db_path: Path):
    con = sqlite3.connect(db_path, check_same_thread=False)  # ✅ Fix for threading
    con.execute("""
    CREATE TABLE IF NOT EXISTS jobs (
        guid TEXT PRIMARY KEY,
        url TEXT,
        status TEXT NOT NULL,
        tries INTEGER NOT NULL DEFAULT 0,
        http_status_meta INTEGER,
        http_status_report INTEGER,
        path TEXT,
        size_bytes INTEGER,
        updated_at REAL NOT NULL
    )
    """)
    con.commit()
    return con

def db_upsert_job(con, guid, url):
    con.execute("""
        INSERT OR IGNORE INTO jobs(guid, url, status, updated_at)
        VALUES(?, ?, 'pending', ?)
    """, (guid, url, time.time()))
    con.commit()

def db_next_batch(con, n):
    cur = con.execute("""
        SELECT guid, url, tries FROM jobs
        WHERE status IN ('pending','failed') AND tries < ?
        ORDER BY updated_at ASC
        LIMIT ?
    """, (MAX_RETRIES, n))
    return cur.fetchall()

def db_mark(con, guid, **kw):
    fields = ", ".join([f"{k}=?" for k in kw.keys()])
    con.execute(f"UPDATE jobs SET {fields}, updated_at=? WHERE guid=?", (*kw.values(), time.time(), guid))
    con.commit()

def db_inc_try(con, guid):
    con.execute("UPDATE jobs SET tries = tries + 1, updated_at=? WHERE guid=?", (time.time(), guid))
    con.commit()

def db_counts(con):
    done = con.execute("SELECT COUNT(*) FROM jobs WHERE status='done'").fetchone()[0]
    pend = con.execute("SELECT COUNT(*) FROM jobs WHERE status='pending'").fetchone()[0]
    fail = con.execute("SELECT COUNT(*) FROM jobs WHERE status='failed'").fetchone()[0]
    return done, pend, fail

# ====================== Token Manager ======================

class FileTokenManager:
    def __init__(self, token_file: Path):
        self.token_file = token_file
        self._cached_token: Optional[str] = None
        self._cached_mtime: float = 0.0

    def _read_now(self) -> Optional[str]:
        if not self.token_file.exists():
            return None
        try:
            token = self.token_file.read_text(encoding="utf-8").strip()
            return token or None
        except Exception:
            return None

    def get(self) -> Optional[str]:
        try:
            mtime = self.token_file.stat().st_mtime
        except FileNotFoundError:
            mtime = 0.0
        if (self._cached_token is None) or (mtime != self._cached_mtime):
            tok = self._read_now()
            if tok:
                self._cached_token = tok
                self._cached_mtime = mtime
        return self._cached_token

    async def wait_for_update(self, old_token: Optional[str], poll_interval: float = 3.0):
        logging.info("Waiting for a fresh token...")
        while True:
            await asyncio.sleep(poll_interval)
            tok = self.get()
            if tok and tok != old_token:
                logging.info("✅ Detected new token.")
                return tok

# ====================== Networking ======================

async def fetch_metadata(session, token_mgr, guid, base_url):
    url = f"{base_url}{guid}"
    backoff = BACKOFF_BASE_S
    token_used = token_mgr.get()

    for _ in range(MAX_RETRIES):
        token = token_mgr.get()
        if not token:
            token = await token_mgr.wait_for_update(token_used)

        headers = {"Authorization": f"Bearer {token}"}
        try:
            async with session.get(url, headers=headers, timeout=REQUEST_TIMEOUT_S) as r:
                body = await r.text()
                if r.status in (401, 403):
                    token_used = token
                    await token_mgr.wait_for_update(token_used)
                    await asyncio.sleep(backoff)
                    backoff *= 2
                    continue
                if r.status in (429, 500, 502, 503, 504):
                    await asyncio.sleep(backoff)
                    backoff *= 2
                    continue
                if r.status != 200:
                    return r.status, None, body
                return r.status, json.loads(body), body
        except Exception:
            await asyncio.sleep(backoff)
            backoff *= 2
    return None, None, None

async def fetch_report(session, token_mgr, report_url, needs_auth):
    backoff = BACKOFF_BASE_S
    token_used = token_mgr.get()

    for _ in range(MAX_RETRIES):
        headers = {}
        if needs_auth:
            token = token_mgr.get()
            if not token:
                token = await token_mgr.wait_for_update(token_used)
            headers["Authorization"] = f"Bearer {token}"
        try:
            async with session.get(report_url, headers=headers, timeout=REQUEST_TIMEOUT_S) as r:
                content = await r.read()
                if r.status in (401, 403) and needs_auth:
                    token_used = headers.get("Authorization")
                    await token_mgr.wait_for_update(token_used)
                    await asyncio.sleep(backoff)
                    backoff *= 2
                    continue
                if r.status in (429, 500, 502, 503, 504):
                    await asyncio.sleep(backoff)
                    backoff *= 2
                    continue
                return r.status, content
        except Exception:
            await asyncio.sleep(backoff)
            backoff *= 2
    return None, None

def extract_guid_from_url(url: str) -> Optional[str]:
    p = urlparse(url)
    segs = [s for s in p.path.split("/") if s]
    return segs[-1] if segs else None

# ====================== Worker ======================

async def process_one(root, guid, url, session, token_mgr, con, base_url):
    status_meta, meta, body_meta = await fetch_metadata(session, token_mgr, guid, base_url)
    if status_meta != 200 or not meta:
        db_mark(con, guid, status="failed", http_status_meta=status_meta)
        save_error_json(root, guid, "Metadata fetch failed", "metadata", status_meta, body_meta)
        return

    report_url = meta.get("reportUrl")
    if not report_url:
        db_mark(con, guid, status="failed")
        save_error_json(root, guid, "Missing reportUrl", "metadata")
        return

    status_rep, content = await fetch_report(session, token_mgr, report_url, APPLY_AUTH_TO_REPORT_URL)
    if status_rep != 200 or content is None:
        db_mark(con, guid, status="failed", http_status_report=status_rep)
        save_error_json(root, guid, "Download failed", "report")
        return

    path = root / "reports" / f"{guid}{guess_ext(report_url)}"
    with open(path, "wb") as f:
        f.write(content)

    db_mark(con, guid, status="done", path=str(path), size_bytes=len(content))
    logging.info(f"[OK] {guid} -> {path}")

async def worker_pool(root, con, token_mgr, base_url, max_conc):
    timeout = aiohttp.ClientTimeout(total=None)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        sem = asyncio.Semaphore(max_conc)

        async def run_one(g, u):
            async with sem:
                db_inc_try(con, g)
                await process_one(root, g, u, session, token_mgr, con, base_url)

        while True:
            batch = db_next_batch(con, max_conc * 4)
            if not batch:
                await asyncio.sleep(5)
                continue
            tasks = [asyncio.create_task(run_one(g, u)) for (g, u, _t) in batch]
            await asyncio.gather(*tasks)
            done, pend, fail = db_counts(con)
            logging.info(f"Progress: done={done}, pending={pend}, failed={fail}")

# ====================== CSV Watcher ======================

def seed_jobs_from_csv(con, csv_path: Path, col_idx: int):
    added = 0
    seen = set()
    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        rows = list(reader)
        for row in rows:
            url = row[col_idx].strip() if len(row) > col_idx else ""
            if not url or url in seen:
                continue
            guid = extract_guid_from_url(url)
            if not guid:
                continue
            db_upsert_job(con, guid, url)
            seen.add(url)
            added += 1
    return added

def poll_csv_for_updates(csv_path: Path, con: sqlite3.Connection, col_idx=0, interval=10):
    def loop():
        while True:
            try:
                added = seed_jobs_from_csv(con, csv_path, col_idx)
                if added > 0:
                    logging.info(f"[WATCHER] Added {added} new job(s) from CSV.")
            except Exception as e:
                logging.error(f"[WATCHER ERROR] {e}")
            time.sleep(interval)
    threading.Thread(target=loop, daemon=True).start()

# ====================== Main ======================

def parse_args():
    p = ArgumentParser(description="Auto-downloader for Xapien reports.")
    p.add_argument("--csv", default=CSV_INPUT_DEFAULT)
    p.add_argument("--col", type=int, default=URL_COLUMN_INDEX_DEFAULT)
    p.add_argument("--base", default=BASE_URL_DEFAULT)
    p.add_argument("--out", default=OUTPUT_ROOT_DEFAULT)
    p.add_argument("--token-file", default=TOKEN_FILE_DEFAULT)
    p.add_argument("--concurrency", type=int, default=MAX_CONCURRENCY_DEFAULT)
    return p.parse_args()

def main():
    args = parse_args()
    root = Path(args.out)
    ensure_dirs(root)

    token_mgr = FileTokenManager(Path(args.token_file))
    con = db_init(root / "updating_downloads.db")  # ✅ New DB file name

    poll_csv_for_updates(Path(args.csv), con, args.col, interval=10)

    try:
        asyncio.run(worker_pool(root, con, token_mgr, args.base, args.concurrency))
    except KeyboardInterrupt:
        logging.warning("Interrupted by user.")
    finally:
        done, pend, fail = db_counts(con)
        logging.info(f"SUMMARY: done={done}, pending={pend}, failed={fail}")

if __name__ == "__main__":
    main()
