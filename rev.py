#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Fundamental Data Downloader (Free Float)
Version: 3.0.0

Fitur:
- Local cache first
- Fallback GitHub
- SQLite storage
- Checkpoint resume
- Incremental update
- Parallel download
- Retry & rate limit handling
- Logging
- Android/Termux friendly
"""

import time
import random
import sqlite3
import requests
import logging
import argparse
import sys
import shutil

from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ==========================================
# VERSION
# ==========================================
__version__ = "3.0.0"

# ==========================================
# DEFAULT CONFIG
# ==========================================
DEFAULT_GITHUB_RAW_BASE = (
    "https://raw.githubusercontent.com/"
    "nosrednaluapnhoj/saham/tickers/"
)

DEFAULT_GITHUB_API = (
    "https://api.github.com/repos/"
    "nosrednaluapnhoj/saham/tickers"
)

DEFAULT_BASE_STORAGE = Path("/storage/emulated/0/Saham")
DEFAULT_DB_NAME = "fundamental_data.db"

LOCAL_TICKERS_DIR = Path("./tickers")

DEFAULT_DELAY_MIN = 2
DEFAULT_DELAY_MAX = 5

DEFAULT_MAX_RETRY = 3
DEFAULT_CHECKPOINT_EVERY = 10
DEFAULT_UPDATE_AFTER_DAYS = 7

DEFAULT_MAX_WORKERS = 2

# ==========================================
# LOGGING
# ==========================================
def setup_logging(log_dir=None, verbose=False):

    if log_dir is None:
        log_dir = Path.cwd() / "logs"
    else:
        log_dir = Path(log_dir)

    log_dir.mkdir(parents=True, exist_ok=True)

    log_file = (
        log_dir /
        f"downloader_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    )

    level = logging.DEBUG if verbose else logging.INFO

    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(sys.stdout)
        ]
    )

    return logging.getLogger(__name__)

# ==========================================
# INTERNET & SESSION
# ==========================================
def create_requests_session(retries=3, backoff_factor=0.5):

    session = requests.Session()

    retry_strategy = Retry(
        total=retries,
        backoff_factor=backoff_factor,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )

    adapter = HTTPAdapter(max_retries=retry_strategy)

    session.mount("http://", adapter)
    session.mount("https://", adapter)

    return session

def check_internet_connection():

    try:
        requests.get("https://github.com", timeout=5)
        return True
    except:
        return False

# ==========================================
# LOCAL CACHE
# ==========================================
def get_local_ticker_files():

    if not LOCAL_TICKERS_DIR.exists():
        return []

    try:

        files = sorted([
            f.name
            for f in LOCAL_TICKERS_DIR.glob("*.txt")
        ])

        logging.info(f"✓ Found {len(files)} local ticker files")

        return files

    except Exception as e:
        logging.error(f"Gagal membaca local ticker dir: {e}")
        return []

def load_local_ticker_list(filename):

    file_path = LOCAL_TICKERS_DIR / filename

    if not file_path.exists():
        return []

    try:

        with open(file_path, "r", encoding="utf-8") as f:

            tickers = [
                line.strip().upper()
                for line in f
                if line.strip()
            ]

        logging.info(
            f"✓ Loaded {len(tickers)} tickers from local cache"
        )

        return tickers

    except Exception as e:
        logging.error(f"Gagal membaca local cache: {e}")
        return []

def save_local_cache(filename, tickers):

    try:

        LOCAL_TICKERS_DIR.mkdir(
            parents=True,
            exist_ok=True
        )

        file_path = LOCAL_TICKERS_DIR / filename

        with open(file_path, "w", encoding="utf-8") as f:

            for ticker in tickers:
                f.write(f"{ticker}\n")

        logging.info(f"✓ Local cache saved: {file_path}")

    except Exception as e:
        logging.warning(f"Gagal save local cache: {e}")

# ==========================================
# GITHUB
# ==========================================
def get_ticker_files(
    session,
    github_api_url=DEFAULT_GITHUB_API
):

    try:

        response = session.get(
            github_api_url,
            timeout=15
        )

        if response.status_code == 200:

            files = [
                f["name"]
                for f in response.json()
                if f["name"].endswith(".txt")
            ]

            return sorted(files)

    except Exception as e:
        logging.error(f"Gagal mengambil ticker files: {e}")

    return []

def download_ticker_list(
    session,
    base_url,
    filename
):

    url = f"{base_url}{filename}"

    try:

        response = session.get(url, timeout=15)

        if response.status_code == 200:

            content = response.content.decode("utf-8")

            tickers = [
                line.strip().upper()
                for line in content.splitlines()
                if line.strip()
            ]

            logging.info(
                f"✓ Downloaded {len(tickers)} tickers"
            )

            return tickers

    except Exception as e:
        logging.error(f"Download ticker error: {e}")

    return []

# ==========================================
# DATABASE
# ==========================================
def initialize_database(db_path):

    conn = None

    try:

        conn = sqlite3.connect(
            str(db_path),
            timeout=10.0
        )

        conn.execute("PRAGMA journal_mode=WAL")

        conn.execute("""
            CREATE TABLE IF NOT EXISTS free_float (
                Ticker TEXT PRIMARY KEY,
                "Shares Outstanding" REAL,
                "Float Shares" REAL,
                "Free Float (%)" REAL,
                Status TEXT,
                Error TEXT,
                "Last Updated" TEXT
            )
        """)

        conn.commit()

    except Exception as e:
        logging.error(f"DB init error: {e}")
        raise

    finally:
        if conn:
            conn.close()

def should_update_ticker(
    ticker,
    db_path,
    days_threshold
):

    if not db_path.exists():
        return True

    try:

        conn = sqlite3.connect(
            str(db_path),
            timeout=10.0
        )

        cursor = conn.cursor()

        cursor.execute(
            'SELECT "Last Updated" '
            'FROM free_float WHERE Ticker=?',
            (ticker,)
        )

        row = cursor.fetchone()

        conn.close()

        if not row:
            return True

        last_updated = datetime.fromisoformat(row[0])

        age = datetime.now() - last_updated

        return age.days >= days_threshold

    except Exception as e:

        logging.warning(
            f"Gagal cek update ticker {ticker}: {e}"
        )

        return True

def save_to_db(result, db_path):

    try:

        conn = sqlite3.connect(
            str(db_path),
            timeout=10.0
        )

        conn.execute("""
            INSERT OR REPLACE INTO free_float
            (
                Ticker,
                "Shares Outstanding",
                "Float Shares",
                "Free Float (%)",
                Status,
                Error,
                "Last Updated"
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            result["Ticker"],
            result["Shares Outstanding"],
            result["Float Shares"],
            result["Free Float (%)"],
            result["Status"],
            result["Error"],
            datetime.now().isoformat()
        ))

        conn.commit()
        conn.close()

        return True

    except Exception as e:

        logging.error(
            f"DB Save Error {result['Ticker']}: {e}"
        )

        return False

# ==========================================
# CHECKPOINT
# ==========================================
def load_checkpoint(checkpoint_file):

    if checkpoint_file.exists():

        try:

            with open(
                checkpoint_file,
                "r",
                encoding="utf-8"
            ) as f:

                return [
                    line.strip()
                    for line in f
                    if line.strip()
                ]

        except Exception as e:
            logging.warning(f"Gagal load checkpoint: {e}")

    return []

def save_checkpoint(processed_list, checkpoint_file):

    try:

        checkpoint_file.parent.mkdir(
            parents=True,
            exist_ok=True
        )

        with open(
            checkpoint_file,
            "w",
            encoding="utf-8"
        ) as f:

            for ticker in processed_list:
                f.write(f"{ticker}\n")

        return True

    except Exception as e:

        logging.error(
            f"Gagal save checkpoint: {e}"
        )

        return False

def remove_checkpoint(checkpoint_file):

    try:

        if checkpoint_file.exists():
            checkpoint_file.unlink()

    except Exception as e:
        logging.warning(f"Gagal hapus checkpoint: {e}")

# ==========================================
# FREE FLOAT FETCHER
# ==========================================
def get_free_float(
    ticker,
    max_retry=DEFAULT_MAX_RETRY
):

    import yfinance as yf

    last_error = None

    for attempt in range(max_retry):

        try:

            stock = yf.Ticker(ticker)

            info = stock.info

            shares_outstanding = info.get(
                "sharesOutstanding"
            )

            float_shares = info.get(
                "floatShares"
            )

            free_float_pct = None

            if (
                shares_outstanding and
                float_shares and
                shares_outstanding > 0
            ):

                free_float_pct = (
                    float_shares /
                    shares_outstanding
                ) * 100

            return {
                "Ticker": ticker,
                "Shares Outstanding":
                    shares_outstanding,
                "Float Shares":
                    float_shares,
                "Free Float (%)":
                    round(free_float_pct, 2)
                    if free_float_pct else None,
                "Status": "SUCCESS",
                "Error": None
            }

        except Exception as e:

            error_msg = str(e)
            last_error = error_msg

            lower_error = error_msg.lower()

            if (
                "rate" in lower_error or
                "429" in lower_error or
                "too many" in lower_error
            ):

                wait_time = (attempt + 1) * 30

                logging.warning(
                    f"Rate limit {ticker}, "
                    f"wait {wait_time}s"
                )

                time.sleep(wait_time)

            else:

                if attempt < max_retry - 1:

                    wait_time = random.uniform(3, 7)

                    time.sleep(wait_time)

    return {
        "Ticker": ticker,
        "Shares Outstanding": None,
        "Float Shares": None,
        "Free Float (%)": None,
        "Status": "FAILED",
        "Error": (
            last_error[:500]
            if last_error else
            "Unknown error"
        )
    }

# ==========================================
# PARALLEL
# ==========================================
def process_parallel(
    tickers,
    db_path,
    max_workers,
    delay_range
):

    results = []

    with ThreadPoolExecutor(
        max_workers=max_workers
    ) as executor:

        future_to_ticker = {
            executor.submit(
                get_free_float,
                ticker
            ): ticker
            for ticker in tickers
        }

        for future in as_completed(future_to_ticker):

            ticker = future_to_ticker[future]

            try:

                result = future.result()

                save_to_db(result, db_path)

                results.append(result)

                logging.info(
                    f"{ticker} -> "
                    f"{result['Status']}"
                )

            except Exception as e:

                logging.error(
                    f"Parallel error {ticker}: {e}"
                )

            time.sleep(
                random.uniform(
                    delay_range[0],
                    delay_range[1]
                )
            )

    return results

# ==========================================
# DISK CHECK
# ==========================================
def check_disk_space(
    path,
    required_mb=100
):

    try:

        disk_usage = shutil.disk_usage(path)

        free_mb = (
            disk_usage.free /
            (1024 * 1024)
        )

        return free_mb >= required_mb

    except:
        return True

# ==========================================
# MAIN
# ==========================================
def main():

    parser = argparse.ArgumentParser(
        description="Free Float Downloader"
    )

    parser.add_argument(
        "--storage",
        type=str,
        default=str(DEFAULT_BASE_STORAGE)
    )

    parser.add_argument(
        "--parallel",
        action="store_true"
    )

    parser.add_argument(
        "--max-workers",
        type=int,
        default=DEFAULT_MAX_WORKERS
    )

    parser.add_argument(
        "--verbose",
        action="store_true"
    )

    parser.add_argument(
        "--force-update",
        action="store_true"
    )

    args = parser.parse_args()

    global logger

    logger = setup_logging(
        verbose=args.verbose
    )

    logger.info(
        f"Fundamental Downloader "
        f"v{__version__}"
    )

    # ======================================
    # INTERNET CHECK
    # ======================================
    if not check_internet_connection():

        logger.error("Tidak ada internet")

        sys.exit(1)

    # ======================================
    # SESSION
    # ======================================
    session = create_requests_session()

    # ======================================
    # LOCAL CACHE FIRST
    # ======================================
    logger.info(
        "Mengecek local ticker cache..."
    )

    ticker_files = get_local_ticker_files()

    # fallback GitHub
    if not ticker_files:

        logger.info(
            "Local cache tidak ditemukan"
        )

        ticker_files = get_ticker_files(session)

        if not ticker_files:

            logger.error(
                "Gagal mendapatkan ticker files"
            )

            sys.exit(1)

    logger.info("Ticker files:")

    for i, f in enumerate(ticker_files, 1):
        logger.info(f"{i}. {f}")

    # ======================================
    # SELECT FILE
    # ======================================
    while True:

        try:

            choice = int(
                input(
                    f"\nPilih file "
                    f"(1-{len(ticker_files)}): "
                )
            )

            if 1 <= choice <= len(ticker_files):

                selected_file = ticker_files[
                    choice - 1
                ]

                break

        except:
            pass

    country_name = selected_file.replace(
        ".txt",
        ""
    )

    # ======================================
    # LOAD TICKERS
    # ======================================
    all_tickers = load_local_ticker_list(
        selected_file
    )

    if not all_tickers:

        logger.info(
            "Download ticker dari GitHub..."
        )

        all_tickers = download_ticker_list(
            session,
            DEFAULT_GITHUB_RAW_BASE,
            selected_file
        )

        if all_tickers:
            save_local_cache(
                selected_file,
                all_tickers
            )

    if not all_tickers:

        logger.error("Ticker kosong")

        sys.exit(1)

    # ======================================
    # STORAGE
    # ======================================
    save_dir = (
        Path(args.storage) /
        country_name
    )

    save_dir.mkdir(
        parents=True,
        exist_ok=True
    )

    db_path = save_dir / DEFAULT_DB_NAME

    checkpoint_file = (
        save_dir / "checkpoint.txt"
    )

    # ======================================
    # DISK CHECK
    # ======================================
    if not check_disk_space(save_dir):

        logger.error("Disk space tidak cukup")

        sys.exit(1)

    # ======================================
    # INIT DB
    # ======================================
    initialize_database(db_path)

    # ======================================
    # CHECKPOINT
    # ======================================
    checkpoint_tickers = (
        load_checkpoint(checkpoint_file)
        if not args.force_update
        else []
    )

    # ======================================
    # FILTER UPDATE
    # ======================================
    tickers_to_process = []

    for ticker in all_tickers:

        if ticker in checkpoint_tickers:
            continue

        if should_update_ticker(
            ticker,
            db_path,
            DEFAULT_UPDATE_AFTER_DAYS
        ):

            tickers_to_process.append(ticker)

    logger.info("=" * 60)
    logger.info(f"Country : {country_name}")
    logger.info(f"Tickers : {len(all_tickers):,}")
    logger.info(
        f"Update  : "
        f"{len(tickers_to_process):,}"
    )
    logger.info("=" * 60)

    if not tickers_to_process:

        logger.info("Semua data masih fresh")

        remove_checkpoint(checkpoint_file)

        sys.exit(0)

    confirm = input(
        f"\nUpdate "
        f"{len(tickers_to_process):,} "
        f"ticker? (y/n): "
    )

    if confirm.lower() != "y":

        logger.info("Dibatalkan")

        sys.exit(0)

    # ======================================
    # START PROCESS
    # ======================================
    start_time = time.time()

    success_count = 0
    fail_count = 0

    processed_now = checkpoint_tickers.copy()

    if args.parallel:

        batch_size = args.max_workers * 2

        for batch_idx in range(
            0,
            len(tickers_to_process),
            batch_size
        ):

            batch = tickers_to_process[
                batch_idx:
                batch_idx + batch_size
            ]

            results = process_parallel(
                batch,
                db_path,
                args.max_workers,
                (
                    DEFAULT_DELAY_MIN,
                    DEFAULT_DELAY_MAX
                )
            )

            for r in results:

                if r["Status"] == "SUCCESS":
                    success_count += 1
                else:
                    fail_count += 1

                processed_now.append(
                    r["Ticker"]
                )

            save_checkpoint(
                processed_now,
                checkpoint_file
            )

    else:

        total = len(tickers_to_process)

        for i, ticker in enumerate(
            tickers_to_process,
            1
        ):

            progress_pct = (
                i / total
            ) * 100

            logger.info(
                f"[{i}/{total}] "
                f"{progress_pct:.1f}% "
                f"{ticker}"
            )

            result = get_free_float(ticker)

            save_to_db(result, db_path)

            if result["Status"] == "SUCCESS":

                success_count += 1

            else:

                fail_count += 1

            processed_now.append(ticker)

            if i % DEFAULT_CHECKPOINT_EVERY == 0:

                save_checkpoint(
                    processed_now,
                    checkpoint_file
                )

            delay = random.uniform(
                DEFAULT_DELAY_MIN,
                DEFAULT_DELAY_MAX
            )

            time.sleep(delay)

    # ======================================
    # CLEANUP
    # ======================================
    remove_checkpoint(checkpoint_file)

    total_time = time.time() - start_time

    logger.info("=" * 60)
    logger.info("UPDATE SELESAI")
    logger.info("=" * 60)
    logger.info(f"Sukses : {success_count:,}")
    logger.info(f"Gagal  : {fail_count:,}")
    logger.info(
        f"Waktu  : "
        f"{total_time / 60:.1f} menit"
    )
    logger.info(f"Database : {db_path}")
    logger.info("=" * 60)

# ==========================================
# ENTRY POINT
# ==========================================
if __name__ == "__main__":

    try:
        main()

    except KeyboardInterrupt:

        logging.info(
            "\nProcess dihentikan user"
        )

        sys.exit(1)

    except Exception as e:

        logging.exception(
            f"FATAL ERROR: {e}"
        )

        sys.exit(1)
