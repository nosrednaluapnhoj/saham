import yfinance as yf
import pandas as pd
import time
import random
import os
import sqlite3
import sys
import concurrent.futures
from io import StringIO
import contextlib

# ==============================
# CONFIGURATION
# ==============================
TICKER_FILE = "ticker.txt"

# Simpan ke HP
DB_PATH = "/storage/emulated/0/Saham/Indonesia/freefloat.db"

DELAY_MIN = 1
DELAY_MAX = 3
MAX_RETRY = 3
MAX_WORKERS = 1

# ==============================
# SUPPRESS STDERR
# ==============================
@contextlib.contextmanager
def suppress_stderr():
    old_stderr = sys.stderr
    sys.stderr = StringIO()
    try:
        yield
    finally:
        sys.stderr = old_stderr

# ==============================
# LOAD TICKERS (CLEAN)
# ==============================
def load_tickers(filename):
    tickers = set()

    try:
        with open(filename, "r") as f:
            for line in f:
                t = line.strip().upper()
                if not t:
                    continue

                # hapus suffix (.JK, dll)
                t_clean = t.split(".")[0]
                tickers.add(t_clean)

    except FileNotFoundError:
        print(f"❌ File {filename} tidak ditemukan")
        return []

    return sorted(tickers)

# ==============================
# GET FREE FLOAT (ALWAYS UPDATE)
# ==============================
def get_free_float(ticker):
    ticker_clean = ticker.split(".")[0]
    ticker_yf = ticker_clean + ".JK"

    for attempt in range(MAX_RETRY):
        try:
            with suppress_stderr():
                stock = yf.Ticker(ticker_yf)
                info = stock.info

            shares_outstanding = info.get('sharesOutstanding')
            float_shares = info.get('floatShares')

            free_float_pct = None
            if shares_outstanding and float_shares:
                free_float_pct = (float_shares / shares_outstanding) * 100

            return {
                "Ticker": ticker_clean,
                "Shares Outstanding": shares_outstanding,
                "Float Shares": float_shares,
                "Free Float (%)": round(free_float_pct, 2) if free_float_pct else None
            }

        except Exception as e:
            error_msg = str(e).lower()

            if "rate" in error_msg or "too many" in error_msg or "429" in error_msg:
                wait_time = (attempt + 1) * 30
                print(f"⚠️ RATE LIMIT! Tunggu {wait_time} detik...")
                time.sleep(wait_time)
            else:
                print(f"⚠️ Retry {attempt+1}/{MAX_RETRY} {ticker_clean} error: {e}")
                if attempt < MAX_RETRY - 1:
                    time.sleep(random.uniform(3, 7))

    return {
        "Ticker": ticker_clean,
        "Shares Outstanding": None,
        "Float Shares": None,
        "Free Float (%)": None
    }

# ==============================
# SAVE TO SQLITE (REPLACE = UPDATE)
# ==============================
def append_to_db(result, db_path):
    try:
        conn = sqlite3.connect(db_path)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS free_float (
                Ticker TEXT PRIMARY KEY,
                "Shares Outstanding" REAL,
                "Float Shares" REAL,
                "Free Float (%)" REAL
            )
        """)

        conn.execute("""
            INSERT OR REPLACE INTO free_float
            (Ticker, "Shares Outstanding", "Float Shares", "Free Float (%)")
            VALUES (?, ?, ?, ?)
        """, (
            result['Ticker'],
            result['Shares Outstanding'],
            result['Float Shares'],
            result['Free Float (%)']
        ))

        conn.commit()
        conn.close()
        return True

    except Exception as e:
        print(f"❌ Save error {result['Ticker']}: {e}")
        return False

# ==============================
# MAIN
# ==============================
def main():
    if not os.path.exists(TICKER_FILE):
        print("❌ ticker.txt tidak ditemukan")
        return

    tickers = load_tickers(TICKER_FILE)

    if not tickers:
        print("❌ Tidak ada ticker")
        return

    print(f"\n{'='*50}")
    print(f"TOTAL TICKER : {len(tickers)}")
    print(f"MODE         : ALWAYS UPDATE")
    print(f"DB PATH      : {DB_PATH}")
    print(f"{'='*50}\n")

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(get_free_float, t): t for t in tickers}

        for i, future in enumerate(concurrent.futures.as_completed(futures)):
            ticker = futures[future]

            try:
                result = future.result()
                saved = append_to_db(result, DB_PATH)

                status = "OK" if result['Free Float (%)'] else "NO DATA"

                print(f"[{i+1}/{len(tickers)}] {result['Ticker']} -> {status}")

            except Exception as e:
                print(f"{ticker} ERROR: {e}")

            delay = random.uniform(DELAY_MIN, DELAY_MAX)
            time.sleep(delay)

    print("\n✅ DONE")
    print(f"💾 Database: {DB_PATH}")

# ==============================
if __name__ == "__main__":
    main()