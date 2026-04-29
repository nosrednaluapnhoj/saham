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
import requests

# ==============================
# CONFIGURATION
# ==============================
GITHUB_RAW_BASE = "https://raw.githubusercontent.com/nosrednaluapnhoj/saham/main/data/tickers/"
BASE_STORAGE = "/storage/emulated/0/Saham"
DB_NAME = "fundamental_data.db"

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
# GET TICKER FILES FROM GITHUB
# ==============================
def get_ticker_files():
    """Ambil daftar file .txt dari folder data/tickers di GitHub"""
    api_url = "https://api.github.com/repos/nosrednaluapnhoj/saham/contents/data/tickers"
    try:
        response = requests.get(api_url, timeout=10)
        if response.status_code == 200:
            files = [f['name'] for f in response.json() if f['name'].endswith('.txt')]
            return sorted(files)
    except:
        pass
    return []

# ==============================
# DOWNLOAD TICKER LIST
# ==============================
def download_ticker_list(filename):
    """Download daftar ticker dari file tertentu (sudah include suffix)"""
    url = f"{GITHUB_RAW_BASE}{filename}"
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            tickers = [line.strip().upper() for line in response.text.splitlines() if line.strip()]
            print(f"✓ Loaded {len(tickers)} tickers from {filename}")
            return tickers
    except Exception as e:
        print(f"✗ Error: {e}")
    return []

# ==============================
# GET FREE FLOAT
# ==============================
def get_free_float(ticker):
    """Get free float data untuk satu ticker (ticker sudah include suffix)"""
    for attempt in range(MAX_RETRY):
        try:
            with suppress_stderr():
                stock = yf.Ticker(ticker)
                info = stock.info

            shares_outstanding = info.get('sharesOutstanding')
            float_shares = info.get('floatShares')

            free_float_pct = None
            if shares_outstanding and float_shares and shares_outstanding > 0:
                free_float_pct = (float_shares / shares_outstanding) * 100

            return {
                "Ticker": ticker,
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
                print(f"⚠️ Retry {attempt+1}/{MAX_RETRY} {ticker} error: {e}")
                if attempt < MAX_RETRY - 1:
                    time.sleep(random.uniform(3, 7))

    return {
        "Ticker": ticker,
        "Shares Outstanding": None,
        "Float Shares": None,
        "Free Float (%)": None
    }

# ==============================
# SAVE TO SQLITE
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
    # Ambil daftar file ticker dari GitHub
    ticker_files = get_ticker_files()
    
    if not ticker_files:
        print("❌ Tidak bisa mengambil daftar file dari GitHub")
        return
    
    # Tampilkan pilihan
    print("\n📁 File ticker yang tersedia:")
    for i, f in enumerate(ticker_files, 1):
        print(f"  {i}. {f}")
    
    # Input pilihan
    while True:
        try:
            choice = int(input("\nPilih nomor file: "))
            if 1 <= choice <= len(ticker_files):
                selected_file = ticker_files[choice-1]
                break
        except:
            print("❌ Masukkan nomor yang benar")
    
    # Download ticker list
    country_name = selected_file.replace('.txt', '')
    tickers = download_ticker_list(selected_file)
    
    if not tickers:
        print("❌ Tidak ada ticker")
        return
    
    # Buat folder penyimpanan
    save_dir = f"{BASE_STORAGE}/{country_name}"
    os.makedirs(save_dir, exist_ok=True)
    db_path = f"{save_dir}/{DB_NAME}"
    
    print(f"\n{'='*50}")
    print(f"NEGARA        : {country_name}")
    print(f"TOTAL TICKER  : {len(tickers)}")
    print(f"DB PATH       : {db_path}")
    print(f"{'='*50}\n")
    
    # Proses dengan ThreadPool
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(get_free_float, t): t for t in tickers}
        
        success_count = 0
        for i, future in enumerate(concurrent.futures.as_completed(futures)):
            ticker = futures[future]
            
            try:
                result = future.result()
                saved = append_to_db(result, db_path)
                
                if result['Free Float (%)'] is not None:
                    success_count += 1
                    status = f"OK ({result['Free Float (%)']}%)"
                else:
                    status = "NO DATA"
                
                print(f"[{i+1}/{len(tickers)}] {result['Ticker']} -> {status}")
                
            except Exception as e:
                print(f"[{i+1}/{len(tickers)}] {ticker} ERROR: {e}")
            
            delay = random.uniform(DELAY_MIN, DELAY_MAX)
            time.sleep(delay)
    
    print(f"\n{'='*50}")
    print(f"✅ SELESAI")
    print(f"📊 Berhasil: {success_count}/{len(tickers)} ticker")
    print(f"💾 Database: {db_path}")
    print(f"{'='*50}")

# ==============================
if __name__ == "__main__":
    main()
