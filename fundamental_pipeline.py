import yfinance as yf
import pandas as pd
import time
import random
import os
import sqlite3
import sys
from datetime import datetime
import requests

# ==============================
# CONFIGURATION
# ==============================
GITHUB_RAW_BASE = "https://raw.githubusercontent.com/nosrednaluapnhoj/saham/main/tickers/"
BASE_STORAGE = "/storage/emulated/0/Saham"
DB_NAME = "fundamental_data.db"

DELAY_MIN = 2
DELAY_MAX = 4
MAX_RETRY = 3
CHECKPOINT_EVERY = 10  # SAVE CHECKPOINT EVERY 10 TICKERS

# ==============================
# FUNCTIONS
# ==============================
def get_ticker_files():
    """Fetch list of .txt files from tickers folder on GitHub"""
    api_url = "https://api.github.com/repos/nosrednaluapnhoj/saham/contents/tickers"
    try:
        response = requests.get(api_url, timeout=10)
        if response.status_code == 200:
            files = [f['name'] for f in response.json() if f['name'].endswith('.txt')]
            return sorted(files)
    except:
        pass
    return []

def download_ticker_list(filename):
    """Download ticker list from a specific file"""
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

def get_processed_tickers_from_db(db_path):
    """Get list of already processed tickers from database (source of truth)"""
    if not os.path.exists(db_path):
        return []
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='free_float'")
        if cursor.fetchone():
            cursor.execute("SELECT Ticker FROM free_float")
            processed = [row[0] for row in cursor.fetchall()]
        else:
            processed = []
        
        conn.close()
        return processed
    except Exception as e:
        print(f"⚠️ Error reading DB: {e}")
        return []

def load_checkpoint(checkpoint_file):
    """Load ticker list from checkpoint file (backup for quick resume)"""
    if os.path.exists(checkpoint_file):
        try:
            with open(checkpoint_file, 'r') as f:
                return [line.strip() for line in f if line.strip()]
        except:
            pass
    return []

def save_checkpoint(processed_list, checkpoint_file):
    """Save checkpoint (OVERWRITE, not append)"""
    try:
        with open(checkpoint_file, 'w') as f:
            for ticker in processed_list:
                f.write(f"{ticker}\n")
        return True
    except Exception as e:
        print(f"⚠️ Failed to save checkpoint: {e}")
        return False

def remove_checkpoint(checkpoint_file):
    """Delete checkpoint file after completion"""
    try:
        if os.path.exists(checkpoint_file):
            os.remove(checkpoint_file)
            return True
    except:
        pass
    return False

def get_free_float(ticker):
    """Get free float data for one ticker"""
    for attempt in range(MAX_RETRY):
        try:
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
                print(f"\n⚠️ RATE LIMIT! Waiting {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                if attempt < MAX_RETRY - 1:
                    time.sleep(random.uniform(3, 7))

    return {
        "Ticker": ticker,
        "Shares Outstanding": None,
        "Float Shares": None,
        "Free Float (%)": None
    }

def save_to_db(result, db_path):
    """Save single result to database"""
    try:
        conn = sqlite3.connect(db_path)
        
        conn.execute("""
            CREATE TABLE IF NOT EXISTS free_float (
                Ticker TEXT PRIMARY KEY,
                "Shares Outstanding" REAL,
                "Float Shares" REAL,
                "Free Float (%)" REAL,
                "Last Updated" TEXT
            )
        """)

        conn.execute("""
            INSERT OR REPLACE INTO free_float
            (Ticker, "Shares Outstanding", "Float Shares", "Free Float (%)", "Last Updated")
            VALUES (?, ?, ?, ?, ?)
        """, (
            result['Ticker'],
            result['Shares Outstanding'],
            result['Float Shares'],
            result['Free Float (%)'],
            datetime.now().isoformat()
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
    print("\n" + "="*60)
    print("📊 FUNDAMENTAL DATA DOWNLOADER")
    print("="*60)
    
    ticker_files = get_ticker_files()
    
    if not ticker_files:
        print("❌ Unable to fetch file list from GitHub")
        return
    
    print("\n📁 Available ticker files:")
    for i, f in enumerate(ticker_files, 1):
        print(f"  {i}. {f}")
    
    while True:
        try:
            choice = int(input("\nSelect file number: "))
            if 1 <= choice <= len(ticker_files):
                selected_file = ticker_files[choice-1]
                break
        except:
            print("❌ Please enter a valid number")
    
    country_name = selected_file.replace('.txt', '')
    all_tickers = download_ticker_list(selected_file)
    
    if not all_tickers:
        print("❌ No tickers found")
        return
    
    save_dir = f"{BASE_STORAGE}/{country_name}"
    os.makedirs(save_dir, exist_ok=True)
    db_path = f"{save_dir}/{DB_NAME}"
    checkpoint_file = f"{save_dir}/checkpoint.txt"
    
    print("\n🔍 Checking existing data...")
    processed_from_db = get_processed_tickers_from_db(db_path)
    processed_from_checkpoint = load_checkpoint(checkpoint_file)
    
    processed_tickers = list(set(processed_from_db + processed_from_checkpoint))
    tickers_to_process = [t for t in all_tickers if t not in processed_tickers]
    
    print("\n" + "="*60)
    print(f"📊 DOWNLOAD STATISTICS")
    print("="*60)
    print(f"🌏 Country          : {country_name}")
    print(f"📈 Total tickers   : {len(all_tickers):,}")
    print(f"✅ Processed       : {len(processed_tickers):,}")
    print(f"🔄 Remaining       : {len(tickers_to_process):,}")
    print(f"💾 Database path   : {db_path}")
    if os.path.exists(checkpoint_file):
        print(f"📝 Checkpoint found: {checkpoint_file}")
    print("="*60)
    
    if not tickers_to_process:
        print("\n✅ ALL TICKERS ALREADY PROCESSED!")
        if remove_checkpoint(checkpoint_file):
            print("📝 Checkpoint file removed (job complete)")
        return
    
    confirm = input(f"\nProceed to download {len(tickers_to_process):,} tickers? (y/n): ")
    if confirm.lower() != 'y':
        print("❌ Cancelled")
        return
    
    print("\n🚀 Starting download...\n")
    success_count = 0
    fail_count = 0
    temp_processed = processed_tickers.copy()
    start_time = time.time()
    
    for i, ticker in enumerate(tickers_to_process):
        current_num = len(processed_tickers) + i + 1
        progress_pct = (current_num / len(all_tickers)) * 100
        
        print(f"[{current_num:>5}/{len(all_tickers)}] ({progress_pct:>5.1f}%) {ticker:<10} ", end="")
        
        result = get_free_float(ticker)
        saved = save_to_db(result, db_path)
        
        if result['Free Float (%)'] is not None:
            success_count += 1
            print(f"✓ {result['Free Float (%)']:>6.2f}%")
        else:
            fail_count += 1
            print(f"✗ NO DATA")
        
        temp_processed.append(ticker)
        
        if (i + 1) % CHECKPOINT_EVERY == 0:
            save_checkpoint(temp_processed, checkpoint_file)
            elapsed = time.time() - start_time
            avg_time = elapsed / (i + 1)
            remaining = avg_time * (len(tickers_to_process) - i - 1)
            print(f"  💾 Checkpoint saved ({current_num}/{len(all_tickers)}) | ETA: {remaining/60:.1f} minutes")
        
        delay = random.uniform(DELAY_MIN, DELAY_MAX)
        time.sleep(delay)
    
    print("\n" + "="*60)
    print("✅ PROCESS COMPLETED!")
    print("="*60)
    print(f"📊 New successful tickers : {success_count:,}")
    print(f"⚠️  New failed tickers    : {fail_count:,}")
    print(f"📈 Total in database      : {len(all_tickers):,}")
    print(f"💾 Database location      : {db_path}")
    
    if remove_checkpoint(checkpoint_file):
        print("📝 Checkpoint file removed (job complete)")
    
    total_time = time.time() - start_time
    print(f"⏱️  Execution time        : {total_time/60:.1f} minutes")
    print("="*60)

# ==============================
if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n" + "="*60)
        print("⚠️ PROCESS INTERRUPTED BY USER")
        print("="*60)
        print("✅ Processed data is safely stored in database")
        print("📝 Checkpoint has also been saved")
        print("▶️ Run the script again to resume from last ticker")
        print("="*60)
        
        if 'temp_processed' in locals() and 'checkpoint_file' in locals():
            save_checkpoint(temp_processed, checkpoint_file)
            print("💾 Checkpoint saved, you can resume later")
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        print("✅ Data is safe, rerun to resume")
        if 'temp_processed' in locals() and 'checkpoint_file' in locals():
            save_checkpoint(temp_processed, checkpoint_file)
            print("💾 Checkpoint saved")
