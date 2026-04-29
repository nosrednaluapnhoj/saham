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
GITHUB_RAW_BASE = "https://raw.githubusercontent.com/nosrednaluapnhoj/saham/main/data/tickers/"
BASE_STORAGE = "/storage/emulated/0/Saham"
DB_NAME = "fundamental_data.db"

DELAY_MIN = 2
DELAY_MAX = 4
MAX_RETRY = 3
CHECKPOINT_EVERY = 10  # SAVE CHECKPOINT SETIAP 10 TICKER

# ==============================
# FUNCTIONS
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

def download_ticker_list(filename):
    """Download daftar ticker dari file tertentu"""
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
    """Ambil daftar ticker yang sudah diproses dari database (source of truth)"""
    if not os.path.exists(db_path):
        return []
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Cek apakah tabel ada
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
    """Load daftar ticker dari file checkpoint (backup untuk resume cepat)"""
    if os.path.exists(checkpoint_file):
        try:
            with open(checkpoint_file, 'r') as f:
                return [line.strip() for line in f if line.strip()]
        except:
            pass
    return []

def save_checkpoint(processed_list, checkpoint_file):
    """Simpan checkpoint (OVERWRITE, bukan append)"""
    try:
        with open(checkpoint_file, 'w') as f:
            for ticker in processed_list:
                f.write(f"{ticker}\n")
        return True
    except Exception as e:
        print(f"⚠️ Gagal save checkpoint: {e}")
        return False

def remove_checkpoint(checkpoint_file):
    """Hapus checkpoint file setelah semua selesai"""
    try:
        if os.path.exists(checkpoint_file):
            os.remove(checkpoint_file)
            return True
    except:
        pass
    return False

def get_free_float(ticker):
    """Get free float data untuk satu ticker"""
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
        
        # Create table if not exists
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
    print("📊 FREE FLOAT DOWNLOADER WITH CHECKPOINT RESUME")
    print("="*60)
    
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
    
    # Setup
    country_name = selected_file.replace('.txt', '')
    all_tickers = download_ticker_list(selected_file)
    
    if not all_tickers:
        print("❌ Tidak ada ticker")
        return
    
    save_dir = f"{BASE_STORAGE}/{country_name}"
    os.makedirs(save_dir, exist_ok=True)
    db_path = f"{save_dir}/{DB_NAME}"
    checkpoint_file = f"{save_dir}/checkpoint.txt"
    
    # Ambil daftar ticker yang sudah diproses
    print("\n🔍 Memeriksa data yang sudah ada...")
    processed_from_db = get_processed_tickers_from_db(db_path)
    processed_from_checkpoint = load_checkpoint(checkpoint_file)
    
    # Gabungkan dan unique (prioritaskan database sebagai source of truth)
    processed_tickers = list(set(processed_from_db + processed_from_checkpoint))
    
    # Filter ticker yang belum diproses
    tickers_to_process = [t for t in all_tickers if t not in processed_tickers]
    
    # Statistik
    print("\n" + "="*60)
    print(f"📊 STATISTIK DOWNLOAD")
    print("="*60)
    print(f"🌏 Negara           : {country_name}")
    print(f"📈 Total ticker     : {len(all_tickers):,}")
    print(f"✅ Sudah diproses   : {len(processed_tickers):,}")
    print(f"🔄 Belum diproses   : {len(tickers_to_process):,}")
    print(f"💾 Database path    : {db_path}")
    if os.path.exists(checkpoint_file):
        print(f"📝 Checkpoint found : {checkpoint_file}")
    print("="*60)
    
    if not tickers_to_process:
        print("\n✅ SEMUA TICKER SUDAH DIPROSES!")
        # Hapus checkpoint jika ada
        if remove_checkpoint(checkpoint_file):
            print("📝 Checkpoint file dihapus (pekerjaan selesai)")
        return
    
    # Konfirmasi lanjut
    confirm = input(f"\nLanjut download {len(tickers_to_process):,} ticker? (y/n): ")
    if confirm.lower() != 'y':
        print("❌ Dibatalkan")
        return
    
    # Proses ticker yang belum
    print("\n🚀 Memulai download...\n")
    success_count = 0
    fail_count = 0
    temp_processed = processed_tickers.copy()
    start_time = time.time()
    
    for i, ticker in enumerate(tickers_to_process):
        current_num = len(processed_tickers) + i + 1
        progress_pct = (current_num / len(all_tickers)) * 100
        
        # Tampilkan progress
        print(f"[{current_num:>5}/{len(all_tickers)}] ({progress_pct:>5.1f}%) {ticker:<10} ", end="")
        
        # Download data
        result = get_free_float(ticker)
        saved = save_to_db(result, db_path)
        
        # Update status
        if result['Free Float (%)'] is not None:
            success_count += 1
            print(f"✓ {result['Free Float (%)']:>6.2f}%")
        else:
            fail_count += 1
            print(f"✗ NO DATA")
        
        # Update processed list
        temp_processed.append(ticker)
        
        # CHECKPOINT: Simpan setiap 10 ticker
        if (i + 1) % CHECKPOINT_EVERY == 0:
            save_checkpoint(temp_processed, checkpoint_file)
            elapsed = time.time() - start_time
            avg_time = elapsed / (i + 1)
            remaining = avg_time * (len(tickers_to_process) - i - 1)
            print(f"  💾 Checkpoint saved ({current_num}/{len(all_tickers)}) | ETA: {remaining/60:.1f} menit")
        
        # Random delay untuk hindari rate limit
        delay = random.uniform(DELAY_MIN, DELAY_MAX)
        time.sleep(delay)
    
    # FINAL STEP: Semua selesai
    print("\n" + "="*60)
    print("✅ PROSES SELESAI!")
    print("="*60)
    print(f"📊 Ticker baru sukses : {success_count:,}")
    print(f"⚠️  Ticker baru gagal  : {fail_count:,}")
    print(f"📈 Total di database  : {len(all_tickers):,}")
    print(f"💾 Database lokasi    : {db_path}")
    
    # Hapus checkpoint karena semua sudah selesai
    if remove_checkpoint(checkpoint_file):
        print("📝 Checkpoint file dihapus (pekerjaan selesai)")
    
    # Tampilkan waktu eksekusi
    total_time = time.time() - start_time
    print(f"⏱️  Waktu eksekusi     : {total_time/60:.1f} menit")
    print("="*60)

# ==============================
if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n" + "="*60)
        print("⚠️ PROSES DIHENTIKAN OLEH USER")
        print("="*60)
        print("✅ Data ticker yang sudah diproses TERSIMPAN di database")
        print("📝 Checkpoint juga telah disimpan")
        print("▶️ Jalankan ulang script untuk melanjutkan dari ticker terakhir")
        print("="*60)
        
        # Tetap simpan checkpoint saat interrupt
        if 'temp_processed' in locals() and 'checkpoint_file' in locals():
            save_checkpoint(temp_processed, checkpoint_file)
            print("💾 Checkpoint saved, you can resume later")
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        print("✅ Data aman, jalankan ulang untuk resume")
        if 'temp_processed' in locals() and 'checkpoint_file' in locals():
            save_checkpoint(temp_processed, checkpoint_file)
            print("💾 Checkpoint saved")
