#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Fundamental Data Downloader (Free Float)
Mengunduh data free float saham dari Yahoo Finance
Mendukung checkpoint, resume, update incremental, dan parallel download opsional.
"""

import yfinance as yf
import pandas as pd
import time
import random
import os
import sqlite3
import requests
import logging
import argparse
import sys
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ==============================
# VERSI
# ==============================
__version__ = "2.0.0"

# ==============================
# DEFAULT KONFIGURASI
# ==============================
DEFAULT_GITHUB_RAW_BASE = "https://raw.githubusercontent.com/nosrednaluapnhoj/saham/main/tickers/"
DEFAULT_BASE_STORAGE = Path("/storage/emulated/0/Saham")  # untuk Android
DEFAULT_DB_NAME = "fundamental_data.db"
DEFAULT_DELAY_MIN = 1.5
DEFAULT_DELAY_MAX = 3.0
DEFAULT_MAX_RETRY = 3
DEFAULT_CHECKPOINT_EVERY = 10
DEFAULT_UPDATE_AFTER_DAYS = 7
DEFAULT_MAX_WORKERS = 3  # parallel thread, hati-hati rate limit
DEFAULT_RATE_LIMIT_BACKOFF_FACTOR = 2

# ==============================
# SETUP LOGGING
# ==============================
def setup_logging(log_dir=None, verbose=False):
    """Setup logging ke file dan console"""
    if log_dir is None:
        log_dir = Path.cwd() / "logs"
    else:
        log_dir = Path(log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)
    
    log_file = log_dir / f"downloader_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    
    level = logging.DEBUG if verbose else logging.INFO
    
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)

# ==============================
# KONEKSI INTERNET & SESSION
# ==============================
def create_requests_session(retries=3, backoff_factor=0.5):
    """Buat requests session dengan retry strategy"""
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
    """Cek koneksi internet dengan mencoba ping ke Google DNS atau GitHub"""
    try:
        requests.get("https://8.8.8.8", timeout=5)
        return True
    except:
        try:
            requests.get("https://github.com", timeout=5)
            return True
        except:
            return False

# ==============================
# FUNGSI UTAMA
# ==============================
def get_ticker_files(session, github_api_url="https://api.github.com/repos/nosrednaluapnhoj/saham/contents/tickers"):
    """Fetch daftar file ticker dari GitHub"""
    try:
        response = session.get(github_api_url, timeout=15)
        if response.status_code == 200:
            files = [f['name'] for f in response.json() if f['name'].endswith('.txt')]
            return sorted(files)
    except Exception as e:
        logging.error(f"Gagal mengambil daftar file ticker: {e}")
    return []

def download_ticker_list(session, base_url, filename):
    """Download daftar ticker dari raw GitHub"""
    url = f"{base_url}{filename}"
    try:
        response = session.get(url, timeout=15)
        if response.status_code == 200:
            # Decode dengan UTF-8
            content = response.content.decode('utf-8')
            tickers = [line.strip().upper() for line in content.splitlines() if line.strip()]
            logging.info(f"✓ Loaded {len(tickers)} tickers from {filename}")
            return tickers
    except Exception as e:
        logging.error(f"Download error untuk {filename}: {e}")
    return []

def initialize_database(db_path):
    """Inisialisasi database SQLite dengan timeout dan WAL mode"""
    conn = None
    try:
        conn = sqlite3.connect(str(db_path), timeout=10.0)
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
        logging.debug(f"Database initialized: {db_path}")
    except Exception as e:
        logging.error(f"Gagal init database: {e}")
        raise
    finally:
        if conn:
            conn.close()

def should_update_ticker(ticker, db_path, days_threshold):
    """Cek apakah ticker perlu diupdate berdasarkan umur data"""
    if not db_path.exists():
        return True
    try:
        conn = sqlite3.connect(str(db_path), timeout=10.0)
        cursor = conn.cursor()
        cursor.execute('SELECT "Last Updated" FROM free_float WHERE Ticker=?', (ticker,))
        row = cursor.fetchone()
        conn.close()
        if not row:
            return True
        last_updated = datetime.fromisoformat(row[0])
        age = datetime.now() - last_updated
        return age.days >= days_threshold
    except Exception as e:
        logging.warning(f"Gagal cek update untuk {ticker}: {e}")
        return True  # amankan: anggap perlu update

def load_checkpoint(checkpoint_file):
    """Load daftar ticker yang sudah diproses dari checkpoint file"""
    if checkpoint_file.exists():
        try:
            with open(checkpoint_file, 'r', encoding='utf-8') as f:
                return [line.strip() for line in f if line.strip()]
        except Exception as e:
            logging.warning(f"Gagal load checkpoint: {e}")
    return []

def save_checkpoint(processed_list, checkpoint_file):
    """Simpan checkpoint"""
    try:
        checkpoint_file.parent.mkdir(parents=True, exist_ok=True)
        with open(checkpoint_file, 'w', encoding='utf-8') as f:
            for ticker in processed_list:
                f.write(f"{ticker}\n")
        logging.debug(f"Checkpoint saved: {len(processed_list)} tickers")
        return True
    except Exception as e:
        logging.error(f"Gagal simpan checkpoint: {e}")
        return False

def remove_checkpoint(checkpoint_file):
    """Hapus checkpoint setelah selesai sukses"""
    try:
        if checkpoint_file.exists():
            checkpoint_file.unlink()
            return True
    except Exception as e:
        logging.warning(f"Gagal hapus checkpoint: {e}")
    return False

def get_free_float(ticker, max_retry=DEFAULT_MAX_RETRY):
    """Fetch free float data dengan retry dan rate limit handling"""
    last_error = None
    for attempt in range(max_retry):
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
                "Free Float (%)": round(free_float_pct, 2) if free_float_pct else None,
                "Status": "SUCCESS",
                "Error": None
            }
        except Exception as e:
            error_msg = str(e)
            last_error = error_msg
            lower_error = error_msg.lower()
            # Deteksi rate limit
            if "rate" in lower_error or "too many" in lower_error or "429" in lower_error:
                wait_time = (attempt + 1) * 30
                logging.warning(f"Rate limit pada {ticker}, menunggu {wait_time}s...")
                time.sleep(wait_time)
            else:
                if attempt < max_retry - 1:
                    wait_time = random.uniform(3, 7)
                    logging.debug(f"Retry {attempt+1}/{max_retry} untuk {ticker} setelah {wait_time:.1f}s")
                    time.sleep(wait_time)
    # Gagal total
    return {
        "Ticker": ticker,
        "Shares Outstanding": None,
        "Float Shares": None,
        "Free Float (%)": None,
        "Status": "FAILED",
        "Error": last_error[:500] if last_error else "Unknown error"
    }

def save_to_db(result, db_path):
    """Simpan hasil ke database"""
    try:
        conn = sqlite3.connect(str(db_path), timeout=10.0)
        conn.execute("""
            INSERT OR REPLACE INTO free_float
            (Ticker, "Shares Outstanding", "Float Shares", "Free Float (%)", Status, Error, "Last Updated")
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            result['Ticker'],
            result['Shares Outstanding'],
            result['Float Shares'],
            result['Free Float (%)'],
            result['Status'],
            result['Error'],
            datetime.now().isoformat()
        ))
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        logging.error(f"DB Save Error untuk {result['Ticker']}: {e}")
        return False

def process_single_ticker(ticker, db_path, delay_range=(DEFAULT_DELAY_MIN, DEFAULT_DELAY_MAX)):
    """Proses satu ticker dan simpan (untuk mode sequential)"""
    logging.debug(f"Memproses {ticker}")
    result = get_free_float(ticker)
    save_to_db(result, db_path)
    # Delay
    time.sleep(random.uniform(*delay_range))
    return result

def process_parallel(tickers, db_path, max_workers, delay_range):
    """Proses beberapa ticker secara paralel (hati-hati rate limit)"""
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_ticker = {executor.submit(get_free_float, ticker): ticker for ticker in tickers}
        for future in as_completed(future_to_ticker):
            ticker = future_to_ticker[future]
            try:
                result = future.result()
                save_to_db(result, db_path)
                results.append(result)
                # Delay tambahan antar thread? Lebih baik tidak, karena sudah delay internal di get_free_float
                logging.debug(f"Selesai {ticker}: {result['Status']}")
            except Exception as e:
                logging.error(f"Exception pada {ticker}: {e}")
                # Simpan sebagai failed
                fail_result = {"Ticker": ticker, "Status": "FAILED", "Error": str(e)[:500]}
                save_to_db(fail_result, db_path)
                results.append(fail_result)
            time.sleep(random.uniform(delay_range[0], delay_range[1]))  # tetap beri jeda antar submit
    return results

def check_disk_space(path, required_mb=100):
    """Cek ruang disk minimum (dalam MB)"""
    try:
        disk_usage = shutil.disk_usage(path)
        free_mb = disk_usage.free / (1024 * 1024)
        if free_mb < required_mb:
            logging.warning(f"Ruang disk tersisa {free_mb:.1f} MB, kurang dari {required_mb} MB")
            return False
        return True
    except:
        return True  # jika gagal cek, lanjutkan saja

# ==============================
# MAIN
# ==============================
def main():
    parser = argparse.ArgumentParser(description="Download fundamental data (free float) dari Yahoo Finance")
    parser.add_argument("--storage", type=str, default=str(DEFAULT_BASE_STORAGE), help="Base storage path")
    parser.add_argument("--github-base", type=str, default=DEFAULT_GITHUB_RAW_BASE, help="GitHub raw base URL")
    parser.add_argument("--update-after-days", type=int, default=DEFAULT_UPDATE_AFTER_DAYS, help="Update jika lebih dari N hari")
    parser.add_argument("--delay-min", type=float, default=DEFAULT_DELAY_MIN, help="Minimum delay antar request (detik)")
    parser.add_argument("--delay-max", type=float, default=DEFAULT_DELAY_MAX, help="Maximum delay antar request (detik)")
    parser.add_argument("--max-retry", type=int, default=DEFAULT_MAX_RETRY, help="Maksimum retry per ticker")
    parser.add_argument("--parallel", action="store_true", help="Gunakan mode paralel (eksperimental)")
    parser.add_argument("--max-workers", type=int, default=DEFAULT_MAX_WORKERS, help="Jumlah thread untuk paralel")
    parser.add_argument("--force-update", action="store_true", help="Abaikan checkpoint & update semua ticker")
    parser.add_argument("--verbose", action="store_true", help="Tampilkan output debug")
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    args = parser.parse_args()
    
    # Setup logging
    global logger
    logger = setup_logging(verbose=args.verbose)
    logger.info(f"Memulai Fundamental Data Downloader v{__version__}")
    
    # Cek koneksi internet
    if not check_internet_connection():
        logger.error("Tidak ada koneksi internet. Keluar.")
        sys.exit(1)
    
    # Buat session requests
    session = create_requests_session()
    
    # Ambil daftar file ticker
    logger.info("Mengambil daftar file ticker dari GitHub...")
    ticker_files = get_ticker_files(session)
    if not ticker_files:
        logger.error("Gagal mendapatkan daftar file ticker. Periksa koneksi atau URL.")
        sys.exit(1)
    
    logger.info("File ticker tersedia:")
    for i, f in enumerate(ticker_files, 1):
        logger.info(f"  {i}. {f}")
    
    # Pilih file secara interaktif jika tidak diberikan argumen
    if len(ticker_files) == 1:
        selected_file = ticker_files[0]
        logger.info(f"Hanya satu file, otomatis memilih: {selected_file}")
    else:
        while True:
            try:
                choice = int(input(f"\nPilih nomor file (1-{len(ticker_files)}): "))
                if 1 <= choice <= len(ticker_files):
                    selected_file = ticker_files[choice-1]
                    break
                else:
                    print(f"Masukkan angka antara 1 dan {len(ticker_files)}")
            except ValueError:
                print("Masukkan angka yang valid")
    
    country_name = selected_file.replace('.txt', '')
    logger.info(f"Negara/wilayah: {country_name}")
    
    # Download daftar ticker
    all_tickers = download_ticker_list(session, args.github_base, selected_file)
    if not all_tickers:
        logger.error("Daftar ticker kosong. Keluar.")
        sys.exit(1)
    
    # Siapkan direktori penyimpanan
    save_dir = Path(args.storage) / country_name
    save_dir.mkdir(parents=True, exist_ok=True)
    db_path = save_dir / DEFAULT_DB_NAME
    checkpoint_file = save_dir / "checkpoint.txt"
    
    # Cek ruang disk
    if not check_disk_space(save_dir, required_mb=50):
        logger.error("Ruang disk tidak mencukupi. Keluar.")
        sys.exit(1)
    
    # Inisialisasi database
    initialize_database(db_path)
    
    # Load checkpoint
    checkpoint_tickers = load_checkpoint(checkpoint_file) if not args.force_update else []
    
    # Filter ticker yang perlu diproses
    tickers_to_process = []
    for ticker in all_tickers:
        if args.force_update:
            tickers_to_process.append(ticker)
        else:
            if ticker in checkpoint_tickers:
                continue
            if should_update_ticker(ticker, db_path, args.update_after_days):
                tickers_to_process.append(ticker)
    
    logger.info("="*60)
    logger.info("STATISTIK DOWNLOAD")
    logger.info("="*60)
    logger.info(f"🌏 Country             : {country_name}")
    logger.info(f"📈 Total tickers       : {len(all_tickers):,}")
    logger.info(f"🔄 Perlu update        : {len(tickers_to_process):,}")
    logger.info(f"♻️ Update interval     : {args.update_after_days} hari")
    logger.info(f"💾 Database            : {db_path}")
    logger.info(f"🚀 Mode paralel        : {'Ya' if args.parallel else 'Tidak'}")
    if args.parallel:
        logger.info(f"🧵 Max workers         : {args.max_workers}")
    logger.info("="*60)
    
    if not tickers_to_process:
        logger.info("✅ Semua data masih fresh. Tidak perlu update.")
        remove_checkpoint(checkpoint_file)
        sys.exit(0)
    
    confirm = input(f"\nProses update {len(tickers_to_process):,} ticker? (y/n): ")
    if confirm.lower() != 'y':
        logger.info("Dibatalkan oleh user.")
        sys.exit(0)
    
    logger.info("🚀 Memulai proses update...")
    start_time = time.time()
    success_count = 0
    fail_count = 0
    
    processed_now = checkpoint_tickers.copy()  # ticker yang sudah selesai sebelum run ini
    
    if args.parallel:
        # Proses paralel (perhatikan rate limit masih berisiko)
        # Kita proses dalam batch kecil untuk memudahkan checkpoint
        batch_size = args.max_workers * 2
        total_batches = (len(tickers_to_process) + batch_size - 1) // batch_size
        for batch_idx in range(0, len(tickers_to_process), batch_size):
            batch = tickers_to_process[batch_idx:batch_idx+batch_size]
            logger.info(f"Memproses batch {batch_idx//batch_size + 1}/{total_batches} ({len(batch)} ticker)")
            results = process_parallel(batch, db_path, args.max_workers, (args.delay_min, args.delay_max))
            for r in results:
                if r['Status'] == 'SUCCESS':
                    success_count += 1
                else:
                    fail_count += 1
                processed_now.append(r['Ticker'])
            # Simpan checkpoint per batch
            save_checkpoint(processed_now, checkpoint_file)
            elapsed = time.time() - start_time
            avg_time_per_ticker = elapsed / len(processed_now)
            remaining = avg_time_per_ticker * (len(tickers_to_process) - len(processed_now))
            logger.info(f"Checkpoint tersimpan. ETA: {remaining/60:.1f} menit")
    else:
        # Mode sequential dengan progress bar manual
        total = len(tickers_to_process)
        for i, ticker in enumerate(tickers_to_process, 1):
            progress_pct = (i / total) * 100
            logger.info(f"[{i:>5}/{total}] ({progress_pct:>5.1f}%) {ticker:<10}", extra={'skip_time': True})
            result = get_free_float(ticker, max_retry=args.max_retry)
            save_to_db(result, db_path)
            if result['Status'] == 'SUCCESS':
                success_count += 1
                logger.info(f"✓ Free Float: {result['Free Float (%)']}%")
            else:
                fail_count += 1
                logger.info(f"✗ GAGAL: {result['Error'][:100]}")
            processed_now.append(ticker)
            # Checkpoint periodik
            if i % DEFAULT_CHECKPOINT_EVERY == 0:
                save_checkpoint(processed_now, checkpoint_file)
                elapsed = time.time() - start_time
                avg = elapsed / i
                remaining = avg * (total - i)
                logger.info(f"💾 Checkpoint tersimpan | ETA: {remaining/60:.1f} menit")
            # Delay
            delay = random.uniform(args.delay_min, args.delay_max)
            time.sleep(delay)
    
    # Hapus checkpoint setelah sukses semua
    remove_checkpoint(checkpoint_file)
    
    total_time = time.time() - start_time
    logger.info("="*60)
    logger.info("✅ UPDATE SELESAI")
    logger.info("="*60)
    logger.info(f"✅ Sukses : {success_count:,}")
    logger.info(f"❌ Gagal  : {fail_count:,}")
    logger.info(f"⏱️ Waktu total : {total_time/60:.1f} menit")
    logger.info(f"💾 Database : {db_path}")
    logger.info("="*60)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.info("\n⚠️ Proses dihentikan oleh user. Progress tersimpan di checkpoint.")
        sys.exit(1)
    except Exception as e:
        logging.exception(f"FATAL ERROR: {e}")
        sys.exit(1)
