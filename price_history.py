# -*- coding: utf-8 -*-
import sys
import io

# Force UTF-8 output (penting untuk terminal Android/Termux)
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')
else:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8')
else:
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

import yfinance as yf
import pandas as pd
import os
import time
import random
from datetime import datetime, timedelta
import requests
import shutil
import glob

# =====================================
# CONFIGURATION
# =====================================
GITHUB_RAW_BASE = "https://raw.githubusercontent.com/nosrednaluapnhoj/saham/main/tickers/"
BASE_STORAGE = "/storage/emulated/0/Saham"
START_DATE_DEFAULT = "2000-01-01"
END_DATE = datetime.today().strftime("%Y-%m-%d")
DELAY_MIN = 2
DELAY_MAX = 4
MAX_RETRY = 3
CHECKPOINT_EVERY = 10

# =====================================
# SAFE PRINT (ASCII only, no emoji)
# =====================================
def safe_print(msg, end='\n'):
    """Print dengan encoding UTF-8, replace character yang tidak didukung"""
    try:
        print(msg, end=end)
    except UnicodeEncodeError:
        # fallback: replace problematic chars
        safe_msg = msg.encode('utf-8', errors='replace').decode('utf-8')
        print(safe_msg, end=end)

# =====================================
# FUNCTIONS
# =====================================
def get_ticker_files():
    """Fetch list of .txt files from data/tickers folder on GitHub"""
    api_url = "https://api.github.com/repos/nosrednaluapnhoj/saham/contents/tickers"
    try:
        response = requests.get(api_url, timeout=10)
        response.encoding = 'utf-8'
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
        response.encoding = 'utf-8'
        if response.status_code == 200:
            tickers = [line.strip() for line in response.text.splitlines() if line.strip()]
            safe_print(f"[OK] Loaded {len(tickers)} tickers from {filename}")
            return tickers
    except Exception as e:
        safe_print(f"[ERROR] {e}")
    return []

def clean_downloaded_data(df, ticker_symbol):
    if df is None or df.empty:
        return pd.DataFrame()
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    df = df.reset_index()
    df["Date"] = pd.to_datetime(df["Date"]).dt.tz_localize(None)
    df.columns = [str(col).strip() for col in df.columns]
    df["Ticker"] = str(ticker_symbol)
    required_cols = ["Ticker", "Date", "Open", "High", "Low", "Close", "Volume"]
    existing_cols = [c for c in required_cols if c in df.columns]
    return df[existing_cols]

def save_checkpoint(final_df, parquet_file, checkpoint_num, total_tickers):
    """Save data to a temporary file and then rename it (atomic operation)"""
    temp_file = parquet_file + f".checkpoint_{checkpoint_num}.tmp"
    try:
        final_df.to_parquet(temp_file, index=False, engine='pyarrow')
        shutil.move(temp_file, parquet_file)
        safe_print(f" [CHECKPOINT {checkpoint_num}] Saved {len(final_df):,} rows")
        return True
    except Exception as e:
        safe_print(f" [WARN] Failed to save checkpoint: {e}")
        if os.path.exists(temp_file):
            try:
                os.remove(temp_file)
            except:
                pass
        return False

def get_processed_tickers(parquet_file):
    """Get list of already processed tickers from parquet file"""
    if os.path.exists(parquet_file):
        try:
            existing_df = pd.read_parquet(parquet_file, engine='pyarrow')
            existing_df["Date"] = pd.to_datetime(existing_df["Date"]).dt.tz_localize(None)
            for col in existing_df.select_dtypes(include='object').columns:
                existing_df[col] = existing_df[col].astype(str)
            processed = existing_df['Ticker'].unique().tolist()
            safe_print(f"[OK] Found {len(processed):,} already processed tickers")
            return processed, existing_df
        except Exception as e:
            safe_print(f"[WARN] Error reading existing file: {e}")
            return [], None
    return [], None

def cleanup_temp_files(save_dir, parquet_file):
    """Clean up unused temporary checkpoint files"""
    try:
        temp_files = glob.glob(f"{save_dir}/*.checkpoint_*.tmp")
        for temp_file in temp_files:
            os.remove(temp_file)
            safe_print(f" [CLEANUP] Removed: {os.path.basename(temp_file)}")
        checkpoint_files = glob.glob(f"{parquet_file}.checkpoint_*.tmp")
        for cf in checkpoint_files:
            if cf != parquet_file:
                os.remove(cf)
                safe_print(f" [CLEANUP] Removed: {os.path.basename(cf)}")
        return True
    except Exception as e:
        safe_print(f" [WARN] Failed to cleanup: {e}")
        return False

def download_with_retry(ticker, start_date, end_date):
    """Download ticker data with retry logic, returns cleaned DataFrame or empty."""
    for attempt in range(MAX_RETRY):
        try:
            df = yf.download(ticker, start=start_date, end=end_date, progress=False)
            if not df.empty:
                df_clean = clean_downloaded_data(df, ticker)
                return df_clean
            else:
                return pd.DataFrame()
        except Exception as e:
            if attempt < MAX_RETRY - 1:
                time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
            else:
                raise e
    return pd.DataFrame()

# =====================================
# MODE 1: FRESH DOWNLOAD
# =====================================
def mode_fresh_download(all_tickers, parquet_file, save_dir):
    """Original download mode: download all or resume from checkpoint."""
    processed_tickers, existing_df = get_processed_tickers(parquet_file)
    tickers_to_process = [t for t in all_tickers if t not in processed_tickers]

    if not tickers_to_process:
        safe_print(f"\n[OK] All {len(all_tickers):,} tickers already downloaded!")
        cleanup_temp_files(save_dir, parquet_file)
        return

    safe_print(f"\n[DATA] Total tickers : {len(all_tickers):,}")
    safe_print(f"[DATA] Already done : {len(processed_tickers):,}")
    safe_print(f"[DATA] To process    : {len(tickers_to_process):,}")

    confirm = input(f"\nProceed to download {len(tickers_to_process):,} tickers? (y/n): ")
    if confirm.lower() != 'y':
        safe_print("[CANCEL] Cancelled")
        return

    checkpoint_counter = len(processed_tickers) // CHECKPOINT_EVERY
    all_data = []
    successful_tickers = 0
    failed_tickers = []
    start_time = time.time()

    for i, ticker in enumerate(tickers_to_process):
        current_num = len(processed_tickers) + i + 1
        progress_pct = (current_num / len(all_tickers)) * 100
        safe_print(f"[{current_num:>5}/{len(all_tickers)}] ({progress_pct:>5.1f}%) {ticker:<10} ", end="")

        try:
            df_clean = download_with_retry(ticker, START_DATE_DEFAULT, END_DATE)
            if not df_clean.empty:
                all_data.append(df_clean)
                successful_tickers += 1
                safe_print("[OK]")
            else:
                safe_print("[WARN] no data")
        except Exception:
            failed_tickers.append(ticker)
            safe_print("[FAIL]")

        if (i + 1) % CHECKPOINT_EVERY == 0 or (i + 1) == len(tickers_to_process):
            if all_data:
                new_df = pd.concat(all_data, ignore_index=True)
                final_df = pd.concat([existing_df, new_df], ignore_index=True) if existing_df is not None else new_df
                final_df = final_df.drop_duplicates(subset=["Ticker", "Date"], keep="last")
                final_df = final_df.sort_values(["Ticker", "Date"]).reset_index(drop=True)
                checkpoint_counter += 1
                save_checkpoint(final_df, parquet_file, checkpoint_counter, len(all_tickers))
                existing_df = final_df
                all_data = []

        elapsed = time.time() - start_time
        avg_time = elapsed / (i + 1)
        remaining = avg_time * (len(tickers_to_process) - i - 1)
        if remaining > 0:
            safe_print(f" [ETA] {remaining/60:.1f} minutes")

        time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

    _print_summary(successful_tickers, failed_tickers, existing_df, parquet_file, start_time, len(tickers_to_process))
    cleanup_temp_files(save_dir, parquet_file)

# =====================================
# MODE 2: UPDATE (TAMBAH DATA TERBARU)
# =====================================
def mode_update(all_tickers, parquet_file, save_dir):
    """Update mode: for each ticker already in the parquet, download only data from (last_date - 5 days) to today to append the latest prices."""
    processed_tickers, existing_df = get_processed_tickers(parquet_file)
    if existing_df is None:
        safe_print("\n[WARN] No existing data found. Please run Fresh Download first.")
        return

    today = datetime.today().date()
    tickers_need_update = []
    tickers_new = []

    for ticker in all_tickers:
        if ticker in processed_tickers:
            ticker_df = existing_df[existing_df["Ticker"] == ticker]
            last_date = ticker_df["Date"].max().date()
            if last_date < today:
                tickers_need_update.append((ticker, last_date))
        else:
            tickers_new.append(ticker)

    safe_print(f"\n[DATA] Total tickers         : {len(all_tickers):,}")
    safe_print(f"[DATA] Tickers need update   : {len(tickers_need_update):,}")
    safe_print(f"[DATA] New tickers (not yet) : {len(tickers_new):,}")

    if not tickers_need_update and not tickers_new:
        safe_print("\n[OK] All data is already up to date!")
        return

    confirm = input(f"\nProceed to update {len(tickers_need_update):,} + {len(tickers_new):,} new tickers? (y/n): ")
    if confirm.lower() != 'y':
        safe_print("[CANCEL] Cancelled")
        return

    all_work = [(t, last_d, "update") for t, last_d in tickers_need_update] + \
               [(t, None, "new") for t in tickers_new]

    all_data = []
    successful = 0
    failed = []
    skipped = 0
    start_time = time.time()
    checkpoint_counter = 0

    for i, (ticker, last_date, kind) in enumerate(all_work):
        if kind == "update":
            start_req = (last_date - timedelta(days=5)).strftime("%Y-%m-%d")
            label = f"UPDATE (from {last_date})"
        else:
            start_req = START_DATE_DEFAULT
            label = "NEW"

        progress_pct = ((i + 1) / len(all_work)) * 100
        safe_print(f"[{i+1:>5}/{len(all_work)}] ({progress_pct:>5.1f}%) {ticker:<10} {label} ", end="")

        try:
            df_clean = download_with_retry(ticker, start_req, END_DATE)
            if not df_clean.empty:
                all_data.append(df_clean)
                successful += 1
                safe_print(f"[OK] (+{len(df_clean)} rows)")
            else:
                safe_print("[WARN] no new data")
                skipped += 1
        except Exception as e:
            failed.append(ticker)
            safe_print(f"[FAIL] ({e})")

        if (i + 1) % CHECKPOINT_EVERY == 0 or (i + 1) == len(all_work):
            if all_data:
                new_df = pd.concat(all_data, ignore_index=True)
                final_df = pd.concat([existing_df, new_df], ignore_index=True)
                final_df = final_df.drop_duplicates(subset=["Ticker", "Date"], keep="last")
                final_df = final_df.sort_values(["Ticker", "Date"]).reset_index(drop=True)
                checkpoint_counter += 1
                save_checkpoint(final_df, parquet_file, checkpoint_counter, len(all_work))
                existing_df = final_df
                all_data = []

        elapsed = time.time() - start_time
        avg_time = elapsed / (i + 1)
        remaining = avg_time * (len(all_work) - i - 1)
        if remaining > 0:
            safe_print(f" [ETA] {remaining/60:.1f} minutes")

        time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

    _print_summary(successful, failed, existing_df, parquet_file, start_time, len(all_work), skipped=skipped)
    cleanup_temp_files(save_dir, parquet_file)

# =====================================
# MODE 3: BACKFILL (ISI DATA MASA LALU)
# =====================================
def find_date_gaps(ticker_df, start_date_str, min_gap_days=7):
    """Detect gaps in historical data for a ticker."""
    if ticker_df.empty:
        return [(pd.Timestamp(start_date_str), pd.Timestamp(END_DATE))]

    dates = ticker_df["Date"].sort_values().reset_index(drop=True)
    gaps = []

    first_date = dates.iloc[0]
    start_ts = pd.Timestamp(start_date_str)
    if (first_date - start_ts).days > min_gap_days:
        gaps.append((start_ts, first_date - timedelta(days=1)))

    for j in range(len(dates) - 1):
        diff = (dates.iloc[j + 1] - dates.iloc[j]).days
        if diff > min_gap_days:
            gap_start = dates.iloc[j] + timedelta(days=1)
            gap_end = dates.iloc[j + 1] - timedelta(days=1)
            gaps.append((gap_start, gap_end))
    return gaps

def mode_backfill(all_tickers, parquet_file, save_dir):
    """Backfill mode: scan each ticker's data for gaps in history, then download and fill those gaps."""
    processed_tickers, existing_df = get_processed_tickers(parquet_file)
    if existing_df is None:
        safe_print("\n[WARN] No existing data found. Please run Fresh Download first.")
        return

    safe_print("\n[SETUP] Backfill Settings:")
    try:
        start_year = input(f" Backfill from year (default {START_DATE_DEFAULT[:4]}): ").strip()
        backfill_start = f"{start_year}-01-01" if start_year.isdigit() and len(start_year) == 4 else START_DATE_DEFAULT
    except:
        backfill_start = START_DATE_DEFAULT

    try:
        min_gap = int(input(" Minimum gap to fill in days (default 7): ").strip() or "7")
    except:
        min_gap = 7

    safe_print(f"\n[SCAN] Scanning gaps from {backfill_start} with min gap = {min_gap} days...")
    safe_print(" (This may take a moment for large datasets)")

    tickers_with_gaps = []
    for ticker in all_tickers:
        if ticker not in processed_tickers:
            continue
        ticker_df = existing_df[existing_df["Ticker"] == ticker]
        gaps = find_date_gaps(ticker_df, backfill_start, min_gap_days=min_gap)
        if gaps:
            tickers_with_gaps.append((ticker, gaps))

    total_gaps = sum(len(g) for _, g in tickers_with_gaps)
    safe_print(f"\n[SUMMARY] Backfill Summary:")
    safe_print(f" Tickers scanned    : {len(all_tickers):,}")
    safe_print(f" Tickers with gaps  : {len(tickers_with_gaps):,}")
    safe_print(f" Total gaps found   : {total_gaps:,}")

    if not tickers_with_gaps:
        safe_print("\n[OK] No gaps found! Data is complete.")
        return

    safe_print("\n[SAMPLE] Sample gaps (first 5 tickers):")
    for ticker, gaps in tickers_with_gaps[:5]:
        safe_print(f" {ticker}:")
        for gs, ge in gaps[:3]:
            safe_print(f"   {gs.strftime('%Y-%m-%d')} -> {ge.strftime('%Y-%m-%d')} ({(ge-gs).days} days)")
        if len(gaps) > 3:
            safe_print(f"   ... and {len(gaps)-3} more gaps")

    confirm = input(f"\nProceed to backfill {total_gaps:,} gaps across {len(tickers_with_gaps):,} tickers? (y/n): ")
    if confirm.lower() != 'y':
        safe_print("[CANCEL] Cancelled")
        return

    all_data = []
    successful_fills = 0
    empty_fills = 0
    failed_fills = []
    start_time = time.time()
    checkpoint_counter = 0
    total_processed = 0
    total_tasks = len(tickers_with_gaps)

    for idx, (ticker, gaps) in enumerate(tickers_with_gaps):
        safe_print(f"\n[{idx+1:>4}/{total_tasks}] {ticker} -- {len(gaps)} gap(s)")
        ticker_new_data = []
        for gs, ge in gaps:
            gap_start_str = gs.strftime("%Y-%m-%d")
            gap_end_str = (ge + timedelta(days=1)).strftime("%Y-%m-%d")
            safe_print(f"  {gap_start_str} -> {ge.strftime('%Y-%m-%d')} ", end="")

            try:
                df_clean = download_with_retry(ticker, gap_start_str, gap_end_str)
                if not df_clean.empty:
                    ticker_new_data.append(df_clean)
                    successful_fills += 1
                    safe_print(f"[OK] (+{len(df_clean)} rows)")
                else:
                    empty_fills += 1
                    safe_print("[WARN] no data (market closed / not listed yet)")
            except Exception as e:
                failed_fills.append(f"{ticker} [{gap_start_str}->{ge.strftime('%Y-%m-%d')}]")
                safe_print(f"[FAIL] ({e})")

            time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

        if ticker_new_data:
            all_data.extend(ticker_new_data)
            total_processed += 1

        if total_processed % CHECKPOINT_EVERY == 0 or total_processed == total_tasks:
            if all_data:
                new_df = pd.concat(all_data, ignore_index=True)
                final_df = pd.concat([existing_df, new_df], ignore_index=True)
                final_df = final_df.drop_duplicates(subset=["Ticker", "Date"], keep="last")
                final_df = final_df.sort_values(["Ticker", "Date"]).reset_index(drop=True)
                checkpoint_counter += 1
                save_checkpoint(final_df, parquet_file, checkpoint_counter, total_tasks)
                existing_df = final_df
                all_data = []

        elapsed = time.time() - start_time
        avg_time = elapsed / total_processed if total_processed > 0 else 0
        remaining = avg_time * (total_tasks - total_processed)
        if remaining > 0:
            safe_print(f" [ETA] {remaining/60:.1f} minutes")

    total_time = time.time() - start_time
    safe_print(f"\n{'='*60}")
    safe_print(f"[DONE] BACKFILL COMPLETED")
    safe_print(f"{'='*60}")
    safe_print(f"[OK] Gaps filled (with data) : {successful_fills:,}")
    safe_print(f"[WARN] Gaps empty (no trading) : {empty_fills:,}")
    safe_print(f"[FAIL] Failed : {len(failed_fills):,}")
    if failed_fills:
        for f in failed_fills[:10]:
            safe_print(f"  - {f}")
        if len(failed_fills) > 10:
            safe_print(f"  ... and {len(failed_fills)-10} more")
    safe_print(f"[SAVE] Saved to: {parquet_file}")
    safe_print(f"[DATA] Total rows: {len(existing_df):,}")
    safe_print(f"[TIME] Total time: {total_time/60:.1f} minutes")
    safe_print(f"{'='*60}")

    cleanup_temp_files(save_dir, parquet_file)

# =====================================
# HELPER: PRINT SUMMARY
# =====================================
def _print_summary(successful, failed, existing_df, parquet_file, start_time, total_processed, skipped=0):
    total_time = time.time() - start_time
    safe_print(f"\n{'='*60}")
    safe_print(f"[DONE] COMPLETED")
    safe_print(f"{'='*60}")
    safe_print(f"[OK] Successful     : {successful:,}")
    if skipped:
        safe_print(f"[SKIP] Skipped       : {skipped:,}")
    safe_print(f"[FAIL] Failed        : {len(failed):,}")
    if failed:
        safe_print(f" Failed list: {', '.join(failed[:10])}")
        if len(failed) > 10:
            safe_print(f" ... and {len(failed)-10} more")
    safe_print(f"[SAVE] Saved to      : {parquet_file}")
    row_count = len(existing_df) if existing_df is not None else 0
    safe_print(f"[DATA] Total rows    : {row_count:,}")
    safe_print(f"[TIME] Total time    : {total_time/60:.1f} minutes")
    if total_processed > 0:
        safe_print(f"[AVG] Average       : {total_time/total_processed:.1f} sec/ticker")
    safe_print(f"{'='*60}")

# =====================================
# MAIN MENU
# =====================================
def main():
    safe_print("\n" + "="*60)
    safe_print("HISTORICAL PRICE DOWNLOADER (UTF-8 SAFE)")
    safe_print("="*60)

    ticker_files = get_ticker_files()
    if not ticker_files:
        safe_print("[ERROR] Unable to fetch file list from GitHub")
        return

    safe_print("\n[FILES] Available ticker files:")
    for i, f in enumerate(ticker_files, 1):
        safe_print(f" {i}. {f}")

    while True:
        try:
            choice = int(input("\nSelect file number: "))
            if 1 <= choice <= len(ticker_files):
                selected_file = ticker_files[choice - 1]
                break
        except:
            safe_print("[ERROR] Please enter a valid number")

    all_tickers = download_ticker_list(selected_file)
    if not all_tickers:
        return

    country_name = selected_file.replace('.txt', '')
    save_dir = f"{BASE_STORAGE}/{country_name}"
    os.makedirs(save_dir, exist_ok=True)
    parquet_file = f"{save_dir}/price_history.parquet"

    safe_print("\n" + "="*60)
    safe_print("SELECT MODE:")
    safe_print("="*60)
    safe_print(" 1. Fresh Download   - Download semua tickers (resume jika terputus)")
    safe_print(" 2. Update Terbaru   - Tambah data terkini ke data yang sudah ada")
    safe_print(" 3. Backfill History - Isi data historis yang kosong/gap")
    safe_print("="*60)

    while True:
        try:
            mode = int(input("\nPilih mode (1/2/3): "))
            if mode in [1, 2, 3]:
                break
        except:
            pass
        safe_print("[ERROR] Masukkan angka 1, 2, atau 3")

    if mode == 1:
        mode_fresh_download(all_tickers, parquet_file, save_dir)
    elif mode == 2:
        mode_update(all_tickers, parquet_file, save_dir)
    elif mode == 3:
        mode_backfill(all_tickers, parquet_file, save_dir)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        safe_print("\n\n" + "="*60)
        safe_print("[STOP] PROCESS INTERRUPTED BY USER")
        safe_print("="*60)
        safe_print("[OK] Data dari tickers yang selesai sudah disimpan ke checkpoint")
        safe_print("[SAVE] File price_history.parquet aman")
        safe_print("[INFO] Jalankan script lagi untuk melanjutkan dari posisi terakhir")
        safe_print("="*60)
    except Exception as e:
        safe_print(f"\n[ERROR] {e}")
        safe_print("[OK] Checkpoint data aman, jalankan ulang untuk melanjutkan")
