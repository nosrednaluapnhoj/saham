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
# FUNCTIONS
# =====================================
def get_ticker_files():
    """Fetch list of .txt files from data/tickers folder on GitHub"""
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
            tickers = [line.strip() for line in response.text.splitlines() if line.strip()]
            print(f"âœ“ Loaded {len(tickers)} tickers from {filename}")
            return tickers
    except Exception as e:
        print(f"âœ— Error: {e}")
    return []

def clean_downloaded_data(df, ticker_symbol):
    if df is None or df.empty:
        return pd.DataFrame()
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    df = df.reset_index()
    df["Date"] = pd.to_datetime(df["Date"]).dt.tz_localize(None)
    df.columns = [str(col).strip() for col in df.columns]
    df["Ticker"] = ticker_symbol
    required_cols = ["Ticker", "Date", "Open", "High", "Low", "Close", "Volume"]
    existing_cols = [c for c in required_cols if c in df.columns]
    return df[existing_cols]

def save_checkpoint(final_df, parquet_file, checkpoint_num, total_tickers):
    """Save data to a temporary file and then rename it (atomic operation)"""
    temp_file = parquet_file + f".checkpoint_{checkpoint_num}.tmp"
    try:
        final_df.to_parquet(temp_file, index=False)
        shutil.move(temp_file, parquet_file)
        print(f"  ðŸ’¾ CHECKPOINT {checkpoint_num}: Saved {len(final_df):,} rows")
        return True
    except Exception as e:
        print(f"  âš ï¸ Failed to save checkpoint: {e}")
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
            existing_df = pd.read_parquet(parquet_file)
            existing_df["Date"] = pd.to_datetime(existing_df["Date"]).dt.tz_localize(None)
            processed = existing_df['Ticker'].unique().tolist()
            print(f"âœ“ Found {len(processed):,} already processed tickers")
            return processed, existing_df
        except Exception as e:
            print(f"âš ï¸ Error reading existing file: {e}")
            return [], None
    return [], None

def cleanup_temp_files(save_dir, parquet_file):
    """Clean up unused temporary checkpoint files"""
    try:
        temp_files = glob.glob(f"{save_dir}/*.checkpoint_*.tmp")
        for temp_file in temp_files:
            os.remove(temp_file)
            print(f"  ðŸ—‘ï¸ Cleaned up: {os.path.basename(temp_file)}")

        checkpoint_files = glob.glob(f"{parquet_file}.checkpoint_*.tmp")
        for cf in checkpoint_files:
            if cf != parquet_file:
                os.remove(cf)
                print(f"  ðŸ—‘ï¸ Cleaned up: {os.path.basename(cf)}")
        return True
    except Exception as e:
        print(f"  âš ï¸ Failed to cleanup: {e}")
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
        print(f"\nâœ… All {len(all_tickers):,} tickers already downloaded!")
        cleanup_temp_files(save_dir, parquet_file)
        return

    print(f"\nðŸ“Š Total tickers   : {len(all_tickers):,}")
    print(f"ðŸ“ˆ Already done    : {len(processed_tickers):,}")
    print(f"ðŸ”„ To process      : {len(tickers_to_process):,}")

    confirm = input(f"\nProceed to download {len(tickers_to_process):,} tickers? (y/n): ")
    if confirm.lower() != 'y':
        print("âŒ Cancelled")
        return

    checkpoint_counter = len(processed_tickers) // CHECKPOINT_EVERY
    all_data = []
    successful_tickers = 0
    failed_tickers = []
    start_time = time.time()

    for i, ticker in enumerate(tickers_to_process):
        current_num = len(processed_tickers) + i + 1
        progress_pct = (current_num / len(all_tickers)) * 100

        print(f"[{current_num:>5}/{len(all_tickers)}] ({progress_pct:>5.1f}%) {ticker:<10} ", end="")

        try:
            df_clean = download_with_retry(ticker, START_DATE_DEFAULT, END_DATE)
            if not df_clean.empty:
                all_data.append(df_clean)
                successful_tickers += 1
                print("âœ“")
            else:
                print("âš ï¸ no data")
        except Exception:
            failed_tickers.append(ticker)
            print("âœ— FAILED")

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
                    print(f"     â±ï¸ ETA: {remaining/60:.1f} minutes")

        time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

    _print_summary(successful_tickers, failed_tickers, existing_df, parquet_file, start_time, len(tickers_to_process))
    cleanup_temp_files(save_dir, parquet_file)

# =====================================
# MODE 2: UPDATE (TAMBAH DATA TERBARU)
# =====================================
def mode_update(all_tickers, parquet_file, save_dir):
    """
    Update mode: for each ticker already in the parquet, download only
    data from (last_date - 5 days) to today to append the latest prices.
    Tickers not yet in the file will be downloaded fully.
    """
    processed_tickers, existing_df = get_processed_tickers(parquet_file)

    if existing_df is None:
        print("\nâš ï¸ No existing data found. Please run Fresh Download first.")
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

    print(f"\nðŸ“Š Total tickers          : {len(all_tickers):,}")
    print(f"ðŸ”„ Tickers need update    : {len(tickers_need_update):,}")
    print(f"ðŸ†• New tickers (not yet dl): {len(tickers_new):,}")

    if not tickers_need_update and not tickers_new:
        print("\nâœ… All data is already up to date!")
        return

    confirm = input(f"\nProceed to update {len(tickers_need_update):,} + {len(tickers_new):,} new tickers? (y/n): ")
    if confirm.lower() != 'y':
        print("âŒ Cancelled")
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
            label = f"â†‘UPDATE (dari {last_date})"
        else:
            start_req = START_DATE_DEFAULT
            label = "ðŸ†• NEW"

        progress_pct = ((i + 1) / len(all_work)) * 100
        print(f"[{i+1:>5}/{len(all_work)}] ({progress_pct:>5.1f}%) {ticker:<10} {label} ", end="")

        try:
            df_clean = download_with_retry(ticker, start_req, END_DATE)
            if not df_clean.empty:
                all_data.append(df_clean)
                successful += 1
                print(f"âœ“ (+{len(df_clean)} rows)")
            else:
                print("âš ï¸ no new data")
                skipped += 1
        except Exception as e:
            failed.append(ticker)
            print(f"âœ— FAILED ({e})")

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
                    print(f"     â±ï¸ ETA: {remaining/60:.1f} minutes")

        time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

    _print_summary(successful, failed, existing_df, parquet_file, start_time, len(all_work), skipped=skipped)
    cleanup_temp_files(save_dir, parquet_file)

# =====================================
# MODE 3: BACKFILL (ISI DATA MASA LALU)
# =====================================
def find_date_gaps(ticker_df, start_date_str, min_gap_days=7):
    """
    Detect gaps in historical data for a ticker.
    A gap is a period where consecutive dates differ by more than min_gap_days
    (accounting for weekends ~2 days + buffer).
    Returns list of (gap_start, gap_end) tuples.
    """
    if ticker_df.empty:
        return [(pd.Timestamp(start_date_str), pd.Timestamp(END_DATE))]

    dates = ticker_df["Date"].sort_values().reset_index(drop=True)
    gaps = []

    # Check gap at the beginning (before first recorded date)
    first_date = dates.iloc[0]
    start_ts = pd.Timestamp(start_date_str)
    if (first_date - start_ts).days > min_gap_days:
        gaps.append((start_ts, first_date - timedelta(days=1)))

    # Check gaps between consecutive dates
    for j in range(len(dates) - 1):
        diff = (dates.iloc[j + 1] - dates.iloc[j]).days
        if diff > min_gap_days:
            gap_start = dates.iloc[j] + timedelta(days=1)
            gap_end   = dates.iloc[j + 1] - timedelta(days=1)
            gaps.append((gap_start, gap_end))

    return gaps

def mode_backfill(all_tickers, parquet_file, save_dir):
    """
    Backfill mode: scan each ticker's data for gaps in history,
    then download and fill those gaps from yfinance.
    """
    processed_tickers, existing_df = get_processed_tickers(parquet_file)

    if existing_df is None:
        print("\nâš ï¸ No existing data found. Please run Fresh Download first.")
        return

    # Backfill settings
    print("\nâš™ï¸  Backfill Settings:")
    try:
        start_year = input(f"   Backfill from year (default {START_DATE_DEFAULT[:4]}): ").strip()
        backfill_start = f"{start_year}-01-01" if start_year.isdigit() and len(start_year) == 4 else START_DATE_DEFAULT
    except:
        backfill_start = START_DATE_DEFAULT

    try:
        min_gap = int(input("   Minimum gap to fill in days (default 7): ").strip() or "7")
    except:
        min_gap = 7

    print(f"\nðŸ” Scanning gaps from {backfill_start} with min gap = {min_gap} days...")
    print("   (This may take a moment for large datasets)")

    # Scan all tickers for gaps
    tickers_with_gaps = []
    for ticker in all_tickers:
        if ticker not in processed_tickers:
            continue
        ticker_df = existing_df[existing_df["Ticker"] == ticker]
        gaps = find_date_gaps(ticker_df, backfill_start, min_gap_days=min_gap)
        if gaps:
            tickers_with_gaps.append((ticker, gaps))

    total_gaps = sum(len(g) for _, g in tickers_with_gaps)
    print(f"\nðŸ“‹ Backfill Summary:")
    print(f"   Tickers scanned     : {len(all_tickers):,}")
    print(f"   Tickers with gaps   : {len(tickers_with_gaps):,}")
    print(f"   Total gaps found    : {total_gaps:,}")

    if not tickers_with_gaps:
        print("\nâœ… No gaps found! Data is complete.")
        return

    # Preview sample gaps
    print("\nðŸ“Œ Sample gaps (first 5 tickers):")
    for ticker, gaps in tickers_with_gaps[:5]:
        print(f"   {ticker}:")
        for gs, ge in gaps[:3]:
            print(f"      {gs.strftime('%Y-%m-%d')} â†’ {ge.strftime('%Y-%m-%d')} ({(ge-gs).days} days)")
        if len(gaps) > 3:
            print(f"      ... and {len(gaps)-3} more gaps")

    confirm = input(f"\nProceed to backfill {total_gaps:,} gaps across {len(tickers_with_gaps):,} tickers? (y/n): ")
    if confirm.lower() != 'y':
        print("âŒ Cancelled")
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
        print(f"\n[{idx+1:>4}/{total_tasks}] {ticker} â€” {len(gaps)} gap(s)")

        ticker_new_data = []
        for gs, ge in gaps:
            gap_start_str = gs.strftime("%Y-%m-%d")
            gap_end_str   = (ge + timedelta(days=1)).strftime("%Y-%m-%d")
            print(f"       ðŸ“… {gap_start_str} â†’ {ge.strftime('%Y-%m-%d')} ", end="")

            try:
                df_clean = download_with_retry(ticker, gap_start_str, gap_end_str)
                if not df_clean.empty:
                    ticker_new_data.append(df_clean)
                    successful_fills += 1
                    print(f"âœ“ (+{len(df_clean)} rows)")
                else:
                    empty_fills += 1
                    print("âš ï¸ no data (market closed / not listed yet)")
            except Exception as e:
                failed_fills.append(f"{ticker} [{gap_start_str}â†’{ge.strftime('%Y-%m-%d')}]")
                print(f"âœ— FAILED ({e})")

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
                avg_time = elapsed / total_processed
                remaining = avg_time * (total_tasks - total_processed)
                if remaining > 0:
                    print(f"     â±ï¸ ETA: {remaining/60:.1f} minutes")

    total_time = time.time() - start_time
    print(f"\n{'='*60}")
    print(f"ðŸ” BACKFILL COMPLETED")
    print(f"{'='*60}")
    print(f"âœ“ Gaps filled (with data) : {successful_fills:,}")
    print(f"âš ï¸ Gaps empty (no trading) : {empty_fills:,}")
    print(f"âœ— Failed                  : {len(failed_fills):,}")
    if failed_fills:
        for f in failed_fills[:10]:
            print(f"   - {f}")
        if len(failed_fills) > 10:
            print(f"   ... and {len(failed_fills)-10} more")
    print(f"ðŸ’¾ Saved to: {parquet_file}")
    print(f"ðŸ“ˆ Total rows: {len(existing_df):,}")
    print(f"â±ï¸ Total time: {total_time/60:.1f} minutes")
    print(f"{'='*60}")

    cleanup_temp_files(save_dir, parquet_file)

# =====================================
# HELPER: PRINT SUMMARY
# =====================================
def _print_summary(successful, failed, existing_df, parquet_file, start_time, total_processed, skipped=0):
    total_time = time.time() - start_time
    print(f"\n{'='*60}")
    print(f"ðŸ“Š COMPLETED")
    print(f"{'='*60}")
    print(f"âœ“ Successful : {successful:,}")
    if skipped:
        print(f"â­ï¸ Skipped   : {skipped:,}")
    print(f"âœ— Failed     : {len(failed):,}")
    if failed:
        print(f"  Failed list: {', '.join(failed[:10])}")
        if len(failed) > 10:
            print(f"  ... and {len(failed)-10} more")
    print(f"ðŸ’¾ Saved to  : {parquet_file}")
    row_count = len(existing_df) if existing_df is not None else 0
    print(f"ðŸ“ˆ Total rows: {row_count:,}")
    print(f"â±ï¸ Total time: {total_time/60:.1f} minutes")
    if total_processed > 0:
        print(f"âš¡ Average   : {total_time/total_processed:.1f} sec/ticker")
    print(f"{'='*60}")

# =====================================
# MAIN MENU
# =====================================
def main():
    print("\n" + "="*60)
    print("ðŸ“Š HISTORICAL PRICE DOWNLOADER")
    print("="*60)

    ticker_files = get_ticker_files()

    if not ticker_files:
        print("âŒ Unable to fetch file list from GitHub")
        return

    print("\nðŸ“ Available ticker files:")
    for i, f in enumerate(ticker_files, 1):
        print(f"  {i}. {f}")

    while True:
        try:
            choice = int(input("\nSelect file number: "))
            if 1 <= choice <= len(ticker_files):
                selected_file = ticker_files[choice - 1]
                break
        except:
            print("âŒ Please enter a valid number")

    all_tickers = download_ticker_list(selected_file)
    if not all_tickers:
        return

    country_name = selected_file.replace('.txt', '')
    save_dir = f"{BASE_STORAGE}/{country_name}"
    os.makedirs(save_dir, exist_ok=True)
    parquet_file = f"{save_dir}/price_history.parquet"

    # â”€â”€ MODE SELECTION â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    print("\n" + "="*60)
    print("ðŸ› ï¸  SELECT MODE:")
    print("="*60)
    print("  1. ðŸ“¥ Fresh Download   â€” Download semua tickers (resume jika terputus)")
    print("  2. ðŸ”„ Update Terbaru   â€” Tambah data terkini ke data yang sudah ada")
    print("  3. ðŸ” Backfill History â€” Isi data historis yang kosong/gap")
    print("="*60)

    while True:
        try:
            mode = int(input("\nPilih mode (1/2/3): "))
            if mode in [1, 2, 3]:
                break
        except:
            pass
        print("âŒ Masukkan angka 1, 2, atau 3")

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
        print("\n\n" + "="*60)
        print("âš ï¸  PROCESS INTERRUPTED BY USER")
        print("="*60)
        print("âœ… Data dari tickers yang selesai sudah disimpan ke checkpoint")
        print("ðŸ’¾ File price_history.parquet aman")
        print("â–¶ï¸  Jalankan script lagi untuk melanjutkan dari posisi terakhir")
        print("="*60)
    except Exception as e:
        print(f"\nâŒ ERROR: {e}")
        print("âœ… Checkpoint data aman, jalankan ulang untuk melanjutkan")
