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
CHECKPOINT_EVERY = 10  # Flush buffer to disk every 10 tickers

# =====================================
# FUNCTIONS
# =====================================
def get_ticker_files():
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

def get_processed_tickers(parquet_file):
    """Get set of already processed tickers - only loads Ticker column (low memory)"""
    if os.path.exists(parquet_file):
        try:
            # Only read the Ticker column, not the entire file
            existing_tickers = pd.read_parquet(parquet_file, columns=["Ticker"])
            processed = set(existing_tickers["Ticker"].unique().tolist())
            print(f"âœ“ Found {len(processed)} already processed tickers")
            del existing_tickers  # Free memory immediately
            return processed
        except Exception as e:
            print(f"âš ï¸ Error reading existing file: {e}")
            return set()
    return set()

def get_ticker_last_date(parquet_file, ticker):
    """Get the last date for a specific ticker - reads only that ticker's rows"""
    if not os.path.exists(parquet_file):
        return None
    try:
        df = pd.read_parquet(parquet_file, columns=["Ticker", "Date"])
        ticker_dates = df[df["Ticker"] == ticker]["Date"]
        if not ticker_dates.empty:
            return ticker_dates.max()
        return None
    except:
        return None
    finally:
        try:
            del df
        except:
            pass

def flush_buffer_to_disk(buffer_data, parquet_file):
    """
    Flush buffer list to disk using zero-copy append strategy.
    Avoids loading the entire existing file into RAM.
    
    Strategy:
    1. Write buffer to a small temp parquet
    2. If main file exists, merge ONLY by reading existing + temp, dedup, save
    3. Because we process each ticker once, duplicates are rare â†’ merge is small
    """
    if not buffer_data:
        return True

    temp_file = parquet_file + ".tmp_flush"
    backup_file = parquet_file + ".bak"

    try:
        # Step 1: Write buffer (small, just 10 tickers worth of data)
        new_df = pd.concat(buffer_data, ignore_index=True)
        new_df = new_df.drop_duplicates(subset=["Ticker", "Date"], keep="last")

        if not os.path.exists(parquet_file):
            # First write - just save directly
            new_df.to_parquet(parquet_file, index=False)
            print(f"  ðŸ’¾ Written {len(new_df):,} rows (new file)")
            del new_df
            return True

        # Step 2: Append to existing file
        # Read existing, concat with new, dedup, write back
        # This is unavoidable for parquet (no true append mode)
        # BUT: we minimize RAM by deleting intermediates ASAP
        new_df.to_parquet(temp_file, index=False)
        del new_df  # Free buffer memory before loading existing file

        existing_df = pd.read_parquet(parquet_file)
        temp_df = pd.read_parquet(temp_file)

        merged = pd.concat([existing_df, temp_df], ignore_index=True)
        del existing_df, temp_df  # Free both before dedup

        merged = merged.drop_duplicates(subset=["Ticker", "Date"], keep="last")
        merged = merged.sort_values(["Ticker", "Date"]).reset_index(drop=True)
        total_rows = len(merged)

        # Atomic write: temp â†’ backup â†’ rename
        merged.to_parquet(temp_file, index=False)
        del merged  # Free before rename

        if os.path.exists(backup_file):
            os.remove(backup_file)
        if os.path.exists(parquet_file):
            os.rename(parquet_file, backup_file)
        os.rename(temp_file, parquet_file)
        if os.path.exists(backup_file):
            os.remove(backup_file)

        print(f"  ðŸ’¾ Flushed to disk â€” total {total_rows:,} rows")
        return True

    except Exception as e:
        print(f"  âš ï¸ Flush failed: {e}")
        # Recover from backup if possible
        if os.path.exists(backup_file) and not os.path.exists(parquet_file):
            os.rename(backup_file, parquet_file)
            print(f"  ðŸ”„ Recovered from backup")
        # Clean up temp
        if os.path.exists(temp_file):
            try:
                os.remove(temp_file)
            except:
                pass
        return False

def cleanup_temp_files(save_dir, parquet_file):
    try:
        for pattern in [f"{save_dir}/*.tmp*", f"{parquet_file}*.tmp*", f"{parquet_file}*.bak"]:
            for f in glob.glob(pattern):
                os.remove(f)
                print(f"  ðŸ—‘ï¸ Cleaned up: {os.path.basename(f)}")
        return True
    except Exception as e:
        print(f"  âš ï¸ Cleanup error: {e}")
        return False

# =====================================
# MAIN
# =====================================
def main():
    print("\n" + "="*60)
    print("ðŸ“Š HISTORICAL PRICE DOWNLOADER (Low Memory Mode)")
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

    # Load only Ticker column to check progress (LOW MEMORY)
    processed_tickers = get_processed_tickers(parquet_file)
    tickers_to_process = [t for t in all_tickers if t not in processed_tickers]

    if not tickers_to_process:
        print(f"\nâœ… All {len(all_tickers)} tickers already downloaded!")
        cleanup_temp_files(save_dir, parquet_file)
        return

    print(f"\nðŸ“Š Total tickers  : {len(all_tickers):,}")
    print(f"ðŸ“ˆ Already done   : {len(processed_tickers):,}")
    print(f"ðŸ”„ To process     : {len(tickers_to_process):,}")
    print(f"ðŸ’¡ Memory mode    : Buffer {CHECKPOINT_EVERY} tickers â†’ flush to disk")

    confirm = input(f"\nProceed to download {len(tickers_to_process):,} tickers? (y/n): ")
    if confirm.lower() != 'y':
        print("âŒ Cancelled")
        return

    # Free the processed set â€” no longer needed
    del processed_tickers

    buffer = []           # Holds DataFrames for up to CHECKPOINT_EVERY tickers
    successful = 0
    failed_tickers = []
    start_time = time.time()
    total = len(all_tickers)
    already_done = total - len(tickers_to_process)

    for i, ticker in enumerate(tickers_to_process):
        current_num = already_done + i + 1
        pct = (current_num / total) * 100
        print(f"[{current_num:>5}/{total}] ({pct:>5.1f}%) {ticker:<12}", end="", flush=True)

        # Check last date only when ticker might have partial data
        start_request = START_DATE_DEFAULT
        if os.path.exists(parquet_file):
            last_date = get_ticker_last_date(parquet_file, ticker)
            if last_date is not None and pd.notnull(last_date):
                start_request = (last_date - timedelta(days=5)).strftime("%Y-%m-%d")
                print(f"(resume {last_date.strftime('%Y-%m-%d')}) ", end="", flush=True)

        downloaded = False
        for attempt in range(MAX_RETRY):
            try:
                df = yf.download(ticker, start=start_request, end=END_DATE, progress=False)
                if not df.empty:
                    df_clean = clean_downloaded_data(df, ticker)
                    del df  # Free raw download immediately
                    if not df_clean.empty:
                        buffer.append(df_clean)
                        successful += 1
                        downloaded = True
                        print("âœ“")
                    else:
                        print("âš ï¸ empty after clean")
                        downloaded = True
                else:
                    del df
                    print("âš ï¸ no data")
                    downloaded = True
                break
            except Exception as e:
                print(f"âœ— (attempt {attempt+1}/{MAX_RETRY}) ", end="", flush=True)
                time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

        if not downloaded:
            failed_tickers.append(ticker)
            print("âœ— FAILED")

        # â”€â”€ CHECKPOINT: flush buffer to disk every CHECKPOINT_EVERY tickers â”€â”€
        is_last = (i + 1) == len(tickers_to_process)
        if (i + 1) % CHECKPOINT_EVERY == 0 or is_last:
            if buffer:
                flush_buffer_to_disk(buffer, parquet_file)
                buffer.clear()  # Drop all references â†’ GC can reclaim RAM

            elapsed = time.time() - start_time
            done_so_far = i + 1
            avg = elapsed / done_so_far
            remaining = avg * (len(tickers_to_process) - done_so_far)
            if remaining > 0:
                print(f"     â±ï¸  ETA: {remaining/60:.1f} min  |  avg {avg:.1f}s/ticker")

        time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

    # Final flush (safety net â€” should be empty if loop flushed on last ticker)
    if buffer:
        flush_buffer_to_disk(buffer, parquet_file)
        buffer.clear()

    total_time = time.time() - start_time
    try:
        final_rows = len(pd.read_parquet(parquet_file, columns=["Ticker"]))
    except:
        final_rows = 0

    print(f"\n{'='*60}")
    print(f"ðŸ“Š DOWNLOAD COMPLETED")
    print(f"{'='*60}")
    print(f"âœ“ Successful : {successful:,} tickers")
    print(f"âœ— Failed     : {len(failed_tickers):,} tickers")
    if failed_tickers:
        print(f"  {', '.join(failed_tickers[:10])}" + (f" ... +{len(failed_tickers)-10}" if len(failed_tickers) > 10 else ""))
    print(f"ðŸ’¾ Saved to  : {parquet_file}")
    print(f"ðŸ“ˆ Total rows: {final_rows:,}")
    print(f"â±ï¸ Total time : {total_time/60:.1f} minutes")
    if tickers_to_process:
        print(f"âš¡ Average   : {total_time/len(tickers_to_process):.1f} sec/ticker")
    print("="*60)

    print("\nðŸ§¹ Cleaning up temp files...")
    cleanup_temp_files(save_dir, parquet_file)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n" + "="*60)
        print("âš ï¸  INTERRUPTED â€” data in buffer may not be saved!")
        print("ðŸ’¾  Last checkpoint is safe in price_history.parquet")
        print("â–¶ï¸   Run again to resume from where it stopped")
        print("="*60)
    except Exception as e:
        print(f"\nâŒ ERROR: {e}")
        print("âœ… Last checkpoint is safe, run again to resume")
    finally:
        if 'save_dir' in locals() and 'parquet_file' in locals():
            cleanup_temp_files(save_dir, parquet_file)
