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
CHECKPOINT_EVERY = 10  # SAVE EVERY 10 TICKERS (changed from 5 to 10)

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
            print(f"✓ Loaded {len(tickers)} tickers from {filename}")
            return tickers
    except Exception as e:
        print(f"✗ Error: {e}")
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
        print(f"  💾 CHECKPOINT {checkpoint_num}: Saved {len(final_df)} rows ({checkpoint_num*CHECKPOINT_EVERY if checkpoint_num*CHECKPOINT_EVERY <= total_tickers else total_tickers}/{total_tickers} tickers processed)")
        return True
    except Exception as e:
        print(f"  ⚠️ Failed to save checkpoint: {e}")
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
            processed = existing_df['Ticker'].unique().tolist()
            print(f"✓ Found {len(processed)} already processed tickers")
            return processed, existing_df
        except Exception as e:
            print(f"⚠️ Error reading existing file: {e}")
            return [], None
    return [], None

def cleanup_temp_files(save_dir, parquet_file):
    """Clean up unused temporary checkpoint files"""
    try:
        temp_files = glob.glob(f"{save_dir}/*.checkpoint_*.tmp")
        for temp_file in temp_files:
            os.remove(temp_file)
            print(f"  🗑️ Cleaned up: {os.path.basename(temp_file)}")
        
        checkpoint_files = glob.glob(f"{parquet_file}.checkpoint_*.tmp")
        for cf in checkpoint_files:
            if cf != parquet_file:
                os.remove(cf)
                print(f"  🗑️ Cleaned up: {os.path.basename(cf)}")
        
        return True
    except Exception as e:
        print(f"  ⚠️ Failed to cleanup: {e}")
        return False

# =====================================
# MAIN
# =====================================
def main():
    print("\n" + "="*60)
    print("📊 HISTORICAL PRICE DOWNLOADER")
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
    
    all_tickers = download_ticker_list(selected_file)
    if not all_tickers:
        return
    
    country_name = selected_file.replace('.txt', '')
    
    save_dir = f"{BASE_STORAGE}/{country_name}"
    os.makedirs(save_dir, exist_ok=True)
    parquet_file = f"{save_dir}/price_history.parquet"
    
    processed_tickers, existing_df = get_processed_tickers(parquet_file)
    tickers_to_process = [t for t in all_tickers if t not in processed_tickers]
    
    if not tickers_to_process:
        print(f"\n✅ All {len(all_tickers)} tickers already downloaded!")
        cleanup_temp_files(save_dir, parquet_file)
        return
    
    print(f"\n📊 Total tickers: {len(all_tickers):,}")
    print(f"📈 Already processed: {len(processed_tickers):,}")
    print(f"🔄 To process: {len(tickers_to_process):,}")
    
    confirm = input(f"\nProceed to download {len(tickers_to_process):,} tickers? (y/n): ")
    if confirm.lower() != 'y':
        print("❌ Cancelled")
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
        
        start_request = START_DATE_DEFAULT
        if existing_df is not None:
            last_date = existing_df[existing_df["Ticker"] == ticker]["Date"].max()
            if pd.notnull(last_date):
                start_request = (last_date - timedelta(days=5)).strftime("%Y-%m-%d")
                print(f"(from {last_date.strftime('%Y-%m-%d')}) ", end="")
        
        downloaded = False
        for attempt in range(MAX_RETRY):
            try:
                df = yf.download(ticker, start=start_request, end=END_DATE, progress=False)
                if not df.empty:
                    df_clean = clean_downloaded_data(df, ticker)
                    if not df_clean.empty:
                        all_data.append(df_clean)
                        successful_tickers += 1
                        downloaded = True
                        print("✓")
                        break
                    else:
                        print("⚠️ empty")
                        downloaded = True
                        break
                else:
                    print("⚠️ no data")
                    downloaded = True
                    break
            except Exception as e:
                print(f"✗ (attempt {attempt+1})", end=" ")
                time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
        
        if not downloaded:
            failed_tickers.append(ticker)
            print("✗ FAILED")
        
        if (i + 1) % CHECKPOINT_EVERY == 0 or (i + 1) == len(tickers_to_process):
            if all_data:
                new_df = pd.concat(all_data, ignore_index=True)
                if existing_df is not None:
                    final_df = pd.concat([existing_df, new_df], ignore_index=True)
                else:
                    final_df = new_df
                
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
                    print(f"     ⏱️ ETA: {remaining/60:.1f} minutes")
        
        time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
    
    if all_data:
        new_df = pd.concat(all_data, ignore_index=True)
        if existing_df is not None:
            final_df = pd.concat([existing_df, new_df], ignore_index=True)
        else:
            final_df = new_df
        
        final_df = final_df.drop_duplicates(subset=["Ticker", "Date"], keep="last")
        final_df = final_df.sort_values(["Ticker", "Date"]).reset_index(drop=True)
        final_df.to_parquet(parquet_file, index=False)
        print(f"\n💾 FINAL SAVE: {len(final_df)} total rows")
    
    total_time = time.time() - start_time
    print(f"\n" + "="*60)
    print(f"📊 DOWNLOAD COMPLETED")
    print(f"="*60)
    print(f"✓ Successful: {successful_tickers:,} tickers")
    print(f"✗ Failed: {len(failed_tickers):,} tickers")
    if failed_tickers:
        print(f"  Failed list: {', '.join(failed_tickers[:10])}")
        if len(failed_tickers) > 10:
            print(f"  ... and {len(failed_tickers)-10} more")
    print(f"💾 Saved to: {parquet_file}")
    print(f"📈 Total rows: {len(final_df) if 'final_df' in locals() else len(existing_df) if existing_df is not None else 0}")
    print(f"⏱️ Total time: {total_time/60:.1f} minutes")
    print(f"⚡ Average: {total_time/len(tickers_to_process):.1f} seconds/ticker")
    print("="*60)
    
    print("\n🧹 Cleaning up temporary files...")
    cleanup_temp_files(save_dir, parquet_file)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n" + "="*60)
        print("⚠️ PROCESS INTERRUPTED BY USER")
        print("="*60)
        print("✅ Data from completed tickers has been saved to checkpoint files")
        print("💾 Final file (price_history.parquet) is safe")
        print("▶️ Run the script again to resume from where it stopped")
        print("="*60)
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        print("✅ Checkpoint data is safe, you can resume by running again")
    finally:
        if 'save_dir' in locals() and 'parquet_file' in locals():
            cleanup_temp_files(save_dir, parquet_file)
