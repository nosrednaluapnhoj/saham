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
GITHUB_RAW_BASE = "https://raw.githubusercontent.com/nosrednaluapnhoj/saham/main/data/tickers/"
BASE_STORAGE = "/storage/emulated/0/Saham"
START_DATE_DEFAULT = "2000-01-01"
END_DATE = datetime.today().strftime("%Y-%m-%d")
DELAY_MIN = 1
DELAY_MAX = 3
MAX_RETRY = 3
CHECKPOINT_EVERY = 10  # SAVE SETIAP 10 EMITEN (ubah dari 5 ke 10)

# =====================================
# FUNCTIONS
# =====================================
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
    """Simpan data ke file sementara lalu rename (atomic operation)"""
    temp_file = parquet_file + f".checkpoint_{checkpoint_num}.tmp"
    try:
        final_df.to_parquet(temp_file, index=False)
        # Atomic replace (works on most OS)
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
    """Ambil daftar ticker yang sudah terdownload dari file parquet"""
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
    """Bersihkan file temporary checkpoint yang tidak terpakai"""
    try:
        # Hapus semua file checkpoint temporary
        temp_files = glob.glob(f"{save_dir}/*.checkpoint_*.tmp")
        for temp_file in temp_files:
            os.remove(temp_file)
            print(f"  🗑️ Cleaned up: {os.path.basename(temp_file)}")
        
        # Hapus juga file checkpoint lama (selain yang utama)
        checkpoint_files = glob.glob(f"{parquet_file}.checkpoint_*.tmp")
        for cf in checkpoint_files:
            if cf != parquet_file:  # Jangan hapus file utama
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
    print("📊 HISTORICAL PRICE DOWNLOADER WITH CHECKPOINT RESUME")
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
    
    # Download ticker list
    all_tickers = download_ticker_list(selected_file)
    if not all_tickers:
        return
    
    # Nama negara = nama file tanpa .txt
    country_name = selected_file.replace('.txt', '')
    
    # Buat folder penyimpanan
    save_dir = f"{BASE_STORAGE}/{country_name}"
    os.makedirs(save_dir, exist_ok=True)
    parquet_file = f"{save_dir}/price_history.parquet"
    
    # Cek ticker yang sudah diproses
    processed_tickers, existing_df = get_processed_tickers(parquet_file)
    
    # Filter ticker yang belum diproses
    tickers_to_process = [t for t in all_tickers if t not in processed_tickers]
    
    if not tickers_to_process:
        print(f"\n✅ Semua {len(all_tickers)} ticker sudah terdownload!")
        # Bersihkan file temporary jika ada
        cleanup_temp_files(save_dir, parquet_file)
        return
    
    print(f"\n📊 Total tickers: {len(all_tickers):,}")
    print(f"📈 Already processed: {len(processed_tickers):,}")
    print(f"🔄 To process: {len(tickers_to_process):,}")
    
    # Konfirmasi lanjut
    confirm = input(f"\nLanjut download {len(tickers_to_process):,} ticker? (y/n): ")
    if confirm.lower() != 'y':
        print("❌ Dibatalkan")
        return
    
    # Siapkan checkpoint counter
    checkpoint_counter = len(processed_tickers) // CHECKPOINT_EVERY
    all_data = []
    successful_tickers = 0
    failed_tickers = []
    start_time = time.time()
    
    # Download data untuk setiap ticker yang belum diproses
    for i, ticker in enumerate(tickers_to_process):
        current_num = len(processed_tickers) + i + 1
        progress_pct = (current_num / len(all_tickers)) * 100
        
        # Tampilkan progress
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
                        downloaded = True  # skip tapi dianggap sukses
                        break
                else:
                    print("⚠️ no data")
                    downloaded = True  # skip tapi dianggap sukses
                    break
            except Exception as e:
                print(f"✗ (attempt {attempt+1})", end=" ")
                time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
        
        if not downloaded:
            failed_tickers.append(ticker)
            print("✗ FAILED")
        
        # CHECKPOINT: Simpan setiap 10 emiten (atau di akhir)
        if (i + 1) % CHECKPOINT_EVERY == 0 or (i + 1) == len(tickers_to_process):
            if all_data:
                # Gabungkan data baru dengan existing
                new_df = pd.concat(all_data, ignore_index=True)
                if existing_df is not None:
                    final_df = pd.concat([existing_df, new_df], ignore_index=True)
                else:
                    final_df = new_df
                
                # Hapus duplikat dan sort
                final_df = final_df.drop_duplicates(subset=["Ticker", "Date"], keep="last")
                final_df = final_df.sort_values(["Ticker", "Date"]).reset_index(drop=True)
                
                # Simpan checkpoint
                checkpoint_counter += 1
                save_checkpoint(final_df, parquet_file, checkpoint_counter, len(all_tickers))
                
                # Update existing_df untuk checkpoint berikutnya
                existing_df = final_df
                all_data = []  # Kosongkan buffer setelah disimpan
                
                # Tampilkan estimasi waktu selesai
                elapsed = time.time() - start_time
                avg_time = elapsed / (i + 1)
                remaining = avg_time * (len(tickers_to_process) - i - 1)
                if remaining > 0:
                    print(f"     ⏱️ ETA: {remaining/60:.1f} menit")
        
        time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
    
    # SAVE AKHIR (memastikan semua data tersimpan)
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
    
    # Laporan akhir
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
    
    # CLEANUP: Bersihkan file temporary
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
        # Tetap bersihkan file temporary meskipun ada error
        if 'save_dir' in locals() and 'parquet_file' in locals():
            cleanup_temp_files(save_dir, parquet_file)
