import yfinance as yf
import pandas as pd
import os
import time
import random
from datetime import datetime, timedelta
import requests

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

# =====================================
# MAIN
# =====================================
def main():
    # Ambil daftar file ticker dari GitHub
    ticker_files = get_ticker_files()
    
    if not ticker_files:
        print("Tidak bisa mengambil daftar file dari GitHub")
        return
    
    # Tampilkan pilihan
    print("\nFile ticker yang tersedia:")
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
            print("Masukkan nomor yang benar")
    
    # Download ticker list
    tickers = download_ticker_list(selected_file)
    if not tickers:
        return
    
    # Nama negara = nama file tanpa .txt
    country_name = selected_file.replace('.txt', '')
    
    # Buat folder penyimpanan
    save_dir = f"{BASE_STORAGE}/{country_name}"
    os.makedirs(save_dir, exist_ok=True)
    parquet_file = f"{save_dir}/price_history.parquet"
    
    # Load existing data
    existing_df = None
    if os.path.exists(parquet_file):
        try:
            existing_df = pd.read_parquet(parquet_file)
            existing_df["Date"] = pd.to_datetime(existing_df["Date"])
            print(f"Loaded existing data: {len(existing_df)} rows")
        except:
            pass
    
    # Download data untuk setiap ticker
    all_data = []
    for i, ticker in enumerate(tickers):
        print(f"[{i+1}/{len(tickers)}] {ticker}")
        
        start_request = START_DATE_DEFAULT
        if existing_df is not None:
            last_date = existing_df[existing_df["Ticker"] == ticker]["Date"].max()
            if pd.notnull(last_date):
                start_request = (last_date - timedelta(days=5)).strftime("%Y-%m-%d")
        
        for attempt in range(MAX_RETRY):
            try:
                df = yf.download(ticker, start=start_request, end=END_DATE, progress=False)
                if not df.empty:
                    df_clean = clean_downloaded_data(df, ticker)
                    if not df_clean.empty:
                        all_data.append(df_clean)
                    break
            except Exception as e:
                time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
        
        time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
    
    if not all_data:
        print("Tidak ada data baru")
        return
    
    # Gabungkan dan simpan
    new_df = pd.concat(all_data, ignore_index=True)
    final_df = pd.concat([existing_df, new_df], ignore_index=True) if existing_df is not None else new_df
    final_df = final_df.drop_duplicates(subset=["Ticker", "Date"], keep="last")
    final_df = final_df.sort_values(["Ticker", "Date"]).reset_index(drop=True)
    final_df.to_parquet(parquet_file, index=False)
    
    print(f"\n✓ Saved to: {parquet_file}")
    print(f"✓ Total rows: {len(final_df)}")

if __name__ == "__main__":
    main()
