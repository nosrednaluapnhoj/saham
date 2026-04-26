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
# URL raw GitHub untuk file ticker
GITHUB_RAW_BASE = "https://raw.githubusercontent.com/YOUR_USERNAME/YOUR_REPO/main/data/tickers/"

# Simpan ke storage HP
BASE_STORAGE = "/storage/emulated/0/Saham"
PARQUET_FILE_TEMPLATE = "{}/{{}}/price_history.parquet".format(BASE_STORAGE)

START_DATE_DEFAULT = "2000-01-01"
END_DATE = datetime.today().strftime("%Y-%m-%d")

DELAY_MIN = 1
DELAY_MAX = 3
MAX_RETRY = 3

# =====================================
# FUNCTIONS
# =====================================
def get_available_countries():
    """Mendapatkan daftar negara dari repository GitHub"""
    # Alternative: Bisa juga hardcode list negara
    countries = ["indonesia", "us", "singapore", "malaysia", "thailand", "korea", "japan", "china"]
    return countries

def download_ticker_list(country):
    """Download daftar ticker dari GitHub"""
    url = f"{GITHUB_RAW_BASE}{country}.txt"
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            tickers = [line.strip() for line in response.text.splitlines() if line.strip()]
            print(f"✓ Loaded {len(tickers)} tickers from {country}")
            return tickers
        else:
            print(f"✗ Failed to load {country}.txt (HTTP {response.status_code})")
            return []
    except Exception as e:
        print(f"✗ Error loading {country}.txt: {e}")
        return []

def create_country_directory(country):
    """Membuat direktori untuk negara tertentu"""
    country_dir = f"{BASE_STORAGE}/{country}"
    os.makedirs(country_dir, exist_ok=True)
    return country_dir

def get_parquet_path(country):
    """Mendapatkan path file Parquet untuk negara tertentu"""
    return f"{BASE_STORAGE}/{country}/price_history.parquet"

def load_existing_data(country):
    """Load existing Parquet data untuk negara tertentu"""
    parquet_path = get_parquet_path(country)
    if os.path.exists(parquet_path):
        try:
            df = pd.read_parquet(parquet_path)
            df["Date"] = pd.to_datetime(df["Date"])
            print(f"Loaded local data for {country}: {len(df)} rows")
            return df
        except Exception as e:
            print(f"Parquet read error for {country}: {e}")
    return None

def clean_downloaded_data(df, ticker_symbol):
    """Membersihkan data yang didownload"""
    if df is None or df.empty:
        return pd.DataFrame()

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    df = df.reset_index()
    df["Date"] = pd.to_datetime(df["Date"]).dt.tz_localize(None)
    df.columns = [str(col).strip() for col in df.columns]

    # Simpan ticker apa adanya (sudah termasuk suffix)
    df["Ticker"] = ticker_symbol

    required_cols = ["Ticker", "Date", "Open", "High", "Low", "Close", "Volume"]
    existing_cols = [c for c in required_cols if c in df.columns]

    return df[existing_cols]

def download_ticker_data(ticker, country, start_request, existing_df):
    """Download data untuk satu ticker"""
    ticker_clean = ticker  # Tidak menghapus suffix karena sudah lengkap
    
    if existing_df is not None:
        last_date = existing_df[
            existing_df["Ticker"] == ticker
        ]["Date"].max()
        
        if pd.notnull(last_date):
            start_request = (last_date - timedelta(days=5)).strftime("%Y-%m-%d")
    
    for attempt in range(MAX_RETRY):
        try:
            df = yf.download(
                ticker,
                start=start_request,
                end=END_DATE,
                progress=False
            )
            
            if not df.empty:
                df_clean = clean_downloaded_data(df, ticker)
                if not df_clean.empty:
                    return df_clean
                else:
                    print(f"  → No valid columns for {ticker}")
            else:
                print(f"  → No data for {ticker}")
            
        except Exception as e:
            if attempt < MAX_RETRY - 1:
                print(f"  Retry {attempt+1}/{MAX_RETRY}: {str(e)[:50]}")
                time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
            else:
                print(f"  Failed: {str(e)[:50]}")
        
        time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
    
    return None

def process_country(country, tickers):
    """Proses download untuk satu negara"""
    print(f"\n{'='*50}")
    print(f"Processing: {country.upper()}")
    print(f"{'='*50}")
    
    # Buat direktori untuk negara
    create_country_directory(country)
    
    # Load existing data
    existing_df = load_existing_data(country)
    
    all_data = []
    start_request = START_DATE_DEFAULT
    
    for i, ticker in enumerate(tickers):
        print(f"[{i+1}/{len(tickers)}] {ticker}")
        
        df_clean = download_ticker_data(ticker, country, start_request, existing_df)
        
        if df_clean is not None and not df_clean.empty:
            all_data.append(df_clean)
            print(f"  ✓ Downloaded {len(df_clean)} rows")
        else:
            print(f"  ✗ No data downloaded")
    
    if not all_data:
        print(f"No new data for {country}")
        return
    
    # Gabungkan data baru
    new_df = pd.concat(all_data, ignore_index=True)
    
    # Merge dengan data existing
    if existing_df is not None:
        final_df = pd.concat([existing_df, new_df], ignore_index=True)
    else:
        final_df = new_df
    
    # Bersihkan dan sort
    final_df = final_df.drop_duplicates(subset=["Ticker", "Date"], keep="last")
    final_df = final_df.sort_values(["Ticker", "Date"]).reset_index(drop=True)
    
    # Simpan
    parquet_path = get_parquet_path(country)
    final_df.to_parquet(parquet_path, index=False)
    print(f"\n✓ Saved {len(final_df)} total rows to: {parquet_path}\n")

def interactive_menu():
    """Menu interaktif untuk memilih negara dan ticker"""
    print("\n" + "="*50)
    print("   STOCK DATA COLLECTOR v2.0")
    print("="*50)
    
    # Tampilkan daftar negara yang tersedia
    countries = get_available_countries()
    
    print("\nAvailable countries:")
    for i, country in enumerate(countries, 1):
        print(f"  {i}. {country.title()}")
    print(f"  {len(countries)+1}. Custom input (manual ticker list)")
    print(f"  {len(countries)+2}. Exit")
    
    # Pilihan negara
    while True:
        try:
            choice = int(input("\nSelect country (number): "))
            if 1 <= choice <= len(countries):
                selected_country = countries[choice-1]
                break
            elif choice == len(countries)+1:
                return "custom", None
            elif choice == len(countries)+2:
                return "exit", None
            else:
                print("Invalid choice. Try again.")
        except ValueError:
            print("Please enter a number.")
    
    # Konfirmasi download
    print(f"\nSelected: {selected_country.title()}")
    confirm = input("Download data? (y/n): ").lower()
    if confirm != 'y':
        return "exit", None
    
    return "country", selected_country

def custom_ticker_input():
    """Input custom ticker list dari user"""
    print("\n" + "="*50)
    print("   CUSTOM TICKER INPUT")
    print("="*50)
    print("Enter tickers (one per line)")
    print("Include suffix like .JK, .KLSE, etc.")
    print("Press Enter twice to finish:\n")
    
    tickers = []
    while True:
        ticker = input().strip()
        if not ticker:
            if tickers:
                break
            else:
                print("Please enter at least one ticker")
                continue
        tickers.append(ticker)
        print(f"  Added: {ticker}")
    
    if not tickers:
        return None
    
    print(f"\nTotal tickers: {len(tickers)}")
    confirm = input("Download data for these tickers? (y/n): ").lower()
    
    if confirm == 'y':
        country_name = input("Enter country name for storage (e.g., 'custom'): ").strip()
        if not country_name:
            country_name = "custom"
        return country_name, tickers
    else:
        return None

def update_github_url():
    """Update GitHub URL di awal script"""
    print("\n" + "="*50)
    print("   GITHUB CONFIGURATION")
    print("="*50)
    print("Please edit the GITHUB_RAW_BASE variable in this script:")
    print('GITHUB_RAW_BASE = "https://raw.githubusercontent.com/YOUR_USERNAME/YOUR_REPO/main/data/tickers/"')
    print("\nReplace YOUR_USERNAME and YOUR_REPO with your actual GitHub info")
    print("\nExample structure in GitHub:")
    print("  your-repo/")
    print("  └── data/")
    print("      └── tickers/")
    print("          ├── indonesia.txt")
    print("          ├── us.txt")
    print("          └── ...")
    input("\nPress Enter to continue...")

# =====================================
# MAIN
# =====================================
def main():
    # Update URL jika diperlukan (opsional)
    # update_github_url()
    
    while True:
        choice, value = interactive_menu()
        
        if choice == "exit":
            print("\nGoodbye!")
            break
        
        elif choice == "country":
            country = value
            tickers = download_ticker_list(country)
            
            if tickers:
                process_country(country, tickers)
            else:
                print(f"\nFailed to load tickers for {country}")
        
        elif choice == "custom":
            result = custom_ticker_input()
            if result:
                country_name, tickers = result
                process_country(country_name, tickers)
            else:
                print("Custom input cancelled")
        
        print("\n" + "-"*50)
        another = input("Process another country? (y/n): ").lower()
        if another != 'y':
            print("\nGoodbye!")
            break

if __name__ == "__main__":
    main()