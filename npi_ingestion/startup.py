import os
import re
import zipfile
import requests
import pandas as pd
import shutil
from npi_ingestion.config import get_supabase_client
from npi_ingestion.db import SupabaseProviderRepo 

NPPES_ZIP_URL = os.environ.get("NPPES_ZIP_URL", "https://download.cms.gov/nppes/NPPES_Data_Dissemination_May_2025_V2.zip")
TEMP_DIR = "/tmp/nppes_data"
FINAL_CSV_PATH = os.path.join(TEMP_DIR, "npidata.csv")
CSV_REGEX = re.compile(r"^npidata_pfile_\d{8}-\d{8}\.csv$", re.IGNORECASE)
TARGET_CODES = {"324500000X", "3245S0500X"}
TAXONOMY_COLS = [f"Healthcare Provider Taxonomy Code_{i}" for i in range(1, 16)]

CSV_TO_MODEL = {
    "NPI": "npi_number",
    "Provider Organization Name (Legal Business Name)": "organization_name",
    "Provider First Line Business Mailing Address": "address",
    "Provider Business Mailing Address City Name": "city",
    "Provider Business Mailing Address State Name": "state",
    "Provider Business Mailing Address Postal Code": "postal_code",
    "Provider Business Mailing Address Telephone Number": "phone",
    "Healthcare Provider Taxonomy Code_1": "taxonomy_code",
    "Healthcare Provider Taxonomy Group_1": "taxonomy_desc",
    # Authorized official: concatenate first, middle, last name
    # Last Update Date: direct mapping
    "Last Update Date": "last_updated"
}

REQUIRED_MODEL_COLS = [
    "npi_number", "organization_name", "address", "city", "state",
    "postal_code", "phone", "taxonomy_code",
    "last_updated"
]

def truncate_columns(df):
    max_lengths = {
        "npi_number": 10,
        "city": 255,
        "state": 255,
        "postal_code": 255,
        "phone": 255,
        "taxonomy_code": 255,
    }
    for col, max_len in max_lengths.items():
        if col in df.columns:
            df[col] = df[col].apply(lambda x: x[:max_len] if isinstance(x, str) else x)
    return df

def download_and_extract_csv():
    os.makedirs(TEMP_DIR, exist_ok=True)
    zip_path = os.path.join(TEMP_DIR, "nppes.zip")
    print("⬇️ Downloading NPPES ZIP...")
    try:
        with requests.get(NPPES_ZIP_URL, stream=True) as r:
            r.raise_for_status()
            with open(zip_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        print("✅ Downloaded.")
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            matched_files = [name for name in zip_ref.namelist() if CSV_REGEX.match(name)]
            if not matched_files:
                raise FileNotFoundError("❌ No matching npidata_pfile CSV found.")
            target_file = matched_files[0]
            zip_ref.extract(target_file, TEMP_DIR)
            extracted_path = os.path.join(TEMP_DIR, target_file)
            print(f"✅ Extracted: {extracted_path}")
            # Move the extracted CSV to FINAL_CSV_PATH (overwrite if exists)
            if os.path.exists(FINAL_CSV_PATH):
                os.remove(FINAL_CSV_PATH)
            os.rename(extracted_path, FINAL_CSV_PATH)
            print(f"✅ Moved CSV to: {FINAL_CSV_PATH}")
        # Delete all files in TEMP_DIR except the CSV
        for fname in os.listdir(TEMP_DIR):
            fpath = os.path.join(TEMP_DIR, fname)
            if fpath != FINAL_CSV_PATH:
                if os.path.isfile(fpath) or os.path.islink(fpath):
                    os.remove(fpath)
                elif os.path.isdir(fpath):
                    shutil.rmtree(fpath)
        print(f"🧹 Cleaned up temp directory, only CSV remains: {FINAL_CSV_PATH}")
        return FINAL_CSV_PATH
    finally:
        print('Clean up Process Completed')  # No full temp dir deletion, only cleanup above

def filter_and_deduplicate(csv_path):
    print("🔎 Filtering and deduplicating...")
    reader = pd.read_csv(csv_path, nrows=0)
    all_cols = reader.columns.tolist()
    taxonomy_cols = [col for col in TAXONOMY_COLS if col in all_cols]
    chunksize = 100_000
    filtered_rows = []
    for chunk in pd.read_csv(csv_path, dtype=str, chunksize=chunksize, usecols=all_cols):
        mask = chunk[taxonomy_cols].apply(lambda row: any(code in TARGET_CODES for code in row if isinstance(code, str)), axis=1)
        filtered_rows.append(chunk[mask])
    df = pd.concat(filtered_rows, ignore_index=True)
    before = len(df)
    df = df.drop_duplicates()
    after = len(df)
    print(f"Rows before deduplication: {before:,}, after: {after:,}")
    return df

def upsert_to_supabase(df):
    print("⬆️ Upserting to Supabase...")
    # Rename columns to match model/database
    df = df.rename(columns=CSV_TO_MODEL)
    # Ensure all required columns are present
    for col in REQUIRED_MODEL_COLS:
        if col not in df.columns:
            df[col] = "Unknown"
    # Only keep columns that exist in the model/database and are required
    df = df[[col for col in REQUIRED_MODEL_COLS if col in df.columns]]
    # Fill NaN/None/null with empty string
    df = df.fillna("Unknown")
    # Ensure all values are strings
    df = df.astype(str)
    # Truncate columns to max lengths
    df = truncate_columns(df)
    print("Sample row for upsert:")
    print(df.iloc[0].to_dict() if not df.empty else "DataFrame is empty!")
    supabase = get_supabase_client()
    repo = SupabaseProviderRepo(supabase)
    records = df.to_dict(orient="records")
    repo.upsert_providers(records)
    print("✅ Upsert complete.")

def startup_ingest():
    csv_path = download_and_extract_csv()
    df = filter_and_deduplicate(csv_path)
    upsert_to_supabase(df) 