import os
import re
import zipfile
import requests
import pandas as pd
from npi_ingestion.config import get_supabase_client
from npi_ingestion.db import SupabaseProviderRepo
from npi_ingestion.models import Provider

NPPES_ZIP_URL = "https://download.cms.gov/nppes/NPPES_Data_Dissemination_May_2025_V2.zip"
TEMP_DIR = "/tmp/nppes_data"
CSV_REGEX = re.compile(r"^npidata_pfile_\d{8}-\d{8}\.csv$", re.IGNORECASE)
TARGET_CODES = {"324500000X", "3245S0500X"}
TAXONOMY_COLS = [f"Healthcare Provider Taxonomy Code_{i}" for i in range(1, 16)]

def download_and_extract_csv():
    os.makedirs(TEMP_DIR, exist_ok=True)
    zip_path = os.path.join(TEMP_DIR, "nppes.zip")
    print("⬇️ Downloading NPPES ZIP...")
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
        return extracted_path

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
    supabase = get_supabase_client()
    repo = SupabaseProviderRepo(supabase)
    providers = [Provider(**row) for row in df.to_dict(orient="records")]
    repo.upsert_providers(providers)
    print("✅ Upsert complete.")

def startup_ingest():
    csv_path = download_and_extract_csv()
    df = filter_and_deduplicate(csv_path)
    upsert_to_supabase(df) 