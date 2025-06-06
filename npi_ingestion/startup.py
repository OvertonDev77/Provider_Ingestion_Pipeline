import os
import re
import zipfile
import requests
import pandas as pd
import shutil
from npi_ingestion.config import get_supabase_client
from npi_ingestion.db import SupabaseProviderRepo
from npi_ingestion.models import Provider
from npi_ingestion.csv_download_extract import download_and_extract_csv

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
    "postal_code", "phone", "taxonomy_code", "taxonomy_desc",
    "authorized_official", "last_updated"
]

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
    # Concatenate authorized official name fields
    df["authorized_official"] = (
        df.get("Authorized Official First Name", "").fillna("") + " " +
        df.get("Authorized Official Middle Name", "").fillna("") + " " +
        df.get("Authorized Official Last Name", "").fillna("")
    ).str.strip().replace("  ", " ", regex=False)
    # Only keep columns that exist in the model/database
    df = df[[col for col in REQUIRED_MODEL_COLS if col in df.columns]]
    # Fill NaN/None/null with 'unknown'
    df = df.fillna("unknown")
    print("Sample row for upsert:")
    print(df.iloc[0].to_dict() if not df.empty else "DataFrame is empty!")
    supabase = get_supabase_client()
    repo = SupabaseProviderRepo(supabase)
    providers = [Provider(**row) for row in df.to_dict(orient="records")]
    repo.upsert_providers(providers)
    print("✅ Upsert complete.")

def startup_ingest():
    csv_path = download_and_extract_csv()
    df = filter_and_deduplicate(csv_path)
    upsert_to_supabase(df) 