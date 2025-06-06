import os
import re
import zipfile
import requests
import shutil 
import pandas as pd 

NPPES_ZIP_URL = os.environ.get("NPPES_ZIP_URL", "https://download.cms.gov/nppes/NPPES_Data_Dissemination_May_2025_V2.zip")
TEMP_DIR = "/tmp/nppes_data"
FINAL_CSV_PATH = os.path.join(TEMP_DIR, "npidata.csv")
CSV_REGEX = re.compile(r"^npidata_pfile_\d{8}-\d{8}\.csv$", re.IGNORECASE)

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

if __name__ == "__main__":
    csv_path = download_and_extract_csv()
    print(f"Extracted CSV path: {csv_path}") 
    