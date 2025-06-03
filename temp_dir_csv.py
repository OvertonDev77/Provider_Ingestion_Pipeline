import re
import zipfile
import os
import tempfile
import requests

# Step 1: Prepare download URL and temp folder
zip_url = "https://download.cms.gov/nppes/NPPES_Data_Dissemination_May_2025_V2.zip"
temp_dir = tempfile.mkdtemp()
zip_path = os.path.join(temp_dir, "nppes.zip")

# Step 2: Download the file
print("⬇️ Downloading NPPES ZIP...")
with requests.get(zip_url, stream=True) as r:
    r.raise_for_status()
    with open(zip_path, "wb") as f:
        for chunk in r.iter_content(chunk_size=8192):
            f.write(chunk)
print("✅ Downloaded.")

# Step 3: Regex pattern for valid npidata_pfile CSV
pattern = re.compile(r"^npidata_pfile_\d{8}-\d{8}\.csv$", re.IGNORECASE)

# Step 4: Extract matching file only
with zipfile.ZipFile(zip_path, 'r') as zip_ref:
    matched_files = [name for name in zip_ref.namelist() if pattern.match(name)]

    if not matched_files:
        raise FileNotFoundError("❌ No matching npidata_pfile CSV found.")

    # Should only be one match — extract it
    target_file = matched_files[0]
    zip_ref.extract(target_file, temp_dir)
    extracted_path = os.path.join(temp_dir, target_file)
    print(f"✅ Extracted: {extracted_path}")
