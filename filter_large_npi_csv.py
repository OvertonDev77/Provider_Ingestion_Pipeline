import pandas as pd
import multiprocessing as mp

# Your target taxonomy codes
TARGET_CODES = {
    "324500000X", "3245S0500X"
}

# All taxonomy code columns in the NPPES file
TAXONOMY_COLS = [
    f"Healthcare Provider Taxonomy Code_{i}" for i in range(1, 16)
]

def filter_chunk(chunk):
    # Returns only rows where any taxonomy code column matches a target code
    mask = chunk[TAXONOMY_COLS].apply(lambda row: any(code in TARGET_CODES for code in row if isinstance(code, str)), axis=1)
    return chunk[mask]

def main():
    input_csv =  r"C:\Users\Pinda\Downloads\NPPES_Data_Dissemination_May_2025_V2\npidata_pfile_20050523-20250511.csv"

    output_csv = "filtered_npi_providers.csv"
    chunksize = 100_000  # Tune this for your RAM

    # Read header to get columns
    reader = pd.read_csv(input_csv, nrows=0)
    all_cols = reader.columns.tolist() 
    print("all_cols", all_cols)
    # Ensure taxonomy columns exist
    taxonomy_cols = [col for col in TAXONOMY_COLS if col in all_cols]
    if not taxonomy_cols:
        raise ValueError("No taxonomy code columns found in CSV.")

    # Use a process pool for parallel filtering
    with mp.Pool(mp.cpu_count()) as pool, open(output_csv, "w", encoding="utf-8", newline="") as out_f:
        writer = None
        for filtered in pool.imap(
            filter_chunk,
            pd.read_csv(input_csv, dtype=str, chunksize=chunksize, usecols=all_cols)
        ):
            if writer is None:
                filtered.to_csv(out_f, index=False, header=True)
                writer = True
            else:
                filtered.to_csv(out_f, index=False, header=False)

if __name__ == "__main__":
    main()