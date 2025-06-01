import pandas as pd

def main():
    input_csv = "filtered_npi_providers.csv"
    output_csv = "filtered_npi_providers_deduped.csv"

    print(f"Loading {input_csv}...")
    df = pd.read_csv(input_csv, dtype=str)
    print(f"Rows before deduplication: {len(df):,}")

    df_deduped = df.drop_duplicates()
    print(f"Rows after deduplication: {len(df_deduped):,}")

    df_deduped.to_csv(output_csv, index=False)
    print(f"Deduplicated file written to {output_csv}")

if __name__ == "__main__":
    main() 