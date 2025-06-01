import requests
import pandas as pd
from taxonomy_func import get_groupings_and_codes_for_specific_taxonomies

NPI_API_URL = "https://npiregistry.cms.hhs.gov/api/"


def fetch_npi_providers_by_taxonomy_codes(codes, limit=50):
    params = {
        "version": "2.1",
        "taxonomy": ",".join(codes),
        "limit": limit,
        "pretty": "true",
    }
    resp = requests.get(NPI_API_URL, params=params)
    resp.raise_for_status()
    data = resp.json()
    return data.get("results", [])


def main():
    print("Getting residential and substance abuse taxonomy codes...")
    taxonomies = get_groupings_and_codes_for_specific_taxonomies()
    codes = taxonomies["Code"].tolist()
    print(f"Using taxonomy codes (total {len(codes)}):")
    for _, row in taxonomies.iterrows():
        print(f"  Grouping: {row['Grouping']}, Code: {row['Code']}")

    print(f"\nSearching NPI API for all taxonomy codes in one request (limit=50)...")
    providers = fetch_npi_providers_by_taxonomy_codes(codes, limit=50)
    print(f"Found {len(providers)} providers.")
    for p in providers:
        basic = p.get("basic", {})
        name = basic.get("organization_name") or basic.get("name")
        npi = p.get("number")
        tax = next((t for t in p.get("taxonomies", []) if t.get("primary")), None)
        tax_code = tax.get("code") if tax else None
        tax_desc = tax.get("desc") if tax else None
        addr = next((a for a in p.get("addresses", []) if a.get("address_purpose") == "LOCATION"), None)
        city = addr.get("city") if addr else None
        state = addr.get("state") if addr else None
        print(f"  NPI: {npi}, Name: {name}, Taxonomy: {tax_code} - {tax_desc}, Location: {city}, {state}")
    print(f"\nTotal unique providers fetched: {len(providers)} (capped at 50 total)")

if __name__ == "__main__":
    main() 