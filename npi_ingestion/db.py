from typing import List  
import concurrent.futures as cf 

class SupabaseProviderRepo:
    def __init__(self, supabase_client):
        self.supabase = supabase_client

    async def upsert_providers(self, records: List[dict]):
        if not records:
            print("No records to upsert.")
            return
        print(f"Preparing to upsert {len(records)} records.")
        print(f"First record: {records[0]}")
        print(f"Columns being sent: {list(records[0].keys())}")
        response = self.supabase.table("NPIRehabs").upsert(records, on_conflict=["npi_number"]).execute()
        print(f"Upserted {len(records)} records. Response: {response}")