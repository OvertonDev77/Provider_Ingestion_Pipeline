from typing import List

class SupabaseProviderRepo:
    def __init__(self, supabase_client):
        self.supabase = supabase_client

    def upsert_providers(self, records: List[dict]):
        if not records:
            print("No records to upsert.")
            return
        print(f"Preparing to upsert {len(records)} records.")
        print(f"First record: {records[0]}")
        print(f"Columns being sent: {list(records[0].keys())}")  

        # NOTE: Upserting one record at a time is not efficient for large datasets, but is useful for debugging.
        for idx, record in enumerate(records): 
            assert "npi_number" in record.keys() 
            print(f"Upserting record {idx+1}/{len(records)}: {record}") 
            print("Record being inserted\n\n",record)
            try:
                response = self.supabase.table("NPIRehabs").upsert(record, on_conflict=["npi_number"]).execute()
                print(f"Upserted record {idx+1}. Response: {response}")
            except Exception as e:
                print(f"Error upserting record {idx+1}: {e}")
                # The assert above is incorrect - should be checking record.keys() not records.keys()