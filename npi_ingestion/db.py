from typing import List
from .models import Provider

class SupabaseProviderRepo:
    def __init__(self, supabase_client):
        self.supabase = supabase_client

    def upsert_providers(self, providers: List[Provider]):
        records = [p.dict() for p in providers]
        response = self.supabase.table("NPIRehabs").upsert(records, on_conflict=["npi_number"]).execute()
        print(f"Upserted {len(records)} records. Response: {response}") 