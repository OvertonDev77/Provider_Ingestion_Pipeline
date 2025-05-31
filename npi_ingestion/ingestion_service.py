from .api_client import NPIClient
from .db import SupabaseProviderRepo

class IngestionService:
    def __init__(self, api_client: NPIClient, repo: SupabaseProviderRepo):
        self.api_client = api_client
        self.repo = repo

    async def ingest(self):
        providers = await self.api_client.fetch_all_providers()
        self.repo.upsert_providers(providers) 