from fastapi import FastAPI
import asyncio
from npi_ingestion.config import get_supabase_client
from npi_ingestion.api_client import NPIClient
from npi_ingestion.db import SupabaseProviderRepo
from npi_ingestion.ingestion_service import IngestionService
from npi_ingestion.utils import get_logger

RETRY_DELAY = 60  # seconds to wait before retrying on error

app = FastAPI()
logger = get_logger("npi_ingestion.server")

async def ingestion_loop():
    supabase = get_supabase_client()
    api_client = NPIClient()
    repo = SupabaseProviderRepo(supabase)
    service = IngestionService(api_client, repo)
    while True:
        try:
            logger.info("Starting NPI ingestion pipeline...")
            await service.ingest()
            logger.info("NPI ingestion pipeline complete. Waiting for next trigger...")
            await asyncio.sleep(3600)  # Wait 1 hour before next run (adjust as needed)
        except Exception as e:
            logger.error(f"Error in ingestion pipeline: {e}")
            logger.info(f"Retrying in {RETRY_DELAY} seconds...")
            await asyncio.sleep(RETRY_DELAY)

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(ingestion_loop())

@app.post("/run-npi-workflow")
def run_npi_workflow():
    logger.info("Manual trigger received for NPI ingestion pipeline.")
    asyncio.create_task(ingestion_loop())
    return {"status": "Manual ingestion pipeline started in background."} 