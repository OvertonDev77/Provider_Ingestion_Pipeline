from fastapi import FastAPI, BackgroundTasks
import asyncio
from npi_ingestion.startup import startup_ingest
from npi_ingestion.config import get_supabase_client
from npi_ingestion.api_client import NPIClient
from npi_ingestion.db import SupabaseProviderRepo
from npi_ingestion.ingestion_service import IngestionService
from npi_ingestion.utils import get_logger

RETRY_DELAY = 60  # seconds to wait before retrying on error

app = FastAPI()
logger = get_logger("npi_ingestion.server")

# On startup: download, extract, filter, and upsert the CSV
@app.on_event("startup")
def on_startup():
    startup_ingest()

# Health check endpoint
@app.get("/")
def root():
    return {"status": "NPI ingestion service is running."}

# Background ingestion loop for NPI API
async def ingestion_loop():
    supabase = get_supabase_client()
    api_client = NPIClient()
    repo = SupabaseProviderRepo(supabase)
    service = IngestionService(api_client, repo)
    try:
        logger.info("Starting NPI ingestion pipeline...")
        await service.ingest()
        logger.info("NPI ingestion pipeline complete.")
    except Exception as e:
        logger.error(f"Error in ingestion pipeline: {e}")
        logger.info(f"Retrying in {RETRY_DELAY} seconds...")
        await asyncio.sleep(RETRY_DELAY)

# POST route to trigger NPI API ingestion pipeline
@app.post("/run-npi-workflow")
def run_npi_workflow(background_tasks: BackgroundTasks):
    logger.info("Manual trigger received for NPI ingestion pipeline.")
    background_tasks.add_task(asyncio.run, ingestion_loop())
    return {"status": "Manual ingestion pipeline started in background."} 