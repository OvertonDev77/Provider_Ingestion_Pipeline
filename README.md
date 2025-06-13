# Rehab Provider Ingestion Pipeline

This project ingests up-to-date rehab provider data into a Supabase/Postgres database for use in a rehab-finding application. It automates the download, parsing, and upsert of provider data from the NPPES (National Plan and Provider Enumeration System) registry, focusing on substance abuse and mental health facilities.

## Features

- **Automated NPPES Data Ingestion**: On startup, the FastAPI server downloads the latest NPPES provider file (monthly release) and processes it.
- **Asynchronous API Fetching**: Optionally, fetches provider data from the NPI Registry API using asynchronous requests.
- **Supabase/Postgres Integration**: Upserts provider data into a custom `NPIRehabs` table, avoiding duplicates.
- **FastAPI Endpoints**:
  - Startup triggers the NPPES file ingestion.
  - Additional endpoint to run the async API-based ingestion.
- **Configurable via `.env`**: All required API keys and settings are loaded from environment variables (see `.env.example`).
- **Prompt Engineering Rules**: The `.cursor/rules/` directory contains `.mdc` files documenting prompt rules used in the project.

## Project Structure

```
npi_ingestion/
  api_client.py        # Async NPI Registry API client
  db.py                # Supabase/Postgres upsert logic
  ingestion_service.py # Orchestrates API-based ingestion
  config.py            # Loads environment/config
  startup.py           # Handles NPPES file download and ingestion
  utils.py             # Logging utilities
main.py                # CLI entrypoint
server.py              # FastAPI server (triggers workflows)
Dockerfile             # Docker build file
rehabs.make_your_own.sql # Example schema for custom table
.cursor/
  rules/
    ...                # Prompt engineering rules (.mdc files)
```

## Getting Started

### 1. Environment Setup

- Copy `.env.example` to `.env` and fill in your Supabase credentials and any other required keys:
  ```
  SUPABASE_URL=your_supabase_url
  SUPABASE_KEY=your_supabase_key
  ```

### 2. Database Setup

- Use the provided `rehabs.make_your_own.sql` to create the `NPIRehabs` table in your Supabase/Postgres instance:
  ```sql
  -- Example: psql -h <host> -U <user> -d <db> -f rehabs.make_your_own.sql
  ```

### 3. Running the Service

#### a. With Docker (Recommended)

```sh
docker build -t rehab-ingestion .
docker run --rm -p 8000:8000 --env-file .env rehab-ingestion
```

- The FastAPI server will start, automatically triggering the NPPES file ingestion on startup.

### 4. API Usage

- **Startup Ingestion**: On server start, the latest NPPES file is downloaded and ingested.
- **Async API Ingestion**: Trigger the async NPI Registry API workflow:
  ```sh
  curl -X POST http://localhost:8000/run-npi-workflow
  ```

## How It Works

- **NPPES File Ingestion**: Downloads the latest monthly NPPES provider file, filters for relevant taxonomy codes (substance abuse/mental health), deduplicates, and upserts into Supabase.
- **Async API Ingestion**: Optionally fetches providers from the NPI Registry API for more up-to-date or targeted data.
- **Database**: All providers are stored in the `NPIRehabs` table (see `rehabs.make_your_own.sql` for schema).

## Prompt Engineering Rules

- The `.cursor/rules/` directory contains `.mdc` files documenting the types of prompt rules used in this project. These are useful for understanding or extending the prompt engineering logic.

## For Developers

- Modular, typed Python code.
- Logging for observability.
- Easily extensible for new taxonomies, endpoints, or database targets.

## License

MIT

---

## Running Locally (venv, non-Docker)

```sh
python -m venv venv
source venv/bin/activate  # On Linux/Mac
venv\Scripts\activate     # On Windows
pip install -r prod-requirements.txt
uvicorn server:app --reload
```

- The server will start and trigger the NPPES ingestion.
- You can also trigger the async API workflow with:
  ```sh
  curl -X POST http://localhost:8000/run-npi-workflow
  ```
