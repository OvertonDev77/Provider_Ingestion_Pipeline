# NPI Provider Ingestion Pipeline

This project is an asynchronous provider ingestion pipeline designed to fetch, parse, and store healthcare provider data from the NPI Registry API. It is intended to seed a rehab app prototype system with up-to-date provider information, focusing on substance abuse and mental health services.

## Features

- **Asynchronous API Requests**: Efficiently fetches provider data using aiohttp and asyncio.
- **Robust Data Parsing**: Uses Pydantic models for data validation and transformation.
- **Database Upsert**: Stores provider data in a Supabase/Postgres database, using upsert logic to avoid duplicates.
- **Modular Architecture**: Clean separation of API, database, service, and configuration layers.
- **Dockerized**: Easily runnable in any environment with Docker.
- **FastAPI Endpoint**: Optionally trigger ingestion via an HTTP API.

## Project Structure

```
npi_ingestion/
  models.py            # Pydantic Provider model
  api_client.py        # NPIClient class (async, typed, rate-limited)
  db.py                # SupabaseProviderRepo class (upsert logic)
  ingestion_service.py # IngestionService class (workflow orchestration)
  config.py            # Env/config loader
  utils.py             # Logging utility
main.py                # CLI entrypoint, uses the service
server.py              # FastAPI server, triggers workflow
Dockerfile             # Docker build file
```

## Usage

### 1. Environment Setup

- Copy your Supabase credentials into a `.env` file:
  ```
  SUPABASE_URL=your_supabase_url
  SUPABASE_KEY=your_supabase_key
  ```

### 2. Run the Service

#### a. Using Docker (Recommended)

- Build the Docker image:
  ```sh
  docker build -t npi-ingestion .
  ```
- Run the FastAPI server:
  ```sh
  docker run --rm -p 8000:8000 --env-file .env npi-ingestion uvicorn server:app --host 0.0.0.0 --port 8000
  ```
- Trigger the workflow via HTTP:
  ```sh
  curl -X POST http://localhost:8000/run-npi-workflow
  ```

#### b. Using Local venv Environment

- Activate your virtual environment:
  ```sh
  source venv/bin/activate  # On Linux/Mac
  venv\Scripts\activate    # On Windows
  ```
- Install dependencies:
  ```sh
  pip install -r requirements.txt
  ```
- Run the pipeline directly:
  ```sh
  python main.py
  ```
- Or run the FastAPI server locally:
  ```sh
  uvicorn server:app --reload
  ```
- Then trigger the workflow via HTTP:
  ```sh
  curl -X POST http://localhost:8000/run-npi-workflow
  ```

## How It Works

- The pipeline fetches provider data from the NPI Registry API, focusing on substance abuse and mental health taxonomy descriptions.
- Data is parsed and validated using Pydantic models.
- Providers are upserted into the `NPIRehabs` table in your Supabase/Postgres database.
- The workflow can be triggered via CLI or HTTP (FastAPI).

## For Developers

- All code is modular and typed.
- Logging is provided for observability.
- Easily extensible for new taxonomies, endpoints, or database targets.

## License

MIT
