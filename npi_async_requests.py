import aiohttp
import asyncio
import time

NPI_API_URL = "https://npiregistry.cms.hhs.gov/api/"

# Example taxonomy descriptions for substance abuse and mental health
TAXONOMY_DESCRIPTIONS = [
    "Substance Abuse Rehabilitation Facility",
    "Mental Health",
]

MAX_RESULTS_PER_REQUEST = 200
MAX_SKIP = 1000
MAX_TOTAL_RESULTS = 1200
MAX_REQUESTS = 6
RETRY_LIMIT = 3
RETRY_DELAY = 2  # seconds
RATE_LIMIT_DELAY = 1  # seconds between requests
RUN_TIME_LIMIT = 4 * 60 * 60  # 4 hours in seconds
WAIT_TIME = 2 * 60 * 60  # 2 hours in seconds

async def fetch_providers_by_taxonomy_description(description, session, limit=MAX_RESULTS_PER_REQUEST, skip=0):
    params = {
        "version": "2.1",
        "taxonomy_description": description,
        "limit": limit,
        "skip": skip,
        "pretty": "true",
    }
    retries = 0
    while retries < RETRY_LIMIT:
        try:
            async with session.get(NPI_API_URL, params=params) as resp:
                resp.raise_for_status()
                data = await resp.json()
                return data.get("results", [])
        except Exception as e:
            print(f"Error: {e}. Retrying ({retries+1}/{RETRY_LIMIT})...")
            retries += 1
            await asyncio.sleep(RETRY_DELAY)
    print(f"Failed to fetch after {RETRY_LIMIT} retries.")
    return []

async def fetch_all_providers(descriptions=TAXONOMY_DESCRIPTIONS, start_time=None):
    all_results = []
    if start_time is None:
        start_time = time.time()
    async with aiohttp.ClientSession() as session:
        for desc in descriptions:
            skip = 0
            total_fetched = 0
            for req_num in range(MAX_REQUESTS):
                # Check if run time limit is reached
                if time.time() - start_time > RUN_TIME_LIMIT:
                    print("Reached 4 hour run time limit. Pausing for 2 hours...")
                    await asyncio.sleep(WAIT_TIME)
                    print("Resuming after wait.")
                    start_time = time.time()  # Reset start time after wait
                results = await fetch_providers_by_taxonomy_description(desc, session, limit=MAX_RESULTS_PER_REQUEST, skip=skip)
                if not results:
                    break
                all_results.extend(results)
                total_fetched += len(results)
                print(f"Fetched {len(results)} results for '{desc}' (skip={skip})")
                if len(results) < MAX_RESULTS_PER_REQUEST:
                    break  # No more results
                skip += MAX_RESULTS_PER_REQUEST
                if skip > MAX_SKIP or total_fetched >= MAX_TOTAL_RESULTS:
                    break
                await asyncio.sleep(RATE_LIMIT_DELAY)
    return all_results

# For quick testing
if __name__ == "__main__": 
    print("Fetching providers from NPI API...")
    # async def main():
    #     results = await fetch_all_providers(["Substance Abuse Rehabilitation Facility"])
    #     print(f"Found {len(results)} providers:")
    #     for provider in results[:10]:  # Print only first 10 for brevity
    #         print(provider.get("basic", {}).get("organization_name") or provider.get("basic", {}).get("name"))
    # asyncio.run(main()) 