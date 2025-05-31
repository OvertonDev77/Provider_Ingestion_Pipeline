import aiohttp
import asyncio
import time
from typing import List
from .models import Provider

NPI_API_URL = "https://npiregistry.cms.hhs.gov/api/"

class NPIClient:
    MAX_RESULTS_PER_REQUEST = 200
    MAX_SKIP = 1000
    MAX_TOTAL_RESULTS = 1200
    MAX_REQUESTS = 6
    RETRY_LIMIT = 3
    RETRY_DELAY = 2  # seconds
    RATE_LIMIT_DELAY = 1  # seconds between requests
    RUN_TIME_LIMIT = 4 * 60 * 60  # 4 hours in seconds
    WAIT_TIME = 2 * 60 * 60  # 2 hours in seconds

    def __init__(self, taxonomy_descriptions=None):
        if taxonomy_descriptions is None:
            taxonomy_descriptions = [
                "Substance Abuse Rehabilitation Facility",
                "Mental Health",
            ]
        self.taxonomy_descriptions = taxonomy_descriptions

    async def fetch_providers_by_taxonomy_description(self, description, session, limit=MAX_RESULTS_PER_REQUEST, skip=0):
        params = {
            "version": "2.1",
            "taxonomy_description": description,
            "limit": limit,
            "skip": skip,
            "pretty": "true",
        }
        retries = 0
        while retries < self.RETRY_LIMIT:
            try:
                async with session.get(NPI_API_URL, params=params) as resp:
                    resp.raise_for_status()
                    data = await resp.json()
                    return data.get("results", [])
            except Exception as e:
                print(f"Error: {e}. Retrying ({retries+1}/{self.RETRY_LIMIT})...")
                retries += 1
                await asyncio.sleep(self.RETRY_DELAY)
        print(f"Failed to fetch after {self.RETRY_LIMIT} retries.")
        return []

    async def fetch_all_providers(self, start_time=None) -> List[Provider]:
        all_results = []
        if start_time is None:
            start_time = time.time()
        async with aiohttp.ClientSession() as session:
            for desc in self.taxonomy_descriptions:
                skip = 0
                total_fetched = 0
                for req_num in range(self.MAX_REQUESTS):
                    if time.time() - start_time > self.RUN_TIME_LIMIT:
                        print("Reached 4 hour run time limit. Pausing for 2 hours...")
                        await asyncio.sleep(self.WAIT_TIME)
                        print("Resuming after wait.")
                        start_time = time.time()
                    results = await self.fetch_providers_by_taxonomy_description(desc, session, limit=self.MAX_RESULTS_PER_REQUEST, skip=skip)
                    if not results:
                        break
                    all_results.extend(results)
                    total_fetched += len(results)
                    print(f"Fetched {len(results)} results for '{desc}' (skip={skip})")
                    if len(results) < self.MAX_RESULTS_PER_REQUEST:
                        break
                    skip += self.MAX_RESULTS_PER_REQUEST
                    if skip > self.MAX_SKIP or total_fetched >= self.MAX_TOTAL_RESULTS:
                        break
                    await asyncio.sleep(self.RATE_LIMIT_DELAY)
        # Convert to Provider models
        return [Provider.parse_obj(self._parse_npi_result(r)) for r in all_results]

    def _parse_npi_result(self, result):
        basic = result.get("basic", {})
        npi_number = result.get("number")
        organization_name = basic.get("organization_name") or basic.get("name")
        last_updated = basic.get("last_updated")
        taxonomy = next((t for t in result.get("taxonomies", []) if t.get("primary")), None)
        if not taxonomy and result.get("taxonomies"):
            taxonomy = result["taxonomies"][0]
        taxonomy_code = taxonomy.get("code") if taxonomy else None
        taxonomy_desc = taxonomy.get("desc") if taxonomy else None
        address = next((a for a in result.get("addresses", []) if a.get("address_purpose") == "LOCATION"), None)
        if not address and result.get("addresses"):
            address = result["addresses"][0]
        addr_str = address.get("address_1", "")
        city = address.get("city", "")
        state = address.get("state", "")
        postal_code = address.get("postal_code", "")
        phone = address.get("telephone_number", "")
        official = basic.get("authorized_official_first_name", "")
        if official:
            official += " " + basic.get("authorized_official_last_name", "")
        else:
            official = None
        return {
            "npi_number": npi_number,
            "organization_name": organization_name,
            "address": addr_str,
            "city": city,
            "state": state,
            "postal_code": postal_code,
            "phone": phone,
            "taxonomy_code": taxonomy_code,
            "taxonomy_desc": taxonomy_desc,
            "authorized_official": official,
            "last_updated": last_updated
        } 