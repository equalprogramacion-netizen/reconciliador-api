from __future__ import annotations
import httpx

BASE = "https://api.gbif.org/v1"

async def species_match(name: str):
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.get(f"{BASE}/species/match", params={"name": name, "verbose": "true"})
        r.raise_for_status()
        return r.json()

async def species_get(key: int):
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.get(f"{BASE}/species/{key}")
        r.raise_for_status()
        return r.json()
