from __future__ import annotations
import os
import httpx

TOKEN = os.getenv("IUCN_TOKEN")
BASE = "https://apiv3.iucnredlist.org/api/v3"

async def category_by_name(scientific_name: str):
    if not TOKEN:
        return None
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.get(f"{BASE}/species/{scientific_name}", params={"token": TOKEN})
        if r.status_code == 404:
            return None
        r.raise_for_status()
        data = r.json() or {}
        items = data.get("result") or []
        return items[0]["category"] if items else None
