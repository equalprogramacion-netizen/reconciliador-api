from __future__ import annotations
import httpx

BASE = "https://www.marinespecies.org/rest"

async def search_name(q: str) -> list[dict]:
    async with httpx.AsyncClient(timeout=12) as client:
        r = await client.get(f"{BASE}/AphiaRecordsByName/{q}", params={"like": "false"})
        if r.status_code >= 400:
            return []
        arr = r.json() or []
        out = []
        for it in arr if isinstance(arr, list) else []:
            out.append({
                "scientificname": it.get("scientificname"),
                "authority": it.get("authority"),
                "kingdom": it.get("kingdom"),
                "phylum": it.get("phylum"),
                "class": it.get("class"),
                "order": it.get("order"),
                "superfamily": it.get("superfamily"),
                "family": it.get("family"),
                "subfamily": it.get("subfamily"),
                "tribe": it.get("tribe"),
                "subtribe": it.get("subtribe"),
                "genus": it.get("genus"),
                "subgenus": it.get("subgenus"),
                "valid_name": it.get("valid_name"),
                "AphiaName": it.get("valid_name"),
            })
        return out
