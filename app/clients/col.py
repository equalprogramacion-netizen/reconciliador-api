# app/clients/col.py
import httpx

BASE_COL = "https://api.catalogueoflife.org"
DATASET_KEY = "3LR"  # CoL dinámico (ChecklistBank). Si cambia, puedes parametrizarlo.

async def search_name(q: str) -> dict:
    """Busca en CoL por nombre canónico. Devuelve JSON del API."""
    q = (q or "").strip()
    if not q:
        return {}
    try:
        async with httpx.AsyncClient(timeout=12) as client:
            # 1) dataset-scoped (preferido)
            r = await client.get(f"{BASE_COL}/dataset/{DATASET_KEY}/nameusage/search",
                                 params={"q": q, "limit": 5})
            if r.status_code < 400:
                j = r.json() or {}
                # ChecklistBank/CoL suelen devolver {"result":[...], "total":N}
                if (j.get("result") or j.get("results")):
                    return j

            # 2) fallback global nameusage
            r2 = await client.get(f"{BASE_COL}/nameusage/search", params={"q": q, "limit": 5})
            if r2.status_code < 400:
                return r2.json() or {}

            # 3) fallback adicional ChecklistBank “name/search”
            r3 = await client.get("https://api.checklistbank.org/name/search", params={"q": q, "limit": 5})
            if r3.status_code < 400:
                return r3.json() or {}
    except Exception:
        pass
    return {}

async def _detail_for_usage(usage_id: str) -> dict:
    """Trae detalle/ clasificación para un usageId. Intenta dataset-scoped y global."""
    if not usage_id:
        return {}
    try:
        async with httpx.AsyncClient(timeout=12) as client:
            r = await client.get(f"{BASE_COL}/dataset/{DATASET_KEY}/nameusage/{usage_id}")
            if r.status_code < 400:
                return r.json() or {}
            r2 = await client.get(f"{BASE_COL}/nameusage/{usage_id}")
            if r2.status_code < 400:
                return r2.json() or {}
    except Exception:
        pass
    return {}
