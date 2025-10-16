# app/clients/reptile_db.py
from __future__ import annotations
import httpx

BASE = "https://reptile-database.reptarium.cz"

# Firma unificada: acepta q opcional; aquí solo hacemos ping a la home
async def exists(q: str | None = None) -> bool:
    try:
        async with httpx.AsyncClient(timeout=10.0) as cli:
            r = await cli.get(BASE)
            return r.status_code == 200
    except Exception:
        return False
