# app/clients/batrachia.py
from __future__ import annotations
import httpx

BASE = "https://www.batrachia.com"

# Firma unificada: acepta q opcional; ping a la home
async def exists(q: str | None = None) -> bool:
    try:
        async with httpx.AsyncClient(timeout=10.0) as cli:
            r = await cli.get(BASE)
            return r.status_code == 200
    except Exception:
        return False
