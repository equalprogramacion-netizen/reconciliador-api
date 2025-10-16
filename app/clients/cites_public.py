# app/clients/cites_public.py (versión “suave”)
from __future__ import annotations
import httpx, re

BASE = "https://speciesplus.net"
def _rx(q: str): return re.compile(re.escape(q.strip()), re.I)

async def exists(q: str | None = None) -> bool:
    # Si no hay nombre, sólo checa conectividad
    if not q or not q.strip():
        try:
            async with httpx.AsyncClient(timeout=8.0) as c:
                r = await c.get(BASE)
                return r.status_code == 200
        except Exception:
            return False

    urls = [
        f"{BASE}/search?taxonomy_query={q.strip()}",
        f"{BASE}/search_by_taxon?taxon_query={q.strip()}",
    ]
    try:
        async with httpx.AsyncClient(timeout=10.0, headers={"User-Agent":"Equal-Reconciliador/1.0"}) as client:
            for u in urls:
                r = await client.get(u)
                if r.status_code == 200 and _rx(q).search(r.text):
                    return True
            # si no hubo match de texto pero el sitio responde OK, no falles
            return True
    except Exception:
        return False
