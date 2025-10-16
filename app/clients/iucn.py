# app/clients/iucn.py
from __future__ import annotations
import os
import httpx
from functools import lru_cache
from urllib.parse import quote

TOKEN = os.getenv("IUCN_TOKEN", "").strip()
BASE = "https://apiv3.iucnredlist.org/api/v3"

def _norm_name(s: str) -> str:
    s = (s or "").strip()
    parts = s.split()
    if len(parts) >= 2:
        return f"{parts[0].capitalize()} {' '.join(p.lower() for p in parts[1:2])}"
    return s

@lru_cache(maxsize=2048)
def _cache_key(name: str) -> str:
    return _norm_name(name)

async def _http_get_json(url: str, params: dict | None = None, timeout: float = 12.0) -> dict:
    async with httpx.AsyncClient(timeout=timeout) as client:
        r = await client.get(url, params=params or {})
    return r.json() if r.status_code < 400 else {}

# ---------- 1) API oficial IUCN (si hay token) ----------
async def _category_iucn_api(scientific_name: str) -> str | None:
    if not TOKEN:
        return None
    q = _norm_name(scientific_name)
    url = f"{BASE}/species/{quote(q)}"
    async with httpx.AsyncClient(timeout=12.0) as client:
        r = await client.get(url, params={"token": TOKEN})
        if r.status_code == 404:
            return None
        if r.status_code >= 400:
            return None
        data = r.json() or {}
        items = data.get("result") or []
        cat = (items[0] or {}).get("category") if items else None
        if cat:
            return str(cat).strip()
    # Fallbacks de la API oficial
    async with httpx.AsyncClient(timeout=12.0) as client:
        r2 = await client.get(f"{BASE}/species/match", params={"token": TOKEN, "name": q})
        if r2.status_code < 400:
            cat2 = (r2.json() or {}).get("category")
            if cat2:
                return str(cat2).strip()
        r3 = await client.get(f"{BASE}/species/page/1", params={"token": TOKEN, "name": q})
        if r3.status_code < 400:
            items = (r3.json() or {}).get("result") or []
            if items:
                cat3 = (items[0] or {}).get("category")
                if cat3:
                    return str(cat3).strip()
    return None

# ---------- 2) Fallback público: Wikidata SPARQL (sin token) ----------
# ---------- Mapeo de etiquetas Wikidata → códigos UICN (case-insensitive) ----------
_IUCN_MAP_RAW = {
    "Critically endangered": "CR",
    "Endangered": "EN",
    "Vulnerable": "VU",
    "Near threatened": "NT",
    "Least concern": "LC",
    "Data Deficient": "DD",
    "Not Evaluated": "NE",
    "Not evaluated": "NE",
    "Extinct": "EX",
    "Extinct in the wild": "EW",
}
# normaliza a minúsculas para comparar sin sensibilidad a mayúsculas/acentos sencillos
_IUCN_MAP = {k.lower(): v for k, v in _IUCN_MAP_RAW.items()}

async def _category_wikidata(scientific_name: str) -> str | None:
    q = _norm_name(scientific_name)
    sparql = f"""
SELECT ?status ?statusLabel WHERE {{
  ?taxon wdt:P225 "{q}" .
  OPTIONAL {{ ?taxon wdt:P141 ?status. }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
}}
LIMIT 1
""".strip()

    headers = {
        "Accept": "application/sparql-results+json",
        "User-Agent": "reconciliador/1.0 (+https://example.org/contact)"
    }
    async with httpx.AsyncClient(timeout=12.0, headers=headers) as client:
        r = await client.get(
            "https://query.wikidata.org/sparql",
            params={"query": sparql, "format": "json"}
        )
        if r.status_code >= 400:
            return None
        data = r.json() or {}
        bindings = ((data.get("results") or {}).get("bindings")) or []
        if not bindings:
            return None

        # 1) intentar por etiqueta (statusLabel)
        lab = (((bindings[0] or {}).get("statusLabel") or {}).get("value")) or ""
        lab_norm = lab.strip().lower()
        if lab_norm in _IUCN_MAP:
            return _IUCN_MAP[lab_norm]

        # 2) si no hay etiqueta o no mapea, intenta si ya viene como sigla (raro, pero por si acaso)
        if lab and lab.strip().upper() in {"CR","EN","VU","NT","LC","DD","NE","EX","EW"}:
            return lab.strip().upper()

        return None

# ---------- API pública del cliente ----------
async def category_by_name(scientific_name: str) -> str | None:
    # intenta IUCN oficial si hay token, si no usa Wikidata
    cat = await _category_iucn_api(scientific_name)
    if cat:
        return cat
    return await _category_wikidata(scientific_name)

# Alias que usa reconcile.py
async def category_for_name(scientific_name: str) -> str | None:
    return await category_by_name(scientific_name)

# Utilidades opcionales para /debug/conectores
async def exists(name: str) -> bool:
    try:
        return (await category_for_name(name)) is not None
    except Exception:
        return False

async def detail(name: str) -> dict:
    cat = await category_for_name(name)
    return {"name": _norm_name(name), "iucn_category": cat} if cat else {}
