# app/clients/itis.py
from __future__ import annotations
import json, httpx

BASE = "https://www.itis.gov/ITISWebService/jsonservice"

def _coerce_json(d):
    """
    ITIS a veces responde con {"_text": "<json>"}.
    Esto lo convierte en un dict normal para todos los endpoints.
    """
    if isinstance(d, dict) and isinstance(d.get("_text"), str):
        try:
            return json.loads(d["_text"])
        except Exception:
            return d
    return d

async def _get(url: str, params: dict):
    async with httpx.AsyncClient(timeout=12) as client:
        r = await client.get(url, params=params)
        try:
            data = r.json()
        except Exception:
            data = {"_text": r.text}
        return _coerce_json(data)

# --- Endpoints crudos ---
async def search_by_scientific_name(q: str):
    return await _get(f"{BASE}/searchByScientificName", {"srchKey": q})

async def get_full_record(tsn: str):
    return await _get(f"{BASE}/getFullRecordFromTSN", {"tsn": tsn})

async def get_full_hierarchy(tsn: str):
    return await _get(f"{BASE}/getFullHierarchyFromTSN", {"tsn": tsn})

async def get_hierarchy_up(tsn: str):
    return await _get(f"{BASE}/getHierarchyUpFromTSN", {"tsn": tsn})

# --- Normalización de rangos → tus nombres ---
_MAP = {
    "kingdom":"kingdom","phylum":"phylum","class":"class_name","order":"order_name",
    "superfamily":"superfamily","family":"family","subfamily":"subfamily",
    "tribe":"tribe","subtribe":"subtribe","genus":"genus","subgenus":"subgenus",
}

def _parse_nodes(nodes: list[dict]) -> dict:
    out = {}
    for n in nodes or []:
        r = (n.get("rankName") or n.get("rank") or "").strip().lower()
        t = (n.get("taxonName") or n.get("name") or "").strip()
        if r in _MAP and t:
            out[_MAP[r]] = t
    return out

def _pick_hierarchy_nodes(container) -> list:
    """
    Devuelve siempre una lista de nodos de jerarquía a partir de distintas
    formas que devuelve ITIS:
    - {"hierarchyList": {"hierarchy": [...]}}
    - {"hierarchyList": [...]}
    - {"hierarchy": [...]}
    - directamente una lista [...]
    - None / vacío
    """
    if not container:
        return []

    if isinstance(container, list):
        return container

    if isinstance(container, dict):
        hl = container.get("hierarchyList") or container.get("hierarchy")
        if isinstance(hl, list):
            return hl
        if isinstance(hl, dict):
            h = hl.get("hierarchy")
            return h if isinstance(h, list) else []
        # a veces el propio container ya es el que trae "rankName"/"taxonName"
        return []

    return []

async def lookup_taxonomy(name: str) -> dict:
    """
    Busca por nombre → elige un TSN → arma jerarquía con fallbacks.
    Devuelve: {"query", "raw", "full": {"tsn":...}, "parsed": {...}}
    """
    raw = await search_by_scientific_name(name)

    # Resolver TSN (varios formatos posibles)
    tsn = None
    for k in ("scientificNames","acceptedNameList","unacceptedNames"):
        v = raw.get(k)
        if isinstance(v, list) and v:
            c = v[0] or {}
            tsn = c.get("tsn") or c.get("acceptedTsn") or c.get("acceptedTSN")
            break
        if isinstance(v, dict):
            for kk in ("scientificNames","acceptedNames","unacceptedNames"):
                vv = v.get(kk)
                if isinstance(vv, list) and vv:
                    c = vv[0] or {}
                    tsn = c.get("tsn") or c.get("acceptedTsn") or c.get("acceptedTSN")
                    break
            if tsn:
                break

    full = {}
    parsed = {}

    if tsn:
        fr = await get_full_record(tsn)
        nodes = _pick_hierarchy_nodes((fr or {}).get("fullRecord") or fr)

        if not nodes:
            fh = await get_full_hierarchy(tsn)
            nodes = _pick_hierarchy_nodes(fh)

        if not nodes:
            up = await get_hierarchy_up(tsn)
            nodes = _pick_hierarchy_nodes(up)

        parsed = _parse_nodes(nodes if isinstance(nodes, list) else [])
        full = {"tsn": tsn}

    return {"query": name, "raw": raw, "full": full, "parsed": parsed}

__all__ = [
    "search_by_scientific_name", "get_full_record", "get_full_hierarchy",
    "get_hierarchy_up", "lookup_taxonomy"
]
