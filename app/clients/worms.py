# app/clients/worms.py
from __future__ import annotations
import httpx
from typing import Any, Dict, List, Optional

BASE = "https://www.marinespecies.org"
REST = "https://www.marinespecies.org/rest"

# ----------------------------- Utils HTTP -----------------------------
async def _get(cli: httpx.AsyncClient, url: str, params: dict | None = None):
    r = await cli.get(url, params=params or {})
    if r.status_code >= 400:
        return None
    try:
        return r.json()
    except Exception:
        return None

async def ping() -> bool:
    try:
        async with httpx.AsyncClient(timeout=10.0) as cli:
            r = await cli.get(f"{BASE}/")
            return r.status_code == 200
    except Exception:
        return False

# ----------------------------- Enriquecedores -----------------------------
# NUEVO: trae la jerarquía completa por AphiaID (árbol jerárquico)
async def get_classification(aphia_id: int | str) -> dict:
    if not aphia_id:
        return {}
    try:
        async with httpx.AsyncClient(timeout=12.0) as cli:
            url = f"{REST}/AphiaClassificationByAphiaID/{aphia_id}"
            r = await cli.get(url)
            r.raise_for_status()
            return r.json() or {}
    except Exception:
        return {}

async def _enrich_with_record(cli: httpx.AsyncClient, aphia_id: int | str) -> dict:
    """
    Trae la ficha del taxón y extrae jerarquía básica + campos útiles.
    Endpoint: /AphiaRecordByAphiaID/{id}
    """
    rec = await _get(cli, f"{REST}/AphiaRecordByAphiaID/{aphia_id}")
    if not isinstance(rec, dict):
        return {}

    out: Dict[str, Any] = {}
    # Copiamos campos si existen
    for k in ("kingdom","phylum","class","order","family","genus","status","rank","authority"):
        v = rec.get(k)
        if v:
            out[k] = v
    # Científico/IDs válidos
    if rec.get("scientificname"): out["scientificName"] = rec["scientificname"]
    if rec.get("AphiaID"):        out["AphiaID"] = rec["AphiaID"]
    if rec.get("valid_AphiaID"):  out["valid_AphiaID"] = rec["valid_AphiaID"]
    if rec.get("valid_name"):     out["valid_name"] = rec["valid_name"]

    return out

# OPCIONAL: helper que busca y devuelve el mejor registro + clasificación
async def fetch_best(q: str) -> dict:
    """
    Busca por nombre y devuelve el mejor candidato enriquecido con jerarquía,
    usando tanto AphiaRecord (campos directos) como AphiaClassification (árbol).
    """
    if not q or not q.strip():
        return {}
    try:
        async with httpx.AsyncClient(timeout=15.0) as cli:
            url = f"{REST}/AphiaRecordsByName/{q.strip()}"
            data = await _get(cli, url, params={"like": "true", "marine_only": "false", "offset": 1})
            if not isinstance(data, list) or not data:
                return {}
            top = dict(data[0] or {})

            # Enriquecer con ficha básica
            aid = top.get("valid_AphiaID") or top.get("AphiaID")
            if aid:
                extra = await _enrich_with_record(cli, aid)
                if isinstance(extra, dict):
                    top.update({k: v for k, v in extra.items() if v})

                # Enriquecer con clasificación (árbol) para completar superfamily/order/etc.
                clas = await get_classification(aid)
                if isinstance(clas, dict) and clas:
                    top.update({
                        "kingdom":     (clas.get("kingdom") or {}).get("scientificname") or top.get("kingdom"),
                        "phylum":      (clas.get("phylum") or {}).get("scientificname") or top.get("phylum"),
                        "class":       (clas.get("class") or {}).get("scientificname") or top.get("class"),
                        "order":       (clas.get("order") or {}).get("scientificname") or top.get("order"),
                        "superfamily": (clas.get("superfamily") or {}).get("scientificname") or top.get("superfamily"),
                        "family":      (clas.get("family") or {}).get("scientificname") or top.get("family"),
                        "genus":       (clas.get("genus") or {}).get("scientificname") or top.get("genus"),
                    })
            return top
    except Exception:
        return {}

# ----------------------------- API esperada por reconcile -----------------------------
async def exists(q: str | None = None) -> bool:
    if q and q.strip():
        try:
            async with httpx.AsyncClient(timeout=12.0) as cli:
                url = f"{REST}/AphiaRecordsByName/{q.strip()}"
                data = await _get(cli, url, params={"like": "true", "marine_only": "false", "offset": 1})
                return bool(data)
        except Exception:
            return False
    return await ping()

async def search_name(q: str) -> List[dict]:
    """
    Devuelve una LISTA de candidatos WoRMS enriquecidos con jerarquía.
    reconcile._worms_best() toma el primero.
    """
    if not q or not q.strip():
        return []
    try:
        async with httpx.AsyncClient(timeout=15.0) as cli:
            url = f"{REST}/AphiaRecordsByName/{q.strip()}"
            data = await _get(cli, url, params={"like": "true", "marine_only": "false", "offset": 1})
            if not isinstance(data, list) or not data:
                return []

            out: List[dict] = []
            # Enriquecemos los primeros (p.ej., 3) para reducir latencia total
            for it in data[:3]:
                base_item: Dict[str, Any] = {
                    "scientificName": it.get("scientificname"),
                    "authority": it.get("authority"),
                    "rank": it.get("rank"),
                    "status": it.get("status"),
                    "AphiaID": it.get("AphiaID"),
                    "valid_AphiaID": it.get("valid_AphiaID"),
                    "valid_name": it.get("valid_name"),
                }
                aid = it.get("valid_AphiaID") or it.get("AphiaID")
                if aid:
                    # Ficha básica
                    extra = await _enrich_with_record(cli, aid)
                    if isinstance(extra, dict):
                        base_item.update({k: v for k, v in extra.items() if v})

                    # Clasificación completa (añade order/superfamily/etc. si faltaban)
                    clas = await get_classification(aid)
                    if isinstance(clas, dict) and clas:
                        base_item.update({
                            "kingdom":     (clas.get("kingdom") or {}).get("scientificname") or base_item.get("kingdom"),
                            "phylum":      (clas.get("phylum") or {}).get("scientificname") or base_item.get("phylum"),
                            "class":       (clas.get("class") or {}).get("scientificname") or base_item.get("class"),
                            "order":       (clas.get("order") or {}).get("scientificname") or base_item.get("order"),
                            "superfamily": (clas.get("superfamily") or {}).get("scientificname") or base_item.get("superfamily"),
                            "family":      (clas.get("family") or {}).get("scientificname") or base_item.get("family"),
                            "genus":       (clas.get("genus") or {}).get("scientificname") or base_item.get("genus"),
                        })

                out.append(base_item)

            return out
    except Exception:
        return []
