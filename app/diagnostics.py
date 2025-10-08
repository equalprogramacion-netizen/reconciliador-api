from __future__ import annotations

import os
import time
import httpx
import asyncio
from typing import Optional, Dict, Any

TIMEOUT = 10.0
DEFAULT_HEADERS = {
    "User-Agent": "species-compiler/0.1 (+https://example.org)",
    "Accept": "application/json",
    "Accept-Charset": "utf-8",
}

def _safe_text(obj: Any) -> str:
    try:
        s = str(obj)
    except UnicodeError:
        try:
            s = repr(obj)
        except Exception:
            s = "<unprintable>"
    try:
        return s.encode("utf-8", errors="replace").decode("utf-8")
    except Exception:
        return s.encode("ascii", errors="replace").decode("ascii")

def _ascii_header(value: str | None) -> str:
    if value is None:
        return ""
    v = str(value).strip().replace("\r", "").replace("\n", "").replace("\t", " ")
    return v.encode("ascii", errors="replace").decode("ascii")

def _sanitize_headers(h: Dict[str, str] | None) -> Dict[str, str]:
    return {str(k): _ascii_header(v) for k, v in (h or {}).items()}

async def _ping(url: str, params: Optional[Dict[str, Any]] = None, headers: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    t0 = time.perf_counter()
    merged_headers = dict(DEFAULT_HEADERS)
    if headers:
        merged_headers.update(headers)
    merged_headers = _sanitize_headers(merged_headers)

    try:
        async with httpx.AsyncClient(timeout=TIMEOUT, headers=merged_headers, follow_redirects=True) as client:
            r = await client.get(url, params=params or {})
            if not r.encoding:
                r.encoding = "utf-8"

            elapsed = round(time.perf_counter() - t0, 3)
            if r.status_code < 400:
                return {"status": "OK", "http": r.status_code, "tiempo_seg": elapsed, "via": "http"}
            body_excerpt = _safe_text(r.text)[:300] if r.text is not None else None
            return {"status": "FALLA", "http": r.status_code, "tiempo_seg": elapsed, "via": "http", "body": body_excerpt}
    except Exception as e:
        elapsed = round(time.perf_counter() - t0, 3)
        detalle = f"{e.__class__.__name__}: {_safe_text(getattr(e, 'args', [''])[0])}"
        return {"status": "FALLA", "detalle": detalle, "tiempo_seg": elapsed, "via": "http"}

# ---- HTTP checks
async def _check_gbif_http() -> Dict[str, Any]:
    return await _ping("https://api.gbif.org/v1/species/match", {"name": "Homo sapiens"})

async def _check_col_http() -> Dict[str, Any]:
    # usar dataset 3LR de CoL v2
    return await _ping("https://api.catalogueoflife.org/dataset/3LR/nameusage/search", {"q": "Homo sapiens", "limit": 1})

async def _check_worms_http() -> Dict[str, Any]:
    return await _ping("https://www.marinespecies.org/rest/AphiaRecordsByName/Actinia", {"like": "false", "marine_only": "false"})

async def _check_itis_http() -> Dict[str, Any]:
    r = await _ping("https://www.itis.gov/ITISWebService/jsonservice/searchByScientificName", {"srchKey": "Homo sapiens"})
    if r.get("status") == "FALLA" and "getaddrinfo" in _safe_text(r.get("detalle", "")):
        r = await _ping("https://itis.gov/ITISWebService/jsonservice/searchByScientificName", {"srchKey": "Homo sapiens"})
    return r

async def _check_iucn_http() -> Dict[str, Any]:
    token = os.getenv("IUCN_TOKEN")
    if not token:
        return {"status": "NO_TOKEN", "via": "http"}
    return await _ping("https://apiv3.iucnredlist.org/api/v3/species/Homo%20sapiens", {"token": token})

async def _check_sib_http() -> Dict[str, Any]:
    token = os.getenv("SIB_TOKEN")
    headers = {"Authorization": _ascii_header(token)} if token else None
    return await _ping("https://api.catalogo.biodiversidad.co/record_search/search", {"q": "Homo sapiens"}, headers=headers)

# ---- Via clientes + fallback
async def check_gbif() -> Dict[str, Any]:
    try:
        from .clients import gbif
        r = await gbif.species_match("Ateles belzebuth")
        return {"status": "OK" if bool(r) else "FALLA", "via": "client", "sample": "Ateles belzebuth"}
    except Exception as e:
        fb = await _check_gbif_http()
        fb.setdefault("error_client", _safe_text(e))
        return fb

async def check_col() -> Dict[str, Any]:
    try:
        from .clients import col
        r = await col.search_name("Ateles belzebuth")
        ok = isinstance(r, dict) and (r.get("total") or 0) >= 0
        return {"status": "OK" if ok else "FALLA", "via": "client"}
    except Exception as e:
        fb = await _check_col_http()
        fb.setdefault("error_client", _safe_text(e))
        return fb

async def check_worms() -> Dict[str, Any]:
    try:
        from .clients import worms
        r = await worms.search_name("Ateles belzebuth")
        ok = isinstance(r, list)
        return {"status": "OK" if ok else "FALLA", "via": "client"}
    except Exception as e:
        fb = await _check_worms_http()
        fb.setdefault("error_client", _safe_text(e))
        return fb

async def check_itis() -> Dict[str, Any]:
    try:
        from .clients import itis
        raw = await itis.search_by_scientific_name("Aedes aegypti")
        parsed = {}
        # intenta extraer 1 TSN y 1 jerarquía
        try:
            names = raw.get("scientificNames") or []
            tsn = names[0].get("tsn") if names else None
            if tsn:
                full = await itis.get_full_record_from_tsn(tsn)
                parsed = {"tsn": tsn, "has_hierarchy": bool(((full.get("fullRecord") or {}).get("hierarchyList") or {}).get("hierarchy"))}
        except Exception:
            parsed = {}

        return {"status": "OK", "via": "client", "raw_has_names": bool(raw.get("scientificNames")), "parsed": parsed}
    except Exception as e:
        fb = await _check_itis_http()
        fb.setdefault("error_client", _safe_text(e))
        return fb

async def check_iucn() -> Dict[str, Any]:
    try:
        from .clients import iucn
        r = await iucn.category_by_name("Ateles belzebuth")
        return {"status": "OK", "via": "client", "has_category": bool(r)}
    except Exception as e:
        fb = await _check_iucn_http()
        fb.setdefault("error_client", _safe_text(e))
        return fb

async def check_sib() -> Dict[str, Any]:
    try:
        from .clients import sib
        r = await sib.record_search("Ateles", size=1)
        ok = (isinstance(r, list) and len(r) > 0) or \
             (isinstance(r, dict) and any(isinstance(r.get(k), list) and r.get(k) for k in ("results","data","records","items")))
        return {"status": "OK" if ok else "FALLA", "via": "client"}
    except Exception as e:
        fb = await _check_sib_http()
        fb.setdefault("error_client", _safe_text(e))
        return fb

async def check_all() -> Dict[str, Dict[str, Any]]:
    results = await asyncio.gather(
        check_gbif(),
        check_col(),
        check_worms(),
        check_itis(),
        check_iucn(),
        check_sib(),
        return_exceptions=False,
    )
    keys = ["gbif", "col", "worms", "itis", "iucn", "sib"]
    return {k: v for k, v in zip(keys, results)}

if __name__ == "__main__":
    import json
    out = asyncio.run(check_all())
    print(json.dumps(out, ensure_ascii=False, indent=2))
