from __future__ import annotations
import os
import httpx
from typing import Any, Dict, Optional

TIMEOUT = float(os.getenv("SIB_TIMEOUT", "30.0"))
BASE_URL = os.getenv("SIB_BASE_URL", "https://api.catalogo.biodiversidad.co")

DEFAULT_HEADERS = {
    "Accept": "application/json",
    "Accept-Charset": "utf-8",
    "User-Agent": "species-compiler/0.1 (+https://example.org)",
}

def _ascii_header(value: Optional[str]) -> str:
    if not value:
        return ""
    v = value.strip().replace("\r", "").replace("\n", "").replace("\t", " ")
    return v.encode("ascii", errors="replace").decode("ascii")

def _auth_headers() -> Dict[str, str]:
    token = (os.getenv("SIB_TOKEN") or "").strip()
    return {"Authorization": _ascii_header(token)} if token else {}

def _normalize_params(params: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in (params or {}).items():
        out[k] = "true" if isinstance(v, bool) and v else ("false" if isinstance(v, bool) else v)
    return out

def _client_headers(extra: Optional[Dict[str, str]] = None) -> Dict[str, str]:
    h = {**DEFAULT_HEADERS, **_auth_headers()}
    if extra:
        h.update(extra)
    return h

async def record_search(q: str, size: int = 50, count: bool = False, **extra_params) -> Dict[str, Any]:
    params = _normalize_params({"q": q, "size": size, "count": count, **extra_params})
    async with httpx.AsyncClient(timeout=TIMEOUT, headers=_client_headers()) as client:
        r = await client.get(f"{BASE_URL}/record_search/search", params=params)
        r.raise_for_status()
        return r.json()

async def advanced_search(**params) -> Dict[str, Any]:
    q = _normalize_params(params)
    async with httpx.AsyncClient(timeout=TIMEOUT, headers=_client_headers()) as client:
        r = await client.get(f"{BASE_URL}/record_search/advanced_search", params=q)
        r.raise_for_status()
        return r.json()
