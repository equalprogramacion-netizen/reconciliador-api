# app/clients/sib.py
from __future__ import annotations
import os, asyncio
import httpx
from typing import Any, Dict, Optional, Tuple

# --- Config ---
TIMEOUT = float(os.getenv("SIB_TIMEOUT", "30.0"))
# Puedes ajustar el default si tu instancia usa otro host
BASE_URL = os.getenv("SIB_BASE_URL", "https://api.catalogo.biodiversidad.co")

# Alternativas si el host principal no resuelve (DNS/proxy)
FALLBACK_BASES = tuple(
    b for b in [
        os.getenv("SIB_BASE_URL_FALLBACK_1", "https://catalogo.biodiversidad.co"),
        os.getenv("SIB_BASE_URL_FALLBACK_2", "https://biodiversidad.co"),
    ] if b
)

DEFAULT_HEADERS = {
    "Accept": "application/json",
    "Accept-Charset": "utf-8",
    "Accept-Language": "es,es-CO;q=0.9,en;q=0.8",
    "User-Agent": "species-compiler/0.5 (+https://example.org)",
}

# --- Helpers headers / params ---
def _ascii_header(value: Optional[str]) -> str:
    if not value:
        return ""
    v = value.strip().replace("\r", "").replace("\n", "").replace("\t", " ")
    return v.encode("ascii", errors="replace").decode("ascii")

def _auth_headers() -> Dict[str, str]:
    token = (os.getenv("SIB_TOKEN") or "").strip()
    if not token:
        return {}
    # Asegura esquema Bearer si el usuario dejó solo el token
    if not token.lower().startswith("bearer "):
        token = f"Bearer {token}"
    return {"Authorization": _ascii_header(token)}

def _normalize_params(params: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in (params or {}).items():
        if isinstance(v, bool):
            out[k] = "true" if v else "false"
        else:
            out[k] = v
    return out

def _client_headers(extra: Optional[Dict[str, str]] = None) -> Dict[str, str]:
    h = {**DEFAULT_HEADERS, **_auth_headers()}
    if extra:
        h.update(extra)
    return h

# --- HTTP core con fallback de base_url y reintentos suaves ---
async def _do_get(path: str, params: Dict[str, Any]) -> httpx.Response:
    last_exc: Optional[Exception] = None
    # 1: base principal + 2: fallbacks
    for i, base in enumerate((BASE_URL, *FALLBACK_BASES)):
        try:
            async with httpx.AsyncClient(
                base_url=base,
                headers=_client_headers(),
                timeout=TIMEOUT,
                follow_redirects=True,
            ) as client:
                r = await client.get(path, params=params)
                # Si el server responde 406, reintenta una vez con Accept más permisivo
                if r.status_code == 406:
                    h2 = _client_headers({"Accept": "*/*"})
                    async with httpx.AsyncClient(
                        base_url=base,
                        headers=h2,
                        timeout=TIMEOUT,
                        follow_redirects=True,
                    ) as c2:
                        r2 = await c2.get(path, params=params)
                        r2.raise_for_status()
                        return r2
                r.raise_for_status()
                return r
        except Exception as e:
            last_exc = e
            # Si no es el último base, intenta el siguiente
            if i < len(FALLBACK_BASES):
                await asyncio.sleep(0.1)
                continue
            raise
    # Si llega aquí, re-eleva la última excepción
    assert last_exc is not None
    raise last_exc

# --- Endpoints públicos del cliente ---
async def record_search(q: str, size: int = 50, count: bool = False, **extra_params) -> Dict[str, Any]:
    params = _normalize_params({"q": q, "size": size, "count": count, **extra_params})
    r = await _do_get("/record_search/search", params)
    return r.json()

async def advanced_search(**params) -> Dict[str, Any]:
    q = _normalize_params(params)
    r = await _do_get("/record_search/advanced_search", q)
    return r.json()

# Presencia rápida (para “fuentes”)
async def exists(nombre: str) -> bool:
    try:
        j = await record_search(nombre, size=1)
        if isinstance(j, dict):
            for key in ("results", "data", "records", "items"):
                v = j.get(key)
                if isinstance(v, list) and len(v) > 0:
                    return True
        elif isinstance(j, list):
            return len(j) > 0
        return False
    except Exception:
        return False
