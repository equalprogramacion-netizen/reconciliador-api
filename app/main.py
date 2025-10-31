from __future__ import annotations

# ------------------------------------------------------------
# Importaciones estándar y de terceros
# ------------------------------------------------------------
import os, io, json, asyncio, uvicorn, base64, hmac, time
from typing import List, Optional, Dict, Any
from time import perf_counter
from urllib.parse import urlparse

import pandas as pd
import httpx
from fastapi import (
    FastAPI, Depends, Query, HTTPException,
    UploadFile, File, Form, Body, Security, Request
)
from fastapi.responses import RedirectResponse, StreamingResponse, HTMLResponse, JSONResponse, PlainTextResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security.api_key import APIKeyHeader
from sqlalchemy.orm import Session
from sqlalchemy import select
from starlette.middleware.base import BaseHTTPMiddleware
from itsdangerous import TimestampSigner, BadSignature, SignatureExpired
from fastapi.openapi.docs import get_swagger_ui_html

# ------------------------------------------------------------
# Importaciones internas del proyecto
# ------------------------------------------------------------
from .db import SessionLocal, engine
from .models import Base, Taxon, Synonym
from .schemas import TaxonOut, TaxonESOut
from .services.reconcile import (
    reconcile_name,
    obtener_epiteto_especifico,
    buscar_en_fuentes_externas,
    normaliza_nombre,
    _taxonomy_from_col,
)
from .diagnostics import (
    check_gbif, check_col, check_worms, check_itis, check_iucn, check_sib,
    check_reptile_db, check_batrachia, check_aco_birds, check_sib_mammals_2024, check_cites_public
)
from .clients import itis as itis_client
from .clients import col as col_client

# --- ingestors (ACO aves y Mamíferos 2024 SIB)
from .ingestors import aco_birds as ing_aco
from .ingestors import sib_mammals_2024 as ing_mam

# --- NUEVO: router de lotes y cierre del cliente HTTP ---
from .routes.reconcile_batch import router as reconcile_batch_router
from .services.reconcile import close_http_client

# ------------------------------------------------------------
# Metadatos de tags para la documentación
# ------------------------------------------------------------
TAGS_METADATA = [
    {"name": "Salud", "description": "Verificación del servicio y conectores."},
    {"name": "Taxonomía", "description": "Reconciliación de nombres científicos y enriquecimiento desde varias fuentes."},
    {"name": "Archivos", "description": "Subir CSV/Excel y descargar resultados reconciliados."},
    {"name": "UI", "description": "Interfaz web ligera para búsqueda."},
]

# ------------------------------------------------------------
# Instancia FastAPI (usamos /docs propio para tematizar)
# ------------------------------------------------------------
app = FastAPI(
    title="Compilador de Especies (Colombia primero)",
    description="API en español para reconciliar nombres científicos con GBIF e integrar IUCN, Catalogue of Life, WoRMS, ITIS y SIB Colombia.",
    version="0.5.0",
    openapi_tags=TAGS_METADATA,
    docs_url=None,
    redoc_url=None,
    openapi_url="/openapi.json",
)

# ------------------------------------------------------------
# CORS
# ------------------------------------------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ------------------------------------------------------------
# ====== Puerta SSO desde el Hub (ENV) + Tema ======
# ------------------------------------------------------------
IS_PROD = os.getenv("ENV", "development").lower() == "production"
GATE_BYPASS = os.getenv("GATE_BYPASS", "0") == "1"

THEME = {
    "bg":       os.getenv("HUB_COLOR_BG",     "#0a0f1c"),
    "card":     os.getenv("HUB_COLOR_CARD",   "#0f172a"),
    "ink":      os.getenv("HUB_COLOR_INK",    "#f3f6ff"),
    "muted":    os.getenv("HUB_COLOR_MUTED",  "#a9b6d6"),
    "acc":      os.getenv("HUB_COLOR_ACCENT", "#22c55e"),  # verde primario
    "border":   os.getenv("HUB_COLOR_BORDER", "#25345b"),
    "hover":    os.getenv("HUB_COLOR_HOVER",  "#142348"),
    "sel":      os.getenv("HUB_COLOR_SEL",    "#1e3a66"),
    "ok":       os.getenv("HUB_COLOR_OK",     "#22c55e"),  # mismo verde
    "chip":     os.getenv("HUB_COLOR_CHIP",   "#0c1633"),
}

# ------------------------------------------------------------
# ====== Puerta SSO desde el Hub (señales) ======
# ------------------------------------------------------------
GATEWAY_SHARED_SECRET       = os.getenv("GATEWAY_SHARED_SECRET", "cambia-esto-por-un-secreto-largo-y-unico")
GATEWAY_SHARED_SECRET_PREV  = os.getenv("GATEWAY_SHARED_SECRET_PREV", "")  # rotación opcional
GATE_AUD = os.getenv("GATE_AUD", "reconciliador")  # audiencia esperada
HUB_HOME = os.getenv("HUB_HOME", "http://127.0.0.1:8000/choose")  # a dónde enviar si no hay sesión aquí

# Cookie local de este servicio (no es la del Hub)
SVC_SESSION_COOKIE = os.getenv("SVC_SESSION_COOKIE", "svc_reconciliador")
SVC_SESSION_TTL    = int(os.getenv("SVC_SESSION_TTL", "1800"))  # 30 min

# Rutas anónimas permitidas (base)
ANON_PATHS = set((
    "/health", "/healthz", "/favicon.ico", "/robots.txt",
    "/plantilla.xlsx",
    "/static", "/assets",
    "/openapi.json",
))

# Si bypass está activo, abre también raíz, docs y endpoints útiles para pruebas
if GATE_BYPASS:
    ANON_PATHS.update((
        "/", "/docs", "/openapi.json", "/redoc",
        "/debug/config", "/debug/conectores",
        "/ui/buscador",
        "/reconciliar", "/reconciliar/detalle", "/reconciliar/bulk",
        "/suggest", "/fuentes/check",
        "/admin/ingest/aco", "/admin/ingest/mammals",
    ))

# ------------------------------------------------------------
# Helpers de verificación de st + cookie de sesión local
# ------------------------------------------------------------
def _b64url_pad(s: str) -> bytes:
    s += "=" * ((4 - len(s) % 4) % 4)
    return s.encode("ascii")

def _b64url_decode_to_json(b64: str) -> dict:
    raw = base64.urlsafe_b64decode(_b64url_pad(b64))
    return json.loads(raw.decode("utf-8"))

def _compare_digest(a: str, b: str) -> bool:
    try:
        return hmac.compare_digest(a, b)
    except Exception:
        return a == b

def _sign_st_payload(payload_b64: str, secret: str) -> str:
    sig = hmac.new(secret.encode("utf-8"), payload_b64.encode("ascii"), digestmod="sha256").digest()
    return base64.urlsafe_b64encode(sig).rstrip(b"=").decode("ascii")

def _verify_st(token: str) -> dict | None:
    """
    st = <base64url(payload)>.<base64url(signature)>
    payload = {"sub","aud","iat","exp","rid","iss"}
    - Valida HMAC con GATEWAY_SHARED_SECRET (o PREV)
    - Revisa exp y aud
    Devuelve el payload dict si es válido; si no, None
    """
    if not token or "." not in token:
        return None
    parts = token.split(".")
    if len(parts) != 2:
        return None
    payload_b64, sig_b64 = parts[0], parts[1]

    good_sig = _sign_st_payload(payload_b64, GATEWAY_SHARED_SECRET)
    if not _compare_digest(sig_b64, good_sig) and GATEWAY_SHARED_SECRET_PREV:
        good_sig_prev = _sign_st_payload(payload_b64, GATEWAY_SHARED_SECRET_PREV)
        if not _compare_digest(sig_b64, good_sig_prev):
            return None
    elif not _compare_digest(sig_b64, good_sig):
        return None

    try:
        payload = _b64url_decode_to_json(payload_b64)
    except Exception:
        return None

    now = int(time.time())
    if int(payload.get("exp", 0)) < now:
        return None
    if payload.get("aud") != GATE_AUD:
        return None
    return payload

_svc_signer = TimestampSigner(GATEWAY_SHARED_SECRET)

def _set_svc_session(resp, email: str):
    token = _svc_signer.sign(email.encode("utf-8")).decode("utf-8")
    # Si estás detrás de HTTPS, cambia secure=True y considera samesite="strict"
    resp.set_cookie(
        SVC_SESSION_COOKIE, token,
        max_age=SVC_SESSION_TTL,
        httponly=True, samesite=("strict" if IS_PROD else "lax"),
        secure=IS_PROD, path="/"
    )

def _get_svc_email(request: Request) -> str | None:
    tok = request.cookies.get(SVC_SESSION_COOKIE)
    if not tok:
        return None
    try:
        raw = _svc_signer.unsign(tok, max_age=SVC_SESSION_TTL)
        return raw.decode("utf-8")
    except (BadSignature, SignatureExpired):
        return None

def _clear_svc_session(resp):
    resp.delete_cookie(SVC_SESSION_COOKIE, path="/")

# ------------------------------------------------------------
# Middleware GateGuard
# ------------------------------------------------------------
class GateGuardMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        path = (request.url.path or "").rstrip("/") or "/"

        if request.method.upper() == "OPTIONS":
            return await call_next(request)

        # Sirve estáticos sin gate
        if path.startswith("/static") or path.startswith("/assets"):
            return await call_next(request)

        # Anónimos permitidos
        if path in ANON_PATHS:
            return await call_next(request)

        # Si ya hay cookie de sesión local, OK
        email = _get_svc_email(request)
        if email:
            return await call_next(request)

        # Intenta validar st por query o Authorization: Bearer
        st = request.query_params.get("st")
        if not st:
            auth = request.headers.get("Authorization", "")
            if auth.lower().startswith("bearer "):
                st = auth.split(" ", 1)[1].strip()
        payload = _verify_st(st) if st else None

        if payload:
            resp = await call_next(request)
            _set_svc_session(resp, payload.get("sub", ""))
            return resp

        # 401 tematizado (placeholders + replace, sin f-strings)
        html_401 = """
        <!doctype html><html><head><meta charset="utf-8"/>
        <title>401 — Autenticación requerida</title></head>
        <body style="font-family:system-ui;background:__BG__;color:__INK__;display:grid;place-items:center;height:100vh;margin:0">
          <div style="max-width:680px;background:__CARD__;border:1px solid __BORDER__;padding:24px;border-radius:14px">
            <h2 style="margin:0 0 8px">Acceso restringido</h2>
            <p style="margin:0 0 14px;opacity:.8">Para usar el Reconciliador debes entrar desde el Hub.</p>
            <a href="__HUB_HOME__" style="display:inline-block;background:__OK__;color:#08150c;padding:10px 16px;border-radius:10px;font-weight:800;text-decoration:none">Ir al Hub</a>
          </div>
        </body></html>
        """
        themed = (
            html_401
            .replace("__BG__", THEME["bg"])
            .replace("__INK__", THEME["ink"])
            .replace("__CARD__", THEME["card"])
            .replace("__BORDER__", THEME["border"])
            .replace("__OK__", THEME["ok"])
            .replace("__HUB_HOME__", HUB_HOME)
        )
        return HTMLResponse(themed, status_code=401)

if not GATE_BYPASS:
    app.add_middleware(GateGuardMiddleware)

# ------------------------------------------------------------
# Inicializa tablas si no existen
# ------------------------------------------------------------
Base.metadata.create_all(bind=engine)

# ------------------------------------------------------------
# Seguridad por API Key (opcional)
# ------------------------------------------------------------
API_KEY = os.getenv("API_KEY")
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

async def require_key(api_key: str = Security(api_key_header)):
    if not API_KEY:
        return True
    if api_key == API_KEY:
        return True
    raise HTTPException(status_code=401, detail="API key inválida")

# ------------------------------------------------------------
# Sesión de base de datos (scoped por request)
# ------------------------------------------------------------
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# ------------------------------------------------------------
# FRONTEND_URL
# ------------------------------------------------------------
FRONTEND_URL = os.getenv("FRONTEND_URL")

# ------------------------------------------------------------
# Concurrencia controlada para lotes
# ------------------------------------------------------------
# (1) Cambio solicitado: subir concurrencia por defecto de 8 -> 12
_SEM_BULK = asyncio.Semaphore(int(os.getenv("BULK_CONCURRENCY", "12")))

async def _resolve_one(db: Session, n: str, modo: str) -> dict:
    """Ejecuta reconcile_name con semáforo y arma la fila de salida."""
    nombre = (n or "").strip()
    if not nombre:
        return {}
    n_norm = normaliza_nombre(nombre)
    async with _SEM_BULK:
        t = await reconcile_name(db, n_norm)
    if modo == "db":
        row = {col.name: getattr(t, col.name) for col in Taxon.__table__.columns}
        row["nombre_original"] = nombre
        row["_taxon_id"] = t.id
    else:
        epiteto = obtener_epiteto_especifico(t.canonical_name or t.scientific_name, t.rank)
        row = {
            "_taxon_id": t.id,
            "nombre_original": nombre,
            "nombre_cientifico": t.scientific_name,
            "epiteto_especifico": epiteto,
            "estado": t.status,
            "clave_gbif": t.gbif_key,
            "clave_aceptada_gbif": t.accepted_gbif_key,
            "rango": t.rank,
            "categoria_iucn": t.iucn_category,
            "reino": t.kingdom, "filo": t.phylum, "clase": t.class_name, "orden": t.order_name,
            "superfamilia": t.superfamily, "familia": t.family, "subfamilia": t.subfamily,
            "tribu": t.tribe, "subtribu": t.subtribe, "genero": t.genus, "subgenero": t.subgenus,
            "fuentes": t.sources_csv,
        }
    return row

# ---------------- Raíz ----------------
@app.get("/", include_in_schema=False)
async def root():
    if FRONTEND_URL:
        return RedirectResponse(FRONTEND_URL)

    html = """
    <!doctype html>
    <html lang="es">
      <head>
        <meta charset="utf-8">
        <title>Compilador de Especies</title>
        <meta name="viewport" content="width=device-width, initial-scale=1"/>
      </head>
      <body style="margin:0; font-family: system-ui, -apple-system, Segoe UI, Roboto, Ubuntu; background:__BG__; color:__INK__; min-height:100vh; display:flex; align-items:center; justify-content:center; text-align:center">
        <div>
          <h1 style="margin-bottom:.5rem">Compilador de Especies</h1>
          <p style="opacity:.8">Configura la variable de entorno <code>FRONTEND_URL</code> para redirigir a la UI de usuarios.</p>
          <p><a href="/docs" style="color:__ACC__; text-decoration:underline">Ver documentación de la API</a></p>
        </div>
      </body>
    </html>
    """
    themed = (
        html
        .replace("__BG__", THEME["bg"])
        .replace("__INK__", THEME["ink"])
        .replace("__ACC__", THEME["acc"])
    )
    return HTMLResponse(themed)

# ---------------- Salud / utilidades mínimas ----------------
@app.get("/health", tags=["Salud"])
async def health():
    return {"status": "ok"}

@app.get("/healthz", include_in_schema=False)
async def healthz():
    return PlainTextResponse("ok")

@app.get("/robots.txt", include_in_schema=False)
async def robots():
    return PlainTextResponse("User-agent: *\nDisallow: /", media_type="text/plain")

@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    return PlainTextResponse("", status_code=204)

# ---------------- Debug (con API key) ----------------
@app.get("/debug/config", tags=["Salud"], dependencies=[Depends(require_key)])
async def debug_config():
    db_url = os.getenv("DATABASE_URL", "")
    if not db_url:
        return {"database_url": None}
    try:
        p = urlparse(db_url.replace("+pymysql", ""))
        user = p.username or ""
        host = p.hostname or ""
        port = p.port or ""
        path = (p.path or "").lstrip("/")
        return {"database_url_masked": f"{p.scheme}://{user}:***@{host}:{port}/{path}"}
    except Exception:
        return {"database_url_masked": "***"}

@app.get("/debug/conectores", tags=["Salud"], dependencies=[Depends(require_key)])
async def debug_conectores():
    gbif_r, col_r, worms_r, itis_r, iucn_r, sib_r, rep_r, bat_r, aco_r, mam_r, cites_r = await asyncio.gather(
        check_gbif(), check_col(), check_worms(), check_itis(), check_iucn(), check_sib(),
        check_reptile_db(), check_batrachia(), check_aco_birds(), check_sib_mammals_2024(), check_cites_public()
    )
    return {
        "gbif": gbif_r,
        "col": col_r,
        "worms": worms_r,
        "itis": itis_r,
        "iucn": iucn_r,
        "sib_colombia": sib_r,
        "reptile_db": rep_r,
        "batrachia": bat_r,
        "aco_aves": aco_r,
        "mamiferos_2024": mam_r,
        "cites_public": cites_r,
    }

# ---------- DEBUG ITIS ----------
@app.get("/debug/itis/raw", tags=["Salud"], summary="Debug Itis Raw", dependencies=[Depends(require_key)])
async def debug_itis_raw(q: str = Query(..., description="Nombre científico")):
    raw = await itis_client.search_by_scientific_name(q)
    return {"query": q, "raw": raw}

@app.get("/debug/itis", tags=["Salud"], summary="Debug Itis (parseado)", dependencies=[Depends(require_key)])
async def debug_itis(q: str = Query(..., description="Nombre científico")):
    from .clients import itis
    info = await itis.lookup_taxonomy(q)
    return {
        "query": q,
        "parsed": (info or {}).get("parsed") or {},
        "meta": {"tsn": ((info or {}).get("full") or {}).get("tsn")},
        "raw_has_scientificNames": bool(((info or {}).get("raw") or {}).get("scientificNames")),
    }

# ---------- DEBUG CoL ----------
@app.get("/debug/col/raw", tags=["Salud"], summary="Debug CoL Raw", dependencies=[Depends(require_key)])
async def debug_col_raw(q: str = Query(..., description="Nombre científico")):
    qn = normaliza_nombre(q)
    raw = await col_client.search_name(qn)
    usage_id = None
    items = raw.get("result") or raw.get("results") or []
    if items:
        top = items[0]
        usage_id = top.get("id") or top.get("usageId") or top.get("nameUsageId")
    detail = await col_client._detail_for_usage(usage_id) if usage_id else {}
    return {"query": qn, "raw": raw, "top_usage_id": usage_id, "detail": detail}

@app.get("/debug/col", tags=["Salud"], summary="Debug CoL (parseado)", dependencies=[Depends(require_key)])
async def debug_col(q: str = Query(..., description="Nombre científico")):
    qn = normaliza_nombre(q)
    raw = await col_client.search_name(qn)
    items = raw.get("result") or raw.get("results") or []
    if not items:
        return {"query": qn, "parsed": {}, "note": "Sin resultados en CoL (revisa dataset/endpoint)"}

    usage_id = items[0].get("id") or items[0].get("usageId") or items[0].get("nameUsageId")
    detail = await col_client._detail_for_usage(usage_id) if usage_id else {}

    merged = dict(items[0])
    if isinstance(detail, dict) and detail:
        if detail.get("classification"):
            merged["classification"] = detail["classification"]
        for k in ("usage", "accepted", "acceptedName", "authorship", "author", "labelAuthorship", "status"):
            if k in detail and k in detail and detail[k] is not None:
                merged[k] = detail[k]

    parsed = _taxonomy_from_col(merged)
    return {
        "query": qn,
        "usage_id": usage_id,
        "parsed": parsed,
        "has_classification": bool(merged.get("classification")),
    }

# ---------------- Reconciliación simple ----------------
@app.get("/reconcile", response_model=TaxonOut, tags=["Taxonomía"], dependencies=[Depends(require_key)])
async def reconcile(q: str, db: Session = Depends(get_db)):
    t = await reconcile_name(db, q)
    return TaxonOut(
        scientific_name=t.scientific_name,
        status=t.status,
        accepted_gbif_key=t.accepted_gbif_key,
        gbif_key=t.gbif_key,
        rank=t.rank,
        iucn_category=t.iucn_category,
    )

# ---------------- Reconciliación amigable (ES) ----------------
@app.get("/reconciliar", response_model=TaxonESOut, tags=["Taxonomía"], dependencies=[Depends(require_key)])
async def reconciliar(
    q: str = Query(..., description="Nombre científico, p. ej., 'Ateles belzebuth'"),
    db: Session = Depends(get_db),
):
    q_norm = normaliza_nombre(q)
    t = await reconcile_name(db, q_norm)
    epiteto = obtener_epiteto_especifico(t.canonical_name or t.scientific_name, t.rank)
    fuentes = t.sources_csv.split(",") if getattr(t, "sources_csv", None) else await buscar_en_fuentes_externas(q_norm)
    return TaxonESOut(
        nombre_cientifico=t.scientific_name,
        estado=t.status,
        epiteto_especifico=epiteto,
        clave_gbif=t.gbif_key,
        clave_aceptada_gbif=t.accepted_gbif_key,
        rango=t.rank,
        categoria_iucn=t.iucn_category,
        fuentes=fuentes,
    )

# ---------------- Reconciliación detalle ----------------
@app.get("/reconciliar/detalle", tags=["Taxonomía"], dependencies=[Depends(require_key)])
async def reconciliar_detalle(q: str, db: Session = Depends(get_db)):
    t = await reconcile_name(db, normaliza_nombre(q))
    return {
        "scientific_name": t.scientific_name,
        "canonical_name": t.canonical_name,
        "authorship": t.authorship,
        "rank": t.rank,
        "status": t.status,
        "gbif_key": t.gbif_key,
        "accepted_gbif_key": t.accepted_gbif_key,
        "iucn_category": t.iucn_category,
        "kingdom": t.kingdom, "phylum": t.phylum, "class_name": t.class_name,
        "order_name": t.order_name, "superfamily": t.superfamily, "family": t.family,
        "subfamily": t.subfamily, "tribe": t.tribe, "subtribe": t.subtribe,
        "genus": t.genus, "subgenus": t.subgenus,
        "epiteto_especifico": obtener_epiteto_especifico(t.canonical_name or t.scientific_name, t.rank),
        "fuentes": (t.sources_csv or "").split(",") if t.sources_csv else [],
        "provenance": json.loads(t.provenance_json) if t.provenance_json else {},
        "synonyms": [
            {"name": s.name, "authorship": s.authorship, "status": s.status,
             "source": s.source, "external_key": s.external_key, "rank": s.rank,
             "accepted_name": s.accepted_name}
            for s in (t.synonyms or [])
        ],
    }

# ---------------- Check rápido de fuentes externas ----------------
@app.get("/fuentes/check", tags=["Taxonomía"])
async def check_fuentes(q: str):
    ini = perf_counter()
    try:
        fuentes = await buscar_en_fuentes_externas(normaliza_nombre(q))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    fin = perf_counter()
    return {"nombre": q, "encontrado_en": fuentes, "tiempo_seg": round(fin - ini, 3)}

# ---------------- Sugerencias (autocompletado) ----------------
@app.get("/suggest", tags=["Taxonomía"])
async def suggest(q: str, limit: int = 8):
    if not q or not q.strip():
        return []
    url = "https://api.gbif.org/v1/species/suggest"
    params = {"q": q.strip(), "limit": max(1, min(limit, 20))}
    try:
        async with httpx.AsyncClient(timeout=8.0) as client:
            r = await client.get(url, params=params)
            r.raise_for_status()
            data = r.json()
            out = []
            for it in data[:params["limit"]]:
                out.append({
                    "label": it.get("scientificName") or it.get("canonicalName") or it.get("species"),
                    "canonicalName": it.get("canonicalName"),
                    "rank": it.get("rank"),
                    "key": it.get("key"),
                })
            return out
    except Exception:
        return []

# ---------------- BULK JSON -> JSON/CSV/XLSX/NDJSON ----------------
@app.post("/reconciliar/bulk", tags=["Archivos"], dependencies=[Depends(require_key)])
async def reconciliar_bulk(
    payload: dict = Body(..., description='{"names": ["Bos taurus","Ateles belzebuth"], "modo":"resumen|db","format":"json|csv|xlsx|ndjson"}'),
    db: Session = Depends(get_db),
):
    names = payload.get("names") or []
    modo = (payload.get("modo") or "resumen").lower()
    out_format = (payload.get("format") or "json").lower()

    if not isinstance(names, list) or not names:
        raise HTTPException(status_code=400, detail="Debes enviar 'names' como lista no vacía.")

    # Streaming NDJSON con progreso real
    if out_format == "ndjson":
        async def gen():
            tasks = [asyncio.create_task(_resolve_one(db, n, modo)) for n in names]
            done = 0
            total = len(tasks)
            for coro in asyncio.as_completed(tasks):
                row = await coro
                if not row:
                    continue
                done += 1
                yield (json.dumps({"type": "row", "row": row, "done": done, "total": total}) + "\n").encode("utf-8")
        return StreamingResponse(gen(), media_type="application/x-ndjson")

    # Paralelismo controlado (no streaming)
    tareas = [asyncio.create_task(_resolve_one(db, n, modo)) for n in names]
    registros = [r for r in await asyncio.gather(*tareas) if r]

    out_df = pd.DataFrame(registros)

    if modo == "db":
        db_cols = [c.name for c in Taxon.__table__.columns]
        cols = ["nombre_original"] + db_cols + ["_taxon_id"]
        for c in cols:
            if c not in out_df.columns:
                out_df[c] = None
        out_df = out_df[cols]

    # Sinónimos
    syn_df = pd.DataFrame()
    ids = [r["_taxon_id"] for r in registros if "_taxon_id" in r]
    if ids:
        syns = (
            db.execute(select(Synonym).where(Synonym.taxon_id.in_(ids)))
            .unique()
            .scalars()
            .all()
        )
        syn_rows = []
        id_to_name = {}
        if modo == "db":
            id_to_name = {int(r["_taxon_id"]): r.get("scientific_name") for _, r in out_df.iterrows()}
        for s in syns:
            syn_rows.append({
                "id": s.id,
                "taxon_id": s.taxon_id,
                "name": s.name,
                "authorship": s.authorship,
                "status": s.status,
                "source": s.source,
                "external_key": s.external_key,
                "taxon_rank": s.rank,
                "accepted_name": s.accepted_name,
                "scientific_name": s.accepted_name if modo != "db" else id_to_name.get(s.taxon_id),
            })
        syn_df = pd.DataFrame(syn_rows)

    if out_format == "json":
        extra: Dict[str, Any] = {}
        if not syn_df.empty and "_taxon_id" in out_df.columns:
            syn_concat = (
                syn_df.groupby("taxon_id")["name"]
                .apply(lambda xs: " | ".join(sorted(set(x for x in xs if x))))
                .to_dict()
            )
            out_df["synonyms_csv"] = out_df["_taxon_id"].map(syn_concat)

        extra["taxa"] = json.loads(out_df.drop(columns=["_taxon_id"], errors="ignore").to_json(orient="records"))
        if not syn_df.empty:
            extra["synonyms"] = json.loads(syn_df.to_json(orient="records"))
        return JSONResponse(extra)

    safe_base = "bulk"
    if out_format == "csv":
        if not syn_df.empty and "_taxon_id" in out_df.columns:
            syn_concat = (
                syn_df.groupby("taxon_id")["name"]
                .apply(lambda xs: " | ".join(sorted(set(x for x in xs if x))))
                .to_dict()
            )
            out_df["synonyms_csv"] = out_df["_taxon_id"].map(syn_concat)
        out_df = out_df.drop(columns=["_taxon_id"], errors="ignore")
        csv_buf = out_df.to_csv(index=False).encode("utf-8")
        headers = {"Content-Disposition": f'attachment; filename="{safe_base}_reconciliado.csv"'}
        return StreamingResponse(io.BytesIO(csv_buf), media_type="text/csv", headers=headers)

    # xlsx por defecto
    bio_out = io.BytesIO()
    with pd.ExcelWriter(bio_out, engine="openpyxl") as writer:
        (out_df.drop(columns=["_taxon_id"], errors="ignore")).to_excel(writer, index=False, sheet_name="taxones")
        if not syn_df.empty:
            syn_df.to_excel(writer, index=False, sheet_name="sinonimos")
    bio_out.seek(0)
    headers = {"Content-Disposition": f'attachment; filename="{safe_base}_reconciliado.xlsx"'}
    return StreamingResponse(
        bio_out,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers=headers,
    )

# ---------------- Cargar archivo ----------------
@app.post("/reconciliar/archivo", tags=["Archivos"], dependencies=[Depends(require_key)])
async def reconciliar_archivo(
    file: UploadFile = File(...),
    output: str = Form("xlsx"),
    modo: str = Form("db"),
    incluir_sinonimos: bool = Form(True),
    db: Session = Depends(get_db),
):
    data = await file.read()
    if not data:
        raise HTTPException(status_code=400, detail="Archivo vacío.")

    filename = (file.filename or "archivo").lower()
    ext = os.path.splitext(filename)[1].lower()

    # --- Lectura robusta del archivo (CSV/XLSX/XLS/XLSB) ---
    try:
        bio = io.BytesIO(data)
        df = None

        if ext == ".csv":
            # Intenta UTF-8 y luego latin-1; si viene sin header, fuerza una columna
            for enc in ("utf-8", "latin-1"):
                try:
                    bio.seek(0)
                    df = pd.read_csv(bio, encoding=enc)
                    break
                except Exception:
                    pass
            if df is None:
                bio.seek(0)
                df = pd.read_csv(bio, header=None)
        elif ext in (".xlsx", ".xls", ".xlsb"):
            if ext == ".xlsx":
                try:
                    bio.seek(0)
                    df = pd.read_excel(bio, engine="openpyxl")
                except Exception:
                    bio.seek(0)
                    df = pd.read_excel(bio, engine="openpyxl", header=None)
            elif ext == ".xls":
                try:
                    bio.seek(0)
                    df = pd.read_excel(bio, engine="xlrd")
                except Exception as e:
                    raise HTTPException(
                        status_code=400,
                        detail=("El formato .xls requiere xlrd==1.2.0. "
                                "Instala con: pip install 'xlrd==1.2.0'  "
                                f"(detalle: {e})")
                    )
            else:  # .xlsb
                try:
                    bio.seek(0)
                    df = pd.read_excel(bio, engine="pyxlsb")
                except Exception as e:
                    raise HTTPException(
                        status_code=400,
                        detail=("El formato .xlsb requiere pyxlsb. "
                                "Instala con: pip install pyxlsb  "
                                f"(detalle: {e})")
                    )
        else:
            raise HTTPException(status_code=400, detail="Formato no soportado. Usa .csv o .xlsx (recomendado).")

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"No se pudo leer el archivo: {e}")

    if df is None or df.empty:
        raise HTTPException(status_code=400, detail="Archivo vacío o sin filas útiles.")

    # --- Detección y normalización de la columna con nombres científicos ---
    POSIBLES = {"scientific_name", "nombre_cientifico", "nombre", "scientificname", "species"}

    def _sanitize_cols(cols):
        out = []
        for c in cols:
            s = str(c or "").strip().lower()
            s = s.replace("\ufeff", "")  # BOM
            out.append(s)
        return out

    cols_norm = _sanitize_cols(df.columns)

    # Si todas las columnas son Unnamed o índices → asume 1ª como scientific_name
    if all(c.startswith("unnamed") or c == "" for c in cols_norm):
        df.rename(columns={df.columns[0]: "scientific_name"}, inplace=True)
        cols_norm = _sanitize_cols(df.columns)

    # Busca directo
    col_name = next((df.columns[i] for i, c in enumerate(cols_norm) if c in POSIBLES), None)

    # Si no encontramos cabecera válida pero la 1ª-5ª fila parece serla, promuévela a header
    if not col_name:
        head_row = None
        lookahead = min(len(df), 5)
        for i in range(lookahead):
            row0 = [str(x or "").strip().lower() for x in df.iloc[i].tolist()]
            if any(x in POSIBLES for x in row0):
                head_row = i
                break
        if head_row is not None:
            bio2 = io.BytesIO(data)
            if ext == ".csv":
                df = pd.read_csv(bio2, header=head_row)
            elif ext == ".xlsx":
                df = pd.read_excel(bio2, engine="openpyxl", header=head_row)
            elif ext == ".xls":
                df = pd.read_excel(bio2, engine="xlrd", header=head_row)
            else:  # .xlsb
                df = pd.read_excel(bio2, engine="pyxlsb", header=head_row)
            cols_norm = _sanitize_cols(df.columns)
            col_name = next((df.columns[i] for i, c in enumerate(cols_norm) if c in POSIBLES), None)

    # Último intento: si solo hay una columna y no está en POSIBLES, renómbrala
    if not col_name and len(df.columns) == 1 and _sanitize_cols([df.columns[0]])[0] not in POSIBLES:
        df.rename(columns={df.columns[0]: "scientific_name"}, inplace=True)
        cols_norm = _sanitize_cols(df.columns)
        col_name = df.columns[0]

    if not col_name:
        raise HTTPException(
            status_code=400,
            detail=("No encuentro la columna con nombres científicos. "
                    "Usa 'scientific_name' (recomendado) o 'nombre_cientifico', 'nombre', 'scientificname', 'species'."))
    # Limpieza columna
    df[col_name] = df[col_name].astype(str).fillna("").str.strip()

    # === (2) Procesamiento paralelo con DEDUPLICACIÓN ===
    nombres = [n for n in df[col_name].astype(str).fillna("").str.strip().tolist() if n]
    if not nombres:
        raise HTTPException(status_code=400, detail="No se encontraron nombres válidos en la columna seleccionada.")

    def _normkey(s: str) -> str:
        return (s or "").strip().lower()

    rep_por_clave: dict[str, str] = {}
    ocurrencias: dict[str, list[str]] = {}
    for n in nombres:
        k = _normkey(n)
        if k not in rep_por_clave:
            rep_por_clave[k] = n
        ocurrencias.setdefault(k, []).append(n)

    unicos = list(rep_por_clave.values())
    tareas = [asyncio.create_task(_resolve_one(db, n, modo.lower())) for n in unicos]
    res_unicos = await asyncio.gather(*tareas)

    res_por_clave: dict[str, dict] = {}
    for n_rep, fila in zip(unicos, res_unicos):
        if not fila:
            continue
        res_por_clave[_normkey(n_rep)] = fila

    registros: list[dict] = []
    for k, variantes in ocurrencias.items():
        base = res_por_clave.get(k)
        if not base:
            for v in variantes:
                registros.append({"nombre_original": v})
            continue
        for v in variantes:
            r = dict(base)
            r["nombre_original"] = v
            registros.append(r)

    out_df = pd.DataFrame(registros)

    if modo.lower() == "db":
        db_cols = [c.name for c in Taxon.__table__.columns]
        cols = ["nombre_original"] + db_cols + ["_taxon_id"]
        for c in cols:
            if c not in out_df.columns:
                out_df[c] = None
        out_df = out_df[cols]

    syn_df = pd.DataFrame()
    if incluir_sinonimos:
        ids = [r["_taxon_id"] for r in registros if "_taxon_id" in r]
        if ids:
            syns = (
                db.execute(select(Synonym).where(Synonym.taxon_id.in_(ids)))
                .unique()
                .scalars()
                .all()
            )
            syn_rows = []
            id_to_name = {}
            if modo.lower() == "db":
                id_to_name = {int(r["_taxon_id"]): r.get("scientific_name") for _, r in out_df.iterrows()}
            for s in syns:
                syn_rows.append({
                    "id": s.id,
                    "taxon_id": s.taxon_id,
                    "name": s.name,
                    "authorship": s.authorship,
                    "status": s.status,
                    "source": s.source,
                    "external_key": s.external_key,
                    "taxon_rank": s.rank,
                    "accepted_name": s.accepted_name,
                    "scientific_name": id_to_name.get(s.taxon_id) if modo.lower() == "db" else s.accepted_name,
                })
            syn_df = pd.DataFrame(syn_rows)

    safe_base = os.path.splitext(os.path.basename(filename))[0] or "salida"

    if output.lower() == "csv":
        if incluir_sinonimos and not syn_df.empty and "_taxon_id" in out_df.columns:
            syn_concat = (
                syn_df.groupby("taxon_id")["name"]
                .apply(lambda xs: " | ".join(sorted(set(x for x in xs if x))))
                .to_dict()
            )
            out_df["synonyms_csv"] = out_df["_taxon_id"].map(syn_concat)
        out_df = out_df.drop(columns=["_taxon_id"], errors="ignore")

        csv_buf = out_df.to_csv(index=False).encode("utf-8")
        headers = {"Content-Disposition": f'attachment; filename="{safe_base}_reconciliado.csv"'}
        return StreamingResponse(io.BytesIO(csv_buf), media_type="text/csv", headers=headers)

    bio_out = io.BytesIO()
    with pd.ExcelWriter(bio_out, engine="openpyxl") as writer:
        (out_df.drop(columns=["_taxon_id"], errors="ignore")).to_excel(writer, index=False, sheet_name="taxones")
        if incluir_sinonimos and not syn_df.empty:
            syn_df.to_excel(writer, index=False, sheet_name="sinonimos")
    bio_out.seek(0)
    headers = {"Content-Disposition": f'attachment; filename="{safe_base}_reconciliado.xlsx"'}
    return StreamingResponse(bio_out, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", headers=headers)

# ---------------- Plantilla Excel simple ----------------
@app.get("/plantilla.xlsx", tags=["Archivos"])
async def plantilla():
    df = pd.DataFrame({"scientific_name": ["Ateles belzebuth", "Bos taurus"]})
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as w:
        df.to_excel(w, index=False, sheet_name="ejemplo")
    bio.seek(0)
    return StreamingResponse(
        bio,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": 'attachment; filename="plantilla_reconciliar.xlsx"'}
    )

# ---------------- UI Buscador (tematizada, con progreso y NDJSON) ----------------
@app.get("/ui/buscador", tags=["UI"], response_class=HTMLResponse)
async def ui_buscador():
    html = """<!doctype html>
<html lang="es">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Buscador taxonómico</title>
<style>
  :root{
    --bg:__BG__; --card:__CARD__; --ink:__INK__; --muted:__MUTED__;
    --acc:__ACC__; --border:__BORDER__; --hover:__HOVER__; --sel:__SEL__;
    --ok:__OK__; --chip:__CHIP__;
  }
  *{box-sizing:border-box}
  html,body{height:100%}
  body{
    margin:0;
    background:linear-gradient(180deg,var(--bg),#0d1630 50%,var(--bg));
    color:var(--ink);
    font:16px/1.55 system-ui,-apple-system,"Segoe UI",Roboto,Ubuntu;
    -webkit-font-smoothing:antialiased; text-rendering:optimizeLegibility;
  }
  .wrap{max-width:1100px;margin:40px auto;padding:0 16px}
  .card{
    background:var(--card);
    border:1px solid var(--border);
    border-radius:16px;
    box-shadow:0 10px 30px rgba(0,0,0,.35);
    padding:22px;
  }
  h1{font-size:22px;margin:0 0 8px}
  p.small{color:var(--muted);margin:0 0 14px}

  .row{position:relative;display:flex;gap:10px;flex-wrap:wrap}

  /* Inputs con mejor contraste */
  input[type=text], textarea, select{
    flex:1; min-width:300px;
    background:#0d1530;
    border:1px solid var(--border);
    color:var(--ink);
    padding:12px 14px;
    border-radius:12px;
    outline:none;
  }
  input::placeholder, textarea::placeholder{color:#b7c3e4}
  textarea{min-height:100px;resize:vertical}

  /* Botones: verde como primario */
  button,.tab{
    appearance:none; cursor:pointer;
    color:#06130a; background:var(--acc);
    border:0; border-radius:12px; padding:10px 14px; font-weight:700;
  }
  button:disabled{opacity:.6;cursor:not-allowed}

  /* Tabs visualmente homogéneas */
  .tabs{display:flex;gap:8px;margin:10px 0 8px}
  .tab{
    background:#0e1936; color:var(--ink);
    border:1px solid var(--border); font-weight:600;
  }
  .tab.active{background:var(--acc); color:#06130a}

  /* Select alineado con inputs */
  .select{background:#0d1530;border:1px solid var(--border);color:var(--ink);padding:10px;border-radius:10px}

  /* Accesibilidad: foco visible */
  :focus-visible{outline:2px solid var(--acc); outline-offset:2px; border-radius:10px}

  /* Chips/Badges con texto de alto contraste */
  .chip{
    padding:6px 10px;border-radius:999px;border:1px solid var(--border);
    background:var(--chip); font-size:12px; display:inline-block; margin-right:6px;
    color:#dfe6ff;
  }

  .muted{color:var(--muted)}
  .mt{margin-top:10px}
  .hr{height:1px;background:var(--border);margin:14px 0}
  .twrap{overflow:auto}

  /* Tabla: header pegajoso y contraste reforzado */
  table{width:100%;border-collapse:collapse;font-size:14px}
  th,td{border-bottom:1px solid #19254a;padding:9px 10px;text-align:left;vertical-align:top}
  thead th{position:sticky;top:0;background:#111b3b;color:#eaf0ff}

  /* Autocomplete */
  .ac{position:absolute;top:48px;left:0;right:0;background:#0d1530;border:1px solid var(--border);border-radius:12px;max-height:260px;overflow:auto;z-index:10;display:none}
  .ac-item{padding:10px 12px;cursor:pointer;border-bottom:1px solid rgba(255,255,255,.06)}
  .ac-item:last-child{border-bottom:none}
  .ac-item:hover{background:var(--hover)}
  .ac-item.active{background:var(--sel)}
  .ac-secondary{color:var(--muted);font-size:12px;margin-left:6px}

  .sep th{background:transparent;color:var(--muted);font-size:12px;text-transform:uppercase;letter-spacing:.04em}

  /* Progreso */
  progress{width:100%;height:10px;border:1px solid var(--border);border-radius:8px;background:#0b1430}
  progress::-webkit-progress-bar{background:#0b1430;border-radius:8px}
  progress::-webkit-progress-value{background:var(--acc);border-radius:8px}
</style>
</head>
<body>
<div class="wrap">
  <div class="card">
    <h1>Buscador taxonómico</h1>
    <p class="small">Consulta una especie con autocompletado, o varias a la vez (textarea/archivo). Si tu servidor exige API key, usa <code>?key=TU_API_KEY</code> o guarda la clave con el botón.</p>

    <div class="row" style="justify-content:flex-end">
      <button id="saveKey">Guardar API key…</button>
    </div>

    <div class="tabs">
      <button type="button" class="tab active" id="tab1">Una especie</button>
      <button type="button" class="tab" id="tab2">Varias / Archivo</button>
    </div>

    <div id="pane1">
      <div class="row">
        <input id="q1" type="text" placeholder="Bos taurus, Ateles belzebuth, ..." autocomplete="off" aria-label="Nombre científico"/>
        <button id="go1">Buscar</button>
        <div id="ac1" class="ac" role="listbox" aria-label="Sugerencias"></div>
      </div>
      <div id="msg1" class="small muted mt"></div>
      <div id="single" class="mt" hidden>
        <div id="badges1" class="mt"></div>
        <div class="hr"></div>
        <div class="twrap">
          <table><tbody id="grid1"></tbody></table>
        </div>
        <div class="hr"></div>
        <div id="synH1" class="small muted">Sinónimos</div>
        <ul id="syn1"></ul>
        <div class="hr"></div>
        <div id="prov1" class="mt"></div>
      </div>
    </div>

    <div id="pane2" style="display:none">
      <div class="row">
        <select id="modo" class="select">
          <option value="db">Todo lo de la DB</option>
          <option value="resumen" selected>Resumen + taxonomía</option>
        </select>
        <button id="go2">Buscar</button>
        <button id="dlXlsx">Descargar Excel</button>
        <button id="dlCsv">Descargar CSV</button>
        <label class="tab" for="file" style="cursor:pointer;margin-left:auto">o importar archivo</label>
        <input id="file" type="file" hidden>
        <span id="fname" class="small muted"></span>
        <button id="proc" class="tab">Procesar archivo</button>
      </div>

      <textarea id="q2" class="mt" placeholder="Escribe uno por línea, o separados por coma…"></textarea>
      <div id="msg2" class="small muted mt"></div>

      <div class="mt">
        <progress id="pbar" max="100" value="0" style="display:none"></progress>
        <div id="ptext" class="small muted"></div>
        <!-- Marcador para estado simple de subida de archivo -->
        <div id="progress" class="small" style="display:none;margin-top:.5rem;"></div>
      </div>

      <div class="twrap mt">
        <table>
          <thead>
            <tr>
              <th>Nombre original</th>
              <th>Nombre científico</th>
              <th>Estado</th>
              <th>GBIF key</th>
              <th>Rango</th>
              <th>IUCN</th>
              <th>Fuentes</th>
              <th>Sinónimos</th>
              <th>Reino</th>
              <th>Filo</th>
              <th>Clase</th>
              <th>Orden</th>
              <th>Superfamilia</th>
              <th>Familia</th>
              <th>Subfamilia</th>
              <th>Tribu</th>
              <th>Subtribu</th>
              <th>Género</th>
              <th>Subgénero</th>
            </tr>
          </thead>
          <tbody id="rows"></tbody>
        </table>
      </div>
      <div id="count" class="small muted mt"></div>
    </div>
  </div>
</div>

<script>
const $ = s => document.querySelector(s);
const keyParam = new URLSearchParams(location.search).get("key");
if(keyParam){ localStorage.setItem("api_key", keyParam); }
function headers(){ const k = localStorage.getItem("api_key"); return k ? {"X-API-Key": k} : {}; }
$("#saveKey").onclick = () => {
  const v = prompt("API key (X-API-Key):", localStorage.getItem("api_key") || "");
  if(v !== null){ localStorage.setItem("api_key", v.trim()); alert("Guardada."); }
};

function setTab(n){
  const t1 = $("#tab1"), t2 = $("#tab2"), p1 = $("#pane1"), p2 = $("#pane2");
  if(n===1){ t1.classList.add("active"); t2.classList.remove("active"); p1.style.display=""; p2.style.display="none"; $("#q1").focus(); }
  else     { t2.classList.add("active"); t1.classList.remove("active"); p2.style.display=""; p1.style.display="none"; $("#q2").focus(); }
}
$("#tab1").addEventListener("click", ()=>setTab(1));
$("#tab2").addEventListener("click", ()=>setTab(2));

let acTimer=null;
$("#q1").addEventListener("input", () => {
  const q = $("#q1").value.trim();
  clearTimeout(acTimer);
  if(!q){ $("#ac1").style.display="none"; return; }
  acTimer = setTimeout(async () => {
    try{
      const r = await fetch(`/suggest?q=${encodeURIComponent(q)}`, {headers: headers()});
      const arr = await r.json();
      const box = $("#ac1");
      box.innerHTML = (arr||[]).map(it =>
        `<div class="ac-item" data-v="${it.label}">
           ${it.label} <span class="ac-secondary">${it.rank || ""}</span>
         </div>`
      ).join("");
      box.style.display = arr && arr.length ? "block" : "none";
      box.querySelectorAll(".ac-item").forEach(el=>{
        el.onclick = () => { $("#q1").value = el.dataset.v; box.style.display="none"; };
      });
    }catch(e){ $("#ac1").style.display="none"; }
  }, 200);
});

$("#go1").onclick = async () => {
  const q = $("#q1").value.trim();
  if(!q){ $("#msg1").textContent = "Escribe un nombre científico."; return; }
  $("#msg1").textContent = "Buscando…";
  try{
    const r = await fetch(`/reconciliar/detalle?q=${encodeURIComponent(q)}`, {headers: headers()});
    if(r.status === 401){ $("#msg1").textContent = "No autorizado: configura tu API key."; return; }
    if(!r.ok){ $("#msg1").textContent = "Error en la consulta."; return; }
    const d = await r.json();
    $("#msg1").textContent = "";
    $("#single").hidden = false;

    const statusColor = (d.status||"").toLowerCase()==="accepted" ? "var(--ok)" : "var(--chip)";
    $("#badges1").innerHTML = `
      <span class="chip" style="background:${statusColor}">Estado: ${d.status ?? "-"}</span>
      <span class="chip">Rango: ${d.rank ?? "-"}</span>
      <span class="chip">GBIF: ${d.gbif_key ?? "-"}</span>
      <span class="chip">IUCN: ${d.iucn_category ?? "-"}</span>
    `;

    const TR = (k, v) => `<tr><th>${k}</th><td>${v ?? "-"}</td></tr>`;
    const headPairs = [
      ["Nombre científico", d.scientific_name],
      ["Canónico", d.canonical_name],
      ["Autoría", d.authorship],
      ["Epíteto", d.epiteto_especifico],
    ];
    const taxPairs = [
      ["Reino", d.kingdom],
      ["Filo", d.phylum],
      ["Clase", d.class_name],
      ["Orden", d.order_name],
      ["Superfamilia", d.superfamily],
      ["Familia", d.family],
      ["Subfamilia", d.subfamily],
      ["Tribu", d.tribe],
      ["Subtribu", d.subtribe],
      ["Género", d.genus],
      ["Subgénero", d.subgenus],
    ];
    const miscPairs = [["Fuentes", (d.fuentes || []).join(", ")]];

    $("#grid1").innerHTML = [
      ...headPairs.map(([k, v]) => TR(k, v)),
      `<tr class="sep"><th colspan="2">Taxonomía</th></tr>`,
      ...taxPairs.map(([k, v]) => TR(k, v)),
      ...miscPairs.map(([k, v]) => TR(k, v)),
    ].join("");

    const syns = Array.isArray(d.synonyms) ? d.synonyms.filter(Boolean) : [];
    $("#synH1").textContent = `Sinónimos${syns.length ? ` (${syns.length})` : ""}`;
    if(syns.length){
      $("#syn1").innerHTML = syns.map(s => `
        <li>
          ${s.name || "-"}
          ${s.authorship ? `<span class="ac-secondary">${s.authorship}</span>` : ""}
          ${s.status ? `<span class="ac-secondary">[${s.status}]</span>` : ""}
          ${s.source ? `<span class="ac-secondary">${s.source}</span>` : ""}
        </li>
      `).join("");
    }else{
      $("#syn1").innerHTML = `<li class="ac-secondary">No se registran sinónimos para esta especie.</li>`;
    }

    $("#prov1").innerHTML = "<pre style='white-space:pre-wrap'>" + JSON.stringify(d.provenance || {}, null, 2) + "</pre>";
  }catch(e){
    $("#msg1").textContent = "Error de red.";
  }
};

function parseNames(raw){
  return Array.from(new Set(
    raw.split(/[\\n,;]+/).map(s => s.trim()).filter(Boolean)
  ));
}

/* Progreso para bulk y también utilizable en subida de archivo */
function startProgress(total){
  const p = $("#pbar");
  p.style.display = "block";
  p.removeAttribute("value"); // indeterminado hasta que llegue la 1a línea
  $("#ptext").textContent = total ? `Procesando ${total} nombres…` : "Procesando…";
  const simple = $("#progress"); if(simple){ simple.style.display="block"; simple.textContent="Procesando…"; }
}
function updateProgress(done, total){
  const p = $("#pbar");
  p.max = total || 100;
  p.value = done;
  $("#ptext").textContent = `Procesados ${done}/${total}`;
}
function stopProgress(){
  $("#pbar").style.display = "none";
  $("#ptext").textContent = "";
  const simple = $("#progress"); if(simple){ simple.style.display="none"; simple.textContent=""; }
}

async function streamBulk(names, modo){
  $("#rows").innerHTML = "";
  $("#count").textContent = "";
  startProgress(names.length);

  const r = await fetch("/reconciliar/bulk", {
    method: "POST",
    headers: Object.assign({"Content-Type":"application/json"}, headers()),
    body: JSON.stringify({names, modo, format:"ndjson"})
  });
  if(r.status === 401){ $("#msg2").textContent = "No autorizado: configura tu API key."; stopProgress(); return; }
  if(!r.ok){
    let msg = "Error en la consulta.";
    try{ const j = await r.json(); if(j?.detail) msg = j.detail; }catch{}
    $("#msg2").textContent = msg;
    stopProgress();
    return;
  }

  const reader = r.body.getReader();
  const decoder = new TextDecoder();
  let buf = "", doneCnt = 0, total = names.length;

  while(true){
    const {value, done} = await reader.read();
    if(done) break;
    buf += decoder.decode(value, {stream:true});
    let idx;
    while((idx = buf.indexOf("\\n")) >= 0){
      const line = buf.slice(0, idx); buf = buf.slice(idx+1);
      if(!line.trim()) continue;
      try{
        const msg = JSON.parse(line);
        if(msg.type === "row"){
          const rr = msg.row || {};
          doneCnt = msg.done || (doneCnt + 1);
          total = msg.total || total;
          const tr = `
            <tr>
              <td>${rr.nombre_original ?? "-"}</td>
              <td>${rr.nombre_cientifico ?? rr.scientific_name ?? "-"}</td>
              <td>${rr.estado ?? rr.status ?? "-"}</td>
              <td>${rr.clave_gbif ?? rr.gbif_key ?? "-"}</td>
              <td>${rr.rango ?? rr.rank ?? "-"}</td>
              <td>${rr.categoria_iucn ?? rr.iucn_category ?? "-"}</td>
              <td>${(rr.fuentes || rr.sources_csv || "").toString()}</td>
              <td>${(rr.synonyms_csv || "").toString()}</td>
              <td>${rr.reino ?? rr.kingdom ?? "-"}</td>
              <td>${rr.filo ?? rr.phylum ?? "-"}</td>
              <td>${rr.clase ?? rr.class_name ?? "-"}</td>
              <td>${rr.orden ?? rr.order_name ?? "-"}</td>
              <td>${rr.superfamilia ?? rr.superfamily ?? "-"}</td>
              <td>${rr.familia ?? rr.family ?? "-"}</td>
              <td>${rr.subfamilia ?? rr.subfamily ?? "-"}</td>
              <td>${rr.tribu ?? rr.tribe ?? "-"}</td>
              <td>${rr.subtribu ?? rr.subtribe ?? "-"}</td>
              <td>${rr.genero ?? rr.genus ?? "-"}</td>
              <td>${rr.subgenero ?? rr.subgenus ?? "-"}</td>
            </tr>`;
          $("#rows").insertAdjacentHTML("beforeend", tr);
          updateProgress(doneCnt, total);
        }
      }catch(e){}
    }
  }
  $("#count").textContent = `Resultados: ${$("#rows").children.length}`;
  stopProgress();
  $("#msg2").textContent = "";
}

$("#go2").onclick = async () => {
  const names = parseNames($("#q2").value);
  if(names.length === 0){ $("#msg2").textContent = "Escribe al menos un nombre."; return; }
  $("#msg2").textContent = "";
  await streamBulk(names, $("#modo").value);
};

$("#file").onchange = () => {
  const f = $("#file").files[0];
  $("#fname").textContent = f ? f.name : "";
};

// Descargas: mostrar detalle del backend
async function downloadBlob(url, method, body, filename){
  const hdrs = headers();
  if (typeof body === "string") { hdrs["Content-Type"] = "application/json"; }
  const r = await fetch(url, { method, body, headers: hdrs });
  if (!r.ok) {
    let msg = "Error al generar archivo";
    try {
      const ct = r.headers.get("content-type") || "";
      if (ct.includes("application/json")) {
        const j = await r.json();
        if (j?.detail) msg = j.detail;
        else msg = JSON.stringify(j);
      } else {
        msg = await r.text();
      }
    } catch {}
    alert(`${msg}`);
    return;
  }
  const blob = await r.blob();
  const a = document.createElement("a");
  a.href = URL.createObjectURL(blob);
  a.download = filename;
  a.click();
  URL.revokeObjectURL(a.href);
}

/* (3) Handler reemplazado: deshabilita botones y muestra “Procesando…” durante la subida */
$("#proc").onclick = async () => {
  const f = $("#file").files[0];
  if(!f){ alert("Primero selecciona un archivo."); return; }

  $("#proc").disabled = true;
  $("#go2").disabled = true;
  $("#dlXlsx").disabled = true;
  $("#dlCsv").disabled = true;
  startProgress(); // “Procesando…”

  try {
    const fd = new FormData();
    fd.append("file", f);
    fd.append("output", "xlsx"); // por defecto
    fd.append("modo", $("#modo").value);
    fd.append("incluir_sinonimos", "true");

    await downloadBlob("/reconciliar/archivo", "POST", fd, "reconciliado.xlsx");
  } finally {
    stopProgress();
    $("#proc").disabled = false;
    $("#go2").disabled = false;
    $("#dlXlsx").disabled = false;
    $("#dlCsv").disabled = false;
  }
};

$("#dlCsv").onclick = async () => {
  const names = parseNames($("#q2").value);
  if(!names.length){ alert("Escribe nombres primero."); return; }
  const payload = JSON.stringify({names, modo: $("#modo").value, format:"csv"});
  await downloadBlob("/reconciliar/bulk", "POST", payload, "reconciliado.csv");
};
$("#dlXlsx").onclick = async () => {
  const names = parseNames($("#q2").value);
  if(!names.length){ alert("Escribe nombres primero."); return; }
  const payload = JSON.stringify({names, modo: $("#modo").value, format:"xlsx"});
  await downloadBlob("/reconciliar/bulk", "POST", payload, "reconciliado.xlsx");
};
</script>
</body>
</html>
"""
    themed = (
        html
        .replace("__BG__", THEME["bg"])
        .replace("__CARD__", THEME["card"])
        .replace("__INK__", THEME["ink"])
        .replace("__MUTED__", THEME["muted"])
        .replace("__ACC__", THEME["acc"])
        .replace("__BORDER__", THEME["border"])
        .replace("__HOVER__", THEME["hover"])
        .replace("__SEL__", THEME["sel"])
        .replace("__OK__", THEME["ok"])
        .replace("__CHIP__", THEME["chip"])
    )
    return HTMLResponse(themed)

# ---------------- Docs (Swagger UI) tematizados ----------------
@app.get("/docs", include_in_schema=False)
async def custom_swagger_ui():
    resp = get_swagger_ui_html(openapi_url="/openapi.json", title="API Docs")
    body = resp.body.decode("utf-8")
    style = """
    <style>
      :root{ --acc:__ACC__; --bg:__BG__; --ink:__INK__; }
      body{ background: var(--bg) !important; color: var(--ink) !important; }
      .swagger-ui .topbar{ background: var(--acc) !important; }
      .swagger-ui .topbar a{ color:#06130a !important; font-weight:800; }
      .swagger-ui .scheme-container, .swagger-ui .opblock, .swagger-ui .info, .swagger-ui .servers{
        background: transparent !important; color: var(--ink) !important;
      }
      .swagger-ui, .swagger-ui .markdown p, .swagger-ui .model-title, .swagger-ui .parameter__name{
        color: var(--ink) !important;
      }
      .swagger-ui .btn.execute{ background-color: var(--acc) !important; color:#06130a !important; font-weight:800; }
      .swagger-ui .btn.try-out__btn{ border-color: var(--acc) !important; color: var(--acc) !important; }
      .swagger-ui input[type=text], .swagger-ui textarea, .swagger-ui .opblock .opblock-section input, .swagger-ui select{
        background:#0d1530 !important; border:1px solid #25345b !important; color: var(--ink) !important;
      }
      .swagger-ui .opblock.opblock-get{ border-color: var(--acc) !important; background:#0e1a36 !important; }
      .swagger-ui .opblock-get .opblock-summary-method{ background: var(--acc) !important; color:#06130a !important; }
      .swagger-ui .opblock-get .tab-header .tab-item.active h4 span:after{ background: var(--acc) !important; }
    </style>
    """
    style = (
        style
        .replace("__ACC__", THEME["acc"])
        .replace("__BG__", THEME["bg"])
        .replace("__INK__", THEME["ink"])
    )
    themed = body.replace("</head>", style + "</head>")
    return HTMLResponse(themed)

# ------------------------------------------------------------
# Registro de routers (NUEVO: lotes)
# ------------------------------------------------------------
app.include_router(reconcile_batch_router)

# ------------------------------------------------------------
# Shutdown: liberar cliente HTTP global de servicios (NUEVO)
# ------------------------------------------------------------
@app.on_event("shutdown")
async def _shutdown():
    try:
        await close_http_client()
    except Exception:
        pass

# ---------------- Main ----------------
if __name__ == "__main__":
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
