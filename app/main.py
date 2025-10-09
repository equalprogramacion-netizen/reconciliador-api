# app/main.py
from __future__ import annotations

# ------------------------------------------------------------
# Importaciones estándar y de terceros
# ------------------------------------------------------------
import os, io, json, asyncio, uvicorn
from typing import List, Optional, Dict, Any
from time import perf_counter
from urllib.parse import urlparse

import pandas as pd
import httpx
from fastapi import (
    FastAPI, Depends, Query, HTTPException,
    UploadFile, File, Form, Body, Security
)
# Importamos RedirectResponse y HTMLResponse para el redirect/fallback de la raíz
from fastapi.responses import RedirectResponse, StreamingResponse, HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security.api_key import APIKeyHeader
from sqlalchemy.orm import Session
from sqlalchemy import select

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
from .diagnostics import check_gbif, check_col, check_worms, check_itis, check_iucn, check_sib
from .clients import itis as itis_client
from .clients import col as col_client

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
# Instancia FastAPI
# ------------------------------------------------------------
app = FastAPI(
    title="Compilador de Especies (Colombia primero)",
    description="API en español para reconciliar nombres científicos con GBIF e integrar IUCN, Catalogue of Life, WoRMS, ITIS y SIB Colombia.",
    version="0.5.0",
    openapi_tags=TAGS_METADATA,
)

# ------------------------------------------------------------
# CORS abierto (ajusta en producción si lo necesitas)
# ------------------------------------------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ------------------------------------------------------------
# Inicializa tablas si no existen
# ------------------------------------------------------------
Base.metadata.create_all(bind=engine)

# ------------------------------------------------------------
# Seguridad por API Key (opcional). Si no hay API_KEY, no exige header.
# ------------------------------------------------------------
API_KEY = os.getenv("API_KEY")
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

async def require_key(api_key: str = Security(api_key_header)):
    """Valida la API key si está configurada la variable de entorno API_KEY."""
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
# FRONTEND_URL: si se define, redirigimos la raíz '/' hacia esa UI
# ------------------------------------------------------------
FRONTEND_URL = os.getenv("FRONTEND_URL")  # p. ej. https://buscador-ui.onrender.com

# ---------------- Raíz ----------------
@app.get("/", include_in_schema=False)
async def root():
    """
    Comportamiento al entrar a la raíz '/':
    - Si FRONTEND_URL está definida, se redirige al frontend de usuarios.
    - Si no está definida, se muestra una página morada con enlace a /docs.
    """
    if FRONTEND_URL:
        # Redirección 307/302 al frontend (Render, Vercel, etc.)
        return RedirectResponse(FRONTEND_URL)
    # Fallback HTML elegante cuando no hay FRONTEND_URL configurada
    return HTMLResponse("""
    <!doctype html>
    <html lang="es">
      <head>
        <meta charset="utf-8">
        <title>Compilador de Especies</title>
        <meta name="viewport" content="width=device-width, initial-scale=1"/>
      </head>
      <body style="margin:0; font-family: system-ui, -apple-system, Segoe UI, Roboto, Ubuntu; background:#1f1133; color:#fff; min-height:100vh; display:flex; align-items:center; justify-content:center; text-align:center">
        <div>
          <h1 style="margin-bottom:.5rem">Compilador de Especies</h1>
          <p style="opacity:.8">Configura la variable de entorno <code>FRONTEND_URL</code> para redirigir a la UI de usuarios.</p>
          <p><a href="/docs" style="color:#a78bfa; text-decoration:underline">Ver documentación de la API</a></p>
        </div>
      </body>
    </html>
    """)

# ---------------- Salud / Debug ----------------
@app.get("/health", tags=["Salud"])
async def health():
    """Endpoint simple de salud del servicio."""
    return {"status": "ok"}

@app.get("/debug/config", tags=["Salud"])
async def debug_config():
    """Devuelve la URL de la base de datos enmascarada (si existe)."""
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

@app.get("/debug/conectores", tags=["Salud"])
async def debug_conectores():
    """Chequea conectividad con GBIF, CoL, WoRMS, ITIS, IUCN y SIB."""
    gbif_r, col_r, worms_r, itis_r, iucn_r, sib_r = await asyncio.gather(
        check_gbif(), check_col(), check_worms(), check_itis(), check_iucn(), check_sib()
    )
    return {"gbif": gbif_r, "col": col_r, "worms": worms_r, "itis": itis_r, "iucn": iucn_r, "sib_colombia": sib_r}

# ---------- DEBUG ITIS ----------
@app.get("/debug/itis/raw", tags=["Salud"], summary="Debug Itis Raw")
async def debug_itis_raw(q: str = Query(..., description="Nombre científico")):
    """Devuelve la respuesta cruda del cliente ITIS para un nombre científico."""
    raw = await itis_client.search_by_scientific_name(q)
    return {"query": q, "raw": raw}

@app.get("/debug/itis", tags=["Salud"], summary="Debug Itis (parseado)")
async def debug_itis(q: str = Query(..., description="Nombre científico")):
    """Devuelve info parseada desde ITIS (TSN y bandera si trae scientificNames)."""
    from .clients import itis
    info = await itis.lookup_taxonomy(q)
    return {
        "query": q,
        "parsed": (info or {}).get("parsed") or {},
        "meta": {"tsn": ((info or {}).get("full") or {}).get("tsn")},
        "raw_has_scientificNames": bool(((info or {}).get("raw") or {}).get("scientificNames")),
    }

# ---------- DEBUG CoL ----------
@app.get("/debug/col/raw", tags=["Salud"], summary="Debug CoL Raw")
async def debug_col_raw(q: str = Query(..., description="Nombre científico")):
    """Búsqueda y detalle raw del Catalogue of Life para inspección."""
    qn = normaliza_nombre(q)
    raw = await col_client.search_name(qn)
    usage_id = None
    items = raw.get("result") or raw.get("results") or []
    if items:
        top = items[0]
        usage_id = top.get("id") or top.get("usageId") or top.get("nameUsageId")
    detail = await col_client._detail_for_usage(usage_id) if usage_id else {}
    return {"query": qn, "raw": raw, "top_usage_id": usage_id, "detail": detail}

@app.get("/debug/col", tags=["Salud"], summary="Debug CoL (parseado)")
async def debug_col(q: str = Query(..., description="Nombre científico")):
    """Resultado parseado útil para verificar clasificación y campos clave."""
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
            if k in detail and detail[k] is not None:
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
    """Reconciliación rápida (campos esenciales)."""
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
    """
    Reconciliación con nombres y campos amigables en español,
    incluyendo epíteto y fuentes.
    """
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
    """Devuelve todos los detalles del taxón, sinónimos y provenance."""
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
        # taxonomía completa (extendida)
        "kingdom": t.kingdom, "phylum": t.phylum, "class_name": t.class_name,
        "order_name": t.order_name, "superfamily": t.superfamily, "family": t.family,
        "subfamily": t.subfamily, "tribe": t.tribe, "subtribe": t.subtribe,
        "genus": t.genus, "subgenus": t.subgenus,
        # derivados
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
    """Hace un ping rápido a fuentes externas para ver dónde aparece el nombre."""
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
    """Sugiere nombres desde GBIF para autocompletar en la UI."""
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
        # Silenciamos error para no romper la UI; devolvemos lista vacía
        return []

# ---------------- BULK JSON -> JSON/CSV/XLSX ----------------
@app.post("/reconciliar/bulk", tags=["Archivos"], dependencies=[Depends(require_key)])
async def reconciliar_bulk(
    payload: dict = Body(..., description='{"names": ["Bos taurus","Ateles belzebuth"], "modo":"resumen|db","format":"json|csv|xlsx"}'),
    db: Session = Depends(get_db),
):
    """
    Recibe nombres en JSON y devuelve resultados en JSON/CSV/XLSX.
    - modo='db' -> exporta columnas de la tabla + extras
    - modo='resumen' -> exporta una vista resumida amigable
    """
    names = payload.get("names") or []
    modo = (payload.get("modo") or "resumen").lower()
    out_format = (payload.get("format") or "json").lower()

    if not isinstance(names, list) or not names:
        raise HTTPException(status_code=400, detail="Debes enviar 'names' como lista no vacía.")

    registros: list[dict] = []

    for nombre in [str(n or "").strip() for n in names]:
        if not nombre:
            continue
        n_norm = normaliza_nombre(nombre)
        t = await reconcile_name(db, n_norm)

        if modo == "db":
            # Dump de columnas de la tabla Taxon + auxiliares
            row = {col.name: getattr(t, col.name) for col in Taxon.__table__.columns}
            row["nombre_original"] = nombre
            row["_taxon_id"] = t.id
        else:
            # Vista resumida en español
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
        registros.append(row)

    out_df = pd.DataFrame(registros)

    if modo == "db":
        # Alinea el orden de columnas para exportes "completos"
        db_cols = [c.name for c in Taxon.__table__.columns]
        cols = ["nombre_original"] + db_cols + ["_taxon_id"]
        for c in cols:
            if c not in out_df.columns:
                out_df[c] = None
        out_df = out_df[cols]

    # Sinónimos (por _taxon_id)
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
            # Mapa para adjuntar scientific_name al export "db"
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

    # Salidas
    if out_format == "json":
        # JSON: incluimos taxa y opcionalmente synonyms.
        extra = {}
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
        # CSV: agregamos columna de sinónimos concatenados si aplica.
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

    # XLSX con dos hojas si hay sinónimos
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
    """
    Recibe un archivo CSV/XLSX con una columna de nombre científico,
    procesa y devuelve CSV/XLSX reconciliado.
    """
    data = await file.read()
    if not data:
        raise HTTPException(status_code=400, detail="Archivo vacío.")

    filename = (file.filename or "archivo").lower()

    # Lectura tolerante del archivo (CSV/XLSX; intenta ambos si la extensión no ayuda)
    try:
        bio = io.BytesIO(data)
        if filename.endswith(".csv"):
            df = pd.read_csv(bio)
        elif filename.endswith(".xlsx") or filename.endswith(".xls"):
            df = pd.read_excel(bio)
        else:
            try:
                bio.seek(0); df = pd.read_csv(bio)
            except Exception:
                bio.seek(0); df = pd.read_excel(bio)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"No se pudo leer el archivo: {e}")

    POSIBLES = {"scientific_name","nombre_cientifico","nombre","scientificname","species"}
    col_name = next((c for c in df.columns if str(c).strip().lower() in POSIBLES), None)
    if not col_name:
        raise HTTPException(
            status_code=400,
            detail="El archivo debe tener una columna de nombre científico (scientific_name/nombre_cientifico/nombre/species)."
        )

    registros: list[dict] = []
    for nombre in df[col_name].astype(str).fillna(""):
        n = (nombre or "").strip()
        if not n:
            continue
        n_norm = normaliza_nombre(n)
        t = await reconcile_name(db, n_norm)

        if modo.lower() == "db":
            row = {col.name: getattr(t, col.name) for col in Taxon.__table__.columns}
            row["nombre_original"] = n
            row["_taxon_id"] = t.id
        else:
            epiteto = obtener_epiteto_especifico(t.canonical_name or t.scientific_name, t.rank)
            row = {
                "_taxon_id": t.id,
                "nombre_original": n,
                "nombre_cientifico": t.scientific_name,
                "epiteto_especifico": epiteto,
                "estado": t.status,
                "clave_gbif": t.gbif_key,
                "clave_aceptada_gbif": t.accepted_gbif_key,
                "rango": t.rank,
                "categoria_iucn": t.iucn_category,
                "reino": t.kingdom,
                "filo": t.phylum,
                "clase": t.class_name,
                "orden": t.order_name,
                "superfamilia": t.superfamily,
                "familia": t.family,
                "subfamilia": t.subfamily,
                "tribu": t.tribe,
                "subtribu": t.subtribe,
                "genero": t.genus,
                "subgenero": t.subgenus,
                "fuentes": t.sources_csv,
            }
        registros.append(row)

    out_df = pd.DataFrame(registros)

    if modo.lower() == "db":
        db_cols = [c.name for c in Taxon.__table__.columns]
        cols = ["nombre_original"] + db_cols + ["_taxon_id"]
        for c in cols:
            if c not in out_df.columns:
                out_df[c] = None
        out_df = out_df[cols]

    # Sinónimos
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

    # Nombre base seguro para archivos de salida
    safe_base = os.path.splitext(os.path.basename(filename))[0] or "salida"

    if output.lower() == "csv":
        # CSV con opcional columna 'synonyms_csv' agregada
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

    # XLSX con dos hojas si corresponde
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
    """Descarga una plantilla mínima para cargar nombres científicos."""
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

# ---------------- UI Buscador ----------------
@app.get("/ui/buscador", tags=["UI"], response_class=HTMLResponse)
async def ui_buscador():
    """
    UI ligera (HTML/JS/CSS embebido) para consultas individuales o múltiples,
    con autocompletado (GBIF) y exportes CSV/XLSX.
    """
    return HTMLResponse("""<!doctype html>
<html lang="es">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Buscador taxonómico</title>
<style>
  :root{
    --bg:#0b1020; --card:#111833; --ink:#e6ebff; --muted:#9aa3c7; --acc:#7c9bff; --border:#1f2547;
    --hover:#16204a; --sel:#223266; --ok:#1fbe72; --chip:#0b122b;
  }
  *{box-sizing:border-box}
  body{margin:0;background:linear-gradient(180deg,#0b1020,#0e1530 50%,#0b1020);color:var(--ink);font:15px/1.45 system-ui,-apple-system,Segoe UI,Roboto,Ubuntu}
  .wrap{max-width:1100px;margin:40px auto;padding:0 16px}
  .card{background:var(--card);border:1px solid var(--border);border-radius:16px;box-shadow:0 10px 30px rgba(0,0,0,.25);padding:20px}
  h1{font-size:22px;margin:0 0 8px}
  p.small{color:var(--muted);margin:0 0 14px}
  .row{position:relative;display:flex;gap:10px;flex-wrap:wrap}
  input[type=text], textarea{flex:1;min-width:300px;background:#0b1126;border:1px solid var(--border);color:var(--ink);padding:12px 14px;border-radius:12px;outline:none}
  textarea{min-height:90px;resize:vertical}
  button{appearance:none;background:var(--acc);color:#fff;border:0;border-radius:12px;padding:10px 14px;cursor:pointer;font-weight:600}
  button:disabled{opacity:.6;cursor:not-allowed}
  .tabs{display:flex;gap:8px;margin:10px 0 8px}
  .tab{appearance:none;background:#0b1226;border:1px solid var(--border);border-radius:10px;padding:8px 12px;cursor:pointer;color:var(--ink)}
  .tab.active{background:#1a244d}
  .select{background:#0b1126;border:1px solid var(--border);color:var(--ink);padding:10px;border-radius:10px}
  .muted{color:var(--muted)}
  .chip{padding:6px 10px;border-radius:999px;border:1px solid var(--border);background:var(--chip);font-size:12px;display:inline-block;margin-right:6px}
  .mt{margin-top:10px}
  .hr{height:1px;background:var(--border);margin:14px 0}
  .twrap{overflow:auto}
  table{width:100%;border-collapse:collapse;font-size:14px}
  th,td{border-bottom:1px solid #141b3a;padding:8px 10px;text-align:left;vertical-align:top}
  thead th{position:sticky;top:0;background:#0e1530}
  /* Autocomplete */
  .ac{position:absolute;top:48px;left:0;right:0;background:#0b1126;border:1px solid var(--border);border-radius:12px;max-height:260px;overflow:auto;z-index:10;display:none}
  .ac-item{padding:10px 12px;cursor:pointer;border-bottom:1px solid rgba(255,255,255,.04)}
  .ac-item:last-child{border-bottom:none}
  .ac-item:hover{background:var(--hover)}
  .ac-item.active{background:var(--sel)}
  .ac-secondary{color:var(--muted);font-size:12px;margin-left:6px}
  /* separador de sección en la tabla */
  .sep th{background:transparent;color:var(--muted);font-size:12px;text-transform:uppercase;letter-spacing:.04em}
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

    <!-- UNA ESPECIE -->
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

    <!-- VARIAS -->
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

// -------- API KEY (se guarda en localStorage y se envía como header X-API-Key)
const keyParam = new URLSearchParams(location.search).get("key");
if(keyParam){ localStorage.setItem("api_key", keyParam); }
function headers(){ const k = localStorage.getItem("api_key"); return k ? {"X-API-Key": k} : {}; }
$("#saveKey").onclick = () => {
  const v = prompt("API key (X-API-Key):", localStorage.getItem("api_key") || "");
  if(v !== null){ localStorage.setItem("api_key", v.trim()); alert("Guardada."); }
};

// -------- TABS (corregido: quitamos bracket extra en p2)
function setTab(n){
  const t1 = $("#tab1"), t2 = $("#tab2"), p1 = $("#pane1"), p2 = $("#pane2");
  if(n===1){ t1.classList.add("active"); t2.classList.remove("active"); p1.style.display="";   p2.style.display="none"; $("#q1").focus(); }
  else    { t2.classList.add("active"); t1.classList.remove("active"); p2.style.display="";   p1.style.display="none"; $("#q2").focus(); }
}
$("#tab1").addEventListener("click", ()=>setTab(1));
$("#tab2").addEventListener("click", ()=>setTab(2));

// -------- AUTOCOMPLETE (GBIF suggest)
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

// -------- SINGLE: buscar detalle y pintar
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

    // badges
    $("#badges1").innerHTML = `
      <span class="chip">Estado: ${d.status ?? "-"}</span>
      <span class="chip">Rango: ${d.rank ?? "-"}</span>
      <span class="chip">GBIF: ${d.gbif_key ?? "-"}</span>
      <span class="chip">IUCN: ${d.iucn_category ?? "-"}</span>
    `;

    // ---- GRID en orden
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

    // -------- Sinónimos: mostrar lista o aviso si no hay
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

    // -------- Provenance (mantener bloque JSON)
    $("#prov1").innerHTML = "<pre style='white-space:pre-wrap'>" + JSON.stringify(d.provenance || {}, null, 2) + "</pre>";
  }catch(e){
    $("#msg1").textContent = "Error de red.";
  }
};

// -------- VARIAS: parseo entrada → bulk
function parseNames(raw){
  return Array.from(new Set(
    raw.split(/[\\n,;]+/).map(s => s.trim()).filter(Boolean)
  ));
}

$("#go2").onclick = async () => {
  const names = parseNames($("#q2").value);
  if(names.length === 0){ $("#msg2").textContent = "Escribe al menos un nombre."; return; }
  $("#msg2").textContent = "Procesando…";
  const modo = $("#modo").value;
  try{
    const r = await fetch("/reconciliar/bulk", {
      method: "POST",
      headers: Object.assign({"Content-Type":"application/json"}, headers()),
      body: JSON.stringify({names, modo, format:"json"})
    });
    if(r.status === 401){ $("#msg2").textContent = "No autorizado: configura tu API key."; return; }
    const j = await r.json();
    const rows = j.taxa || [];
    const syns = j.synonyms || [];
    const synByTaxon = {};
    syns.forEach(s => { const k = s.taxon_id; (synByTaxon[k] ||= []).push(s.name); });

    $("#rows").innerHTML = rows.map(rr => `
      <tr>
        <td>${rr.nombre_original ?? "-"}</td>
        <td>${rr.nombre_cientifico ?? rr.scientific_name ?? "-"}</td>
        <td>${rr.estado ?? rr.status ?? "-"}</td>
        <td>${rr.clave_gbif ?? rr.gbif_key ?? "-"}</td>
        <td>${rr.rango ?? rr.rank ?? "-"}</td>
        <td>${rr.categoria_iucn ?? rr.iucn_category ?? "-"}</td>
        <td>${(rr.fuentes || rr.sources_csv || "").toString()}</td>
        <td>${(synByTaxon[rr._taxon_id] || rr.synonyms_csv || []).toString()}</td>
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
      </tr>
    `).join("");
    $("#count").textContent = `Resultados: ${rows.length}`;
    $("#msg2").textContent = "";
  }catch(e){
    $("#msg2").textContent = "Error al procesar.";
  }
};

// -------- Archivo → /reconciliar/archivo
$("#file").onchange = () => {
  const f = $("#file").files[0];
  $("#fname").textContent = f ? f.name : "";
};
async function downloadBlob(url, method, body, filename){
  const r = await fetch(url, {method, body, headers: headers()});
  if(!r.ok){ alert("Error al generar archivo"); return; }
  const blob = await r.blob();
  const a = document.createElement("a");
  a.href = URL.createObjectURL(blob);
  a.download = filename;
  a.click();
  URL.revokeObjectURL(a.href);
}
$("#proc").onclick = async () => {
  const f = $("#file").files[0];
  if(!f){ alert("Primero selecciona un archivo."); return; }
  const fd = new FormData();
  fd.append("file", f);
  fd.append("output", "xlsx");
  fd.append("modo", $("#modo").value);
  fd.append("incluir_sinonimos", "true");
  await downloadBlob("/reconciliar/archivo", "POST", fd, "reconciliado.xlsx");
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
    """)

# ---------------- Main ----------------
if __name__ == "__main__":
    # En local: escuchar en 0.0.0.0 facilita probar desde otro dispositivo de la red
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
