# app/ingestors/sib_mammals_2024.py
from __future__ import annotations
from typing import Dict, Optional
import os, threading
import pandas as pd
from sqlalchemy.orm import Session
from .base import resolve_local_path, upsert_taxon

# === Etiquetas/Fuente ===
SOURCE_DB = "SIB-mammals-2024"   # así se guarda en DB via upsert_taxon
SOURCE_LABEL = "mamiferos_2024"  # así se mostrará/enriquecerá en reconcile

# === Config carga perezosa ===
# Si aún no llamaste /admin/ingest/mammals, intentaremos leer desde aquí:
MAMMALS_2024_PATH = os.getenv("MAMMALS_2024_PATH", "/mnt/data/taxon_mami.txt")

# === Índice en memoria (nombre normalizado -> dict taxonómico) ===
_LOCK = threading.Lock()
_LOADED = False
_IDX: Dict[str, Dict[str, str]] = {}

# ---------------- Utils ----------------
def _quitar_tildes(s: str) -> str:
    import unicodedata
    return "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))

def _norm_name(s: Optional[str]) -> str:
    """Primera palabra Capitalizada, resto minúsculas; quita autor entre paréntesis; colapsa espacios; sin tildes."""
    import re
    s = (s or "").strip()
    s = re.sub(r"\s*\([^)]*\)\s*$", "", s)    # quita "(Autor, año)" al final
    s = re.sub(r"\s+", " ", s)
    if not s:
        return ""
    parts = s.split(" ")
    parts = [parts[0].capitalize()] + [p.lower() for p in parts[1:]]
    return _quitar_tildes(" ".join(parts))

def _read_dwc_taxon(path: str) -> pd.DataFrame:
    # Esperado: taxonID, scientificName, kingdom, phylum, class, order, family, genus,
    # specificEpithet, taxonRank, taxonomicStatus, scientificNameAuthorship, subfamily, ...
    return pd.read_csv(path, sep="\t", engine="python", dtype=str).fillna("")

def _row_to_taxonomy(r: pd.Series) -> Dict[str, str]:
    """Mapea columnas Darwin Core -> claves internas que usa la DB/reconcile."""
    sci = (r.get("scientificName") or "").strip()
    # algunos archivos traen genus+specificEpithet sin scientificName completo
    if not sci and (r.get("genus") and r.get("specificEpithet")):
        sci = f"{r.get('genus')} {r.get('specificEpithet')}"
    out: Dict[str, str] = {
        "scientific_name": sci or None,
        "kingdom": r.get("kingdom") or None,
        "phylum": r.get("phylum") or None,
        "class_name": r.get("class") or None,
        "order_name": r.get("order") or None,
        "family": r.get("family") or None,
        "genus": r.get("genus") or None,
        "subfamily": r.get("subfamily") or None,
        "rank": r.get("taxonRank") or None,
        "status_local": r.get("taxonomicStatus") or None,
        "authorship": r.get("scientificNameAuthorship") or None,
        # Si tu fuente trae superfamily/subtribe/subgenus, puedes añadirlos aquí:
        # "superfamily": r.get("superfamily") or None,
        # "subtribe": r.get("subtribe") or None,
        # "subgenus": r.get("subgenus") or None,
    }
    # limpia vacíos:
    return {k: v for k, v in out.items() if v}

def _build_index_from_df(df: pd.DataFrame) -> None:
    global _IDX
    idx: Dict[str, Dict[str, str]] = {}
    for _, r in df.iterrows():
        tax = _row_to_taxonomy(r)
        key = _norm_name(tax.get("scientific_name"))
        if key:
            idx[key] = tax
    _IDX = idx

def _load_once_from_path(path: Optional[str] = None) -> None:
    """Carga el índice si no está listo. Intenta leer MAMMALS_2024_PATH."""
    global _LOADED
    if _LOADED:
        return
    with _LOCK:
        if _LOADED:
            return
        p = path or MAMMALS_2024_PATH
        try:
            if p and os.path.exists(p):
                df = _read_dwc_taxon(p)
                _build_index_from_df(df)
        finally:
            _LOADED = True

# --------------- API pública para reconcile (presencia + enriquecimiento) ---------------
def exists(name: str) -> bool:
    _load_once_from_path()
    return _norm_name(name) in _IDX

def get_taxonomy(name: str) -> Dict[str, str] | None:
    """Devuelve el bloque interno (con class_name/order_name)."""
    _load_once_from_path()
    return _IDX.get(_norm_name(name))

# --- NUEVO: lookup() estilo DWC para que reconcile pueda enriquecer ---
async def lookup(nombre: str, path: Optional[str] = None) -> Dict:
    """
    Responde con claves DWC ('class','order','family','genus', ...) para un nombre puntual.
    El reconciliador ya mapea class→class_name, order→order_name con _taxonomy_from_generic().
    """
    _load_once_from_path(path)
    tax = _IDX.get(_norm_name(nombre)) or {}
    if not tax:
        return {}

    # Convertimos de nuestras claves internas → DWC esperado por reconcile
    return {
        "scientificName": tax.get("scientific_name") or "",
        "kingdom": tax.get("kingdom"),
        "phylum": tax.get("phylum"),
        "class": tax.get("class_name"),     # ← importante: DWC 'class'
        "order": tax.get("order_name"),     # ← importante: DWC 'order'
        "family": tax.get("family"),
        "genus": tax.get("genus"),
        "subfamily": tax.get("subfamily"),
        "taxonRank": tax.get("rank"),
        "taxonomicStatus": (tax.get("status_local") or tax.get("status")),
        # añade superfamily/subtribe/subgenus si las manejas en la ingesta
        # "superfamily": tax.get("superfamily"),
        # "subtribe": tax.get("subtribe"),
        # "subgenus": tax.get("subgenus"),
    }

# alias que el reconciliador también intenta
async def taxonomy(nombre: str, path: Optional[str] = None) -> Dict:
    return await lookup(nombre, path)

async def get(nombre: str, path: Optional[str] = None) -> Dict:
    return await lookup(nombre, path)

async def detail(nombre: str, path: Optional[str] = None) -> Dict:
    return await lookup(nombre, path)

def refresh_index_from_path(path: str) -> None:
    """Recarga el índice manualmente (útil tras una nueva ingesta)."""
    global _LOADED
    with _LOCK:
        df = _read_dwc_taxon(path)
        _build_index_from_df(df)
        _LOADED = True

# --------------- Ingesta principal ---------------
async def ingest(db: Session, url_or_path: str) -> Dict:
    """
    Lee el listado DWC (CSV/TSV), hace upsert en tu tabla y refresca el índice en memoria.
    """
    path = resolve_local_path(url_or_path)
    df = _read_dwc_taxon(path)

    inserted = 0; skipped = 0
    for _, r in df.iterrows():
        sci = (r.get("scientificName") or "").strip()
        if not sci and (r.get("genus") and r.get("specificEpithet")):
            sci = f"{r.get('genus')} {r.get('specificEpithet')}"
        if not sci:
            skipped += 1
            continue

        row = {
            "scientific_name": sci,
            "kingdom": r.get("kingdom") or None,
            "phylum": r.get("phylum") or None,
            "class_name": r.get("class") or None,
            "order_name": r.get("order") or None,
            "family": r.get("family") or None,
            "genus": r.get("genus") or None,
            "subfamily": r.get("subfamily") or None,
            "rank": r.get("taxonRank") or None,
            "status": r.get("taxonomicStatus") or None,
            "authorship": r.get("scientificNameAuthorship") or None,
        }
        row = {k: v for k, v in row.items() if v}

        # Guarda en tu DB con etiqueta de fuente
        upsert_taxon(db, row=row, source=SOURCE_DB, synonyms=None)
        inserted += 1

    # refresca índice in-memory para reconcile
    refresh_index_from_path(path)

    return {"rows": len(df), "inserted": inserted, "skipped": skipped, "source": SOURCE_DB}
