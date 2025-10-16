# app/ingestors/base.py
from __future__ import annotations
import os, io
from typing import Dict, Iterable, Tuple, List
from urllib.parse import urlparse, unquote
import pandas as pd
from sqlalchemy.orm import Session
from app.models import Taxon, Synonym

# ---------- Paths ----------

def resolve_local_path(url_or_path: str) -> str:
    """Acepta:
       - file:///C:/ruta/archivo.csv
       - C:\ruta\archivo.csv
       - C:/ruta/archivo.csv
    y devuelve una ruta válida de Windows.
    """
    s = url_or_path.strip()
    if s.lower().startswith("file://"):
        p = urlparse(s)
        # p.path viene /C:/... → quitar el "/" inicial si es letra de unidad
        path = unquote(p.path or "")
        if os.name == "nt" and len(path) >= 3 and path[0] == "/" and path[1].isalpha() and path[2] == ":":
            path = path[1:]
        return path
    return s

# ---------- Lectores ----------

def load_any_table(url_or_path: str) -> Tuple[pd.DataFrame, List[str]]:
    """
    Lee CSV/TSV/Excel. Devuelve (df, headers_en_minusculas).
    - CSV/TSV: autodetecta sep por , ; o \t
    - Excel: toma la primera hoja
    """
    p = resolve_local_path(url_or_path)
    lower = p.lower()
    if lower.endswith(".xlsx") or lower.endswith(".xls"):
        df = pd.read_excel(p)
    else:
        # intentamos ; luego , luego \t
        try:
            df = pd.read_csv(p, sep=";", engine="python")
        except Exception:
            try:
                df = pd.read_csv(p)
            except Exception:
                df = pd.read_csv(p, sep="\t", engine="python")
    # columnas normalizadas
    norm_cols = [str(c).strip() for c in df.columns]
    return df, [c.lower() for c in norm_cols]

# ---------- Upsert Taxon + sinónimos ----------

def _get(d: Dict, *keys: str) -> str | None:
    for k in keys:
        if k in d and d[k]:
            return str(d[k]).strip()
    return None

def upsert_taxon(
    db: Session,
    row: Dict[str, str],
    source: str = "",
    synonyms: List[str] | None = None,
) -> Taxon:
    """
    `row` DEBE traer 'scientific_name' (en minúsculas o como clave exacta).
    Acepta además cualquiera de estos campos (si vienen, se guardan):
    canonical_name, authorship, rank, kingdom, phylum, class_name, order_name,
    superfamily, family, subfamily, tribe, subtribe, genus, subgenus, status,
    iucn_category, gbif_key, accepted_gbif_key, sources_csv
    """
    # normalizar claves -> minúsculas
    r = {str(k).strip().lower(): (None if pd.isna(v) else (str(v).strip())) for k, v in row.items()}
    sci = _get(r, "scientific_name", "scientificname", "nombre_cientifico")
    if not sci:
        raise ValueError("La fila no trae 'scientific_name' (o alias mapeado).")

    # buscar existente por scientific_name
    t = db.query(Taxon).filter(Taxon.scientific_name == sci).first()
    created = False
    if not t:
        t = Taxon(scientific_name=sci)
        created = True

    # mapeo simple
    for col in (
        "canonical_name","authorship","rank","kingdom","phylum","class_name","order_name",
        "superfamily","family","subfamily","tribe","subtribe","genus","subgenus",
        "status","iucn_category",
    ):
        val = r.get(col)
        if val:
            setattr(t, col, val)

    # opcionales de GBIF/fuentes
    if r.get("gbif_key"): t.gbif_key = r["gbif_key"]
    if r.get("accepted_gbif_key"): t.accepted_gbif_key = r["accepted_gbif_key"]

    # fuentes
    if source:
        s = (t.sources_csv or "").split(",") if t.sources_csv else []
        if source not in s:
            s.append(source)
        t.sources_csv = ",".join(x for x in s if x)

    if created:
        db.add(t)
        db.flush()  # para tener id

    # sinónimos
    if synonyms:
        for name in synonyms:
            name = (name or "").strip()
            if not name:
                continue
            # evitar duplicados simples por (taxon_id, name)
            exists = (
                db.query(Synonym)
                .filter(Synonym.taxon_id == t.id, Synonym.name == name)
                .first()
            )
            if not exists:
                db.add(Synonym(
                    taxon_id=t.id,
                    name=name,
                    source=source or "ingestor",
                ))

    db.commit()
    db.refresh(t)
    return t
