# app/ingestors/aco_birds.py
from __future__ import annotations
from typing import Dict, List, Optional, Tuple
import os, re, unicodedata
import pandas as pd
from functools import lru_cache
from sqlalchemy.orm import Session
from .base import resolve_local_path, load_any_table, upsert_taxon

SOURCE = "ACO-birds"
# Ruta por defecto correcta al CSV dentro de la carpeta aco_birds
DEFAULT_PATH = os.getenv("ACO_BIRDS_PATH", "app/data/aco_birds/aco_birds.csv")

# candidatos para el nombre científico (es/en, con o sin acentos)
CANDIDATOS_SCI = {
    "nombre científico aceptado","nombre cientifico aceptado","nombre científico","nombre cientifico",
    "scientific_name","scientificname","nombre_cientifico","name"
}
GEN_KEYS = {"género","genero","genus"}
SP_KEYS  = {"especie","species","epíteto","epiteto","specific epithet","specific_epithet"}

# -------- normalización compatible con reconcile.normaliza_nombre --------
def _quitar_tildes(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))

def _norm_name(s: str) -> str:
    """
    - quita autores finales '(Autor, año)'
    - colapsa espacios
    - Género Capitalizado; resto minúsculas
    - elimina tildes
    """
    s = (s or "").strip()
    s = re.sub(r"\s*\([^)]*\)\s*$", "", s)
    s = re.sub(r"\s+", " ", s)
    if not s:
        return ""
    partes = s.split(" ")
    partes = [partes[0].capitalize()] + [p.lower() for p in partes[1:]]
    return _quitar_tildes(" ".join(partes))

def _armar_scientific(row: pd.Series, headers: List[str]) -> Optional[str]:
    # 1) columna directa
    for c in headers:
        if c.lower().strip() in CANDIDATOS_SCI and pd.notna(row.get(c)):
            v = str(row.get(c)).strip()
            if v:
                return v
    # 2) genus + species
    g = s = None
    for c in headers:
        lc = c.lower().strip()
        if lc in GEN_KEYS and pd.notna(row.get(c)) and not g:
            g = str(row.get(c)).strip()
        if lc in SP_KEYS and pd.notna(row.get(c)) and not s:
            s = str(row.get(c)).strip()
    if g and s:
        return f"{g} {s}".strip()
    return None

# --- NUEVO: detectar encabezado real embebido en las primeras filas ---
def _ensure_real_header(df: pd.DataFrame) -> pd.DataFrame:
    # Detecta la fila de encabezados reales dentro de las primeras 10 filas
    want = {"orden2022", "familia2022", "nombre 2022"}
    cols_lc = {str(c).strip().lower() for c in df.columns}
    if want.issubset(cols_lc):
        return df

    max_probe = min(10, len(df))
    for i in range(max_probe):
        row_lc = {str(x).strip().lower() for x in df.iloc[i].tolist()}
        if want.issubset(row_lc):
            new_cols = [str(x).strip() for x in df.iloc[i].tolist()]
            df2 = df.iloc[i+1:].copy()
            df2.columns = new_cols
            df2.reset_index(drop=True, inplace=True)
            return df2
    return df

def _normalize_headers(df: pd.DataFrame) -> pd.DataFrame:
    df = _ensure_real_header(df)  # <<< importante

    # mapeos suaves de cabeceras frecuentes (incluye columnas ACO 2022)
    rename: Dict[str, str] = {}
    for c in df.columns:
        lc = str(c).strip().lower()
        if lc in CANDIDATOS_SCI:                          rename[c] = "scientificName"
        elif lc in {"nombre 2022"}:                       rename[c] = "scientificName"
        elif lc in {"kingdom"}:                           rename[c] = "kingdom"
        elif lc in {"phylum"}:                            rename[c] = "phylum"
        elif lc in {"class","clase"}:                     rename[c] = "class"
        elif lc in {"order","orden","orden2022"}:         rename[c] = "order"
        elif lc in {"family","familia","familia2022"}:    rename[c] = "family"
        elif lc in {"genus","género","genero"}:           rename[c] = "genus"
        elif lc in {"taxonrank","rank","rango"}:          rename[c] = "taxonRank"
        elif lc in {"taxonomicstatus","status","estado","estado2022"}:
                                                          rename[c] = "taxonomicStatus"
        elif lc in {"specific epithet","specific_epithet","epíteto","epiteto","especie"}:
                                                          rename[c] = "specificEpithet"
    if rename:
        df = df.rename(columns=rename)
    return df

@lru_cache(maxsize=1)
def _df_index(path: str) -> pd.DataFrame:
    """
    Devuelve un DataFrame indexado por clave canónica '__key__'
    con columnas DWC mínimas para lookup/exists.
    Si no encuentra el CSV, devuelve DF vacío (exists -> False).
    """
    p = resolve_local_path(path)
    if not os.path.exists(p):
        # fallback antiguo por si alguien tenía el CSV en raíz /data
        alt = resolve_local_path("app/data/aco_birds.csv")
        if os.path.exists(alt):
            p = alt
        else:
            # DF vacío con índice válido
            return pd.DataFrame(columns=["__key__", "scientificName"]).set_index("__key__", drop=False)

    df, _ = load_any_table(p)          # lee csv/xlsx/tsv
    df = df.fillna("")
    df = _normalize_headers(df)
    # si no hay scientificName, intenta derivarlo
    if "scientificName" not in df.columns:
        df["scientificName"] = df.apply(lambda r: _armar_scientific(r, list(df.columns)) or "", axis=1)
    # clave canónica
    df["__key__"] = df["scientificName"].apply(_norm_name)
    # asegúrate de que existan columnas DWC esperadas
    for col in ["kingdom","phylum","class","order","family","genus","specificEpithet","taxonRank","taxonomicStatus"]:
        if col not in df.columns:
            df[col] = ""
    return df.set_index("__key__", drop=False)

# ---------- API pública para reconciliador / diagnósticos ----------

async def exists(nombre: str, path: Optional[str] = None) -> bool:
    dfi = _df_index(path or DEFAULT_PATH)
    return _norm_name(nombre) in dfi.index

async def lookup(nombre: str, path: Optional[str] = None) -> Dict:
    """
    Devuelve un dict con claves DWC para que reconcile las mapee:
      class -> class_name, order -> order_name, etc.
    """
    dfi = _df_index(path or DEFAULT_PATH)
    key = _norm_name(nombre)
    if key not in dfi.index:
        return {}
    r = dfi.loc[key]
    return {
        "scientificName": r.get("scientificName") or "",
        "kingdom": r.get("kingdom") or None,
        "phylum": r.get("phylum") or None,
        "class": r.get("class") or None,
        "order": r.get("order") or None,
        "family": r.get("family") or None,
        "genus": r.get("genus") or None,
        "specificEpithet": r.get("specificEpithet") or None,
        "taxonRank": r.get("taxonRank") or "SPECIES",
        "taxonomicStatus": r.get("taxonomicStatus") or "ACCEPTED",
        "_source": SOURCE,
    }

# alias por compatibilidad si en algún sitio llaman taxonomy()/get()/detail()
async def taxonomy(nombre: str, path: Optional[str] = None) -> Dict:
    return await lookup(nombre, path)

async def get(nombre: str, path: Optional[str] = None) -> Dict:
    return await lookup(nombre, path)

async def detail(nombre: str, path: Optional[str] = None) -> Dict:
    return await lookup(nombre, path)

# ---------- Ingesta a DB ----------

async def ingest(db: Session, url_or_path: str) -> Dict:
    path = resolve_local_path(url_or_path)
    df, _ = load_any_table(path)
    df = df.fillna("")
    df = _normalize_headers(df)

    headers = list(df.columns)
    inserted = 0; updated = 0; skipped = 0

    for _, r in df.iterrows():
        sci = _armar_scientific(r, headers)
        if not sci:
            skipped += 1
            continue

        row: Dict[str, str] = {
            "scientific_name": sci,
            "kingdom": r.get("kingdom") or None,
            "phylum": r.get("phylum") or None,
            "class_name": r.get("class") or None,
            "order_name": r.get("order") or None,
            "family": r.get("family") or None,
            "genus": r.get("genus") or None,
            "rank": r.get("taxonRank") or "SPECIES",
            "status": r.get("taxonomicStatus") or "ACCEPTED",
        }
        row = {k: v for k, v in row.items() if v is not None}

        # --- CONTEO ROBUSTO: maneja Taxon, (Taxon, created) o falsy ---
        res = upsert_taxon(db, row=row, source=SOURCE, synonyms=None)

        # Soporta distintas firmas de retorno:
        # - Taxon
        # - (Taxon, created: bool)
        # - None / False
        if isinstance(res, tuple):
            t, created = res[0], bool(res[1])
        else:
            t, created = res, None  # desconocido

        if t is not None and getattr(t, "id", None):
            if created is True:
                inserted += 1
            elif created is False:
                updated += 1
            else:
                # si no sabemos, cuenta como updated (no fue un fallo)
                updated += 1
        else:
            skipped += 1

    # refresca cache para futuras exists/lookup
    try:
        _df_index.cache_clear()
    except Exception:
        pass

    return {"rows": len(df), "inserted": inserted, "updated": updated, "skipped": skipped}
