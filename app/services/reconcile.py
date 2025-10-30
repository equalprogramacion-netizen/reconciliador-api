# app/services/reconcile.py
from __future__ import annotations

from typing import List, Optional, Dict, Any, Tuple, Callable, Awaitable
from sqlalchemy.orm import Session
from sqlalchemy import select, delete
from sqlalchemy.exc import SQLAlchemyError
import re, unicodedata, asyncio, json
import httpx
from collections import OrderedDict
import logging
from email.utils import parsedate_to_datetime
from datetime import datetime, timezone
import os  # <<< NUEVO: permite configurar el tamaño del caché por env var

from ..models import Taxon, Synonym
from ..clients import gbif, col  # iucn se importa lazy en _iucn_category_safe

# --------------------- Logger ---------------------
log = logging.getLogger(__name__)

# --------------------- Concurrencia, backoff y caching ---------------------
_SEM = asyncio.Semaphore(8)  # límite de peticiones simultáneas

_http_client: httpx.AsyncClient | None = None

async def _get_http_client() -> httpx.AsyncClient:
    """Cliente httpx global con HTTP/2 y límites de pool."""
    global _http_client
    if _http_client is None:
        _http_client = httpx.AsyncClient(
            http2=True,
            limits=httpx.Limits(
                max_connections=40,
                max_keepalive_connections=20,
            ),
        )
    return _http_client

async def close_http_client():
    """Llamar en el shutdown global (ej. FastAPI on_event('shutdown')) para cerrar el pool."""
    global _http_client
    if _http_client is not None:
        await _http_client.aclose()
        _http_client = None

async def fetch_json(url: str, params: dict | None = None, timeout=12, retries=2) -> dict:
    """HTTP GET con backoff exponencial y límite de concurrencia (429/5xx se reintentan)."""
    backoff = 0.6
    async with _SEM:
        for i in range(retries + 1):
            try:
                client = await _get_http_client()
                r = await client.get(url, params=params, timeout=timeout)
                if r.status_code == 429 or 500 <= r.status_code < 600:
                    raise httpx.HTTPStatusError("retryable", request=r.request, response=r)
                if r.status_code >= 400:
                    return {}
                try:
                    return r.json()
                except ValueError:
                    # payload no-JSON: degradar a dict vacío
                    return {}
            except httpx.HTTPError as e:  # captura amplia: incluye timeouts
                if i == retries:
                    if log.isEnabledFor(logging.DEBUG):
                        log.debug("fetch_json agotó reintentos %s params=%s: %s", url, params, e)
                    return {}
                # Respetar Retry-After si viene (segundos o HTTP-date)
                wait = backoff
                try:
                    ra = getattr(e, "response", None).headers.get("Retry-After") if getattr(e, "response", None) else None
                    if ra:
                        try:
                            wait = float(ra)
                        except ValueError:
                            try:
                                dt = parsedate_to_datetime(ra)
                                if not dt.tzinfo:
                                    dt = dt.replace(tzinfo=timezone.utc)
                                wait = max(0.0, (dt - datetime.now(timezone.utc)).total_seconds())
                            except Exception:
                                pass
                except Exception:
                    pass
                await asyncio.sleep(wait)
                backoff *= 2

# --------- LRU cache muy simple para (provider, normalized_name) -> payload ---------
# <<< CAMBIO: configurable por variable de entorno ASYNC_CACHE_MAX >>>
_MAX_ASYNC_CACHE = int(os.getenv("ASYNC_CACHE_MAX", "10000"))
_async_cache: "OrderedDict[Tuple[str, str], Any]" = OrderedDict()
_cache_lock = asyncio.Lock()

def _lru_get(key: Tuple[str, str]):
    if key in _async_cache:
        _async_cache.move_to_end(key)
        return _async_cache[key]
    return None

def _lru_put(key: Tuple[str, str], value: Any):
    _async_cache[key] = value
    _async_cache.move_to_end(key)
    if len(_async_cache) > _MAX_ASYNC_CACHE:
        _async_cache.popitem(last=False)

# --- normaliza la clave para maximizar hits entre variantes (acentos/caso/autores) ---
async def _cached(provider: str, name: str, coro_factory: Callable[[], Awaitable[Any]]) -> Any:
    """Cache asíncrono LRU por proveedor/nombre normalizado (con lock)."""
    key = (provider, _canon(name))
    async with _cache_lock:
        hit = _lru_get(key)
    if hit is not None:
        return hit
    res = await coro_factory()
    async with _cache_lock:
        _lru_put(key, res)
    return res

# --------------------- Utilidades de normalización ---------------------
def _quitar_tildes(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))

def normaliza_nombre(q: str) -> str:
    """
    - elimina autores finales "(Linnaeus, 1758)"
    - colapsa espacios
    - Género Capitalizado; resto minúsculas
    - elimina acentos (para comparaciones)
    """
    q = (q or "").strip()
    q = re.sub(r"\s*\([^)]*\)\s*$", "", q)
    q = re.sub(r"\s+", " ", q)
    if not q:
        return q
    partes = q.split(" ")
    partes = [partes[0].capitalize()] + [p.lower() for p in partes[1:]]
    return _quitar_tildes(" ".join(partes))

def _canon(s: Optional[str]) -> str:
    """Normaliza un nombre científico para comparaciones (sin autores, acentos, espacios extras)."""
    return normaliza_nombre(s or "")

def obtener_epiteto_especifico(canonical_or_scientific: Optional[str], rank: Optional[str]) -> Optional[str]:
    if not canonical_or_scientific or (rank and str(rank).upper() != "SPECIES"):
        return None
    partes = (canonical_or_scientific or "").split()
    return partes[-1].lower() if len(partes) >= 2 else None

def _norm_taxon_value(v: str | None) -> str | None:
    """
    Normalizador ligero para valores taxonómicos (orden/clase/etc.).
    Mapas incluyen clados usados erróneamente como clases por algunas fuentes.
    """
    if not v:
        return v
    m = {
        "Crocodilia": "Crocodylia",        # normaliza a 'y'
        "Teleostei": "Actinopterygii",     # clado -> clase
        "Elasmobranchii": "Chondrichthyes",# subclase/clado -> clase
        # capitalización comunes (solo si llegan exactos)
        "mammalia": "Mammalia",
        "aves": "Aves",
        "amphibia": "Amphibia",
        "reptilia": "Reptilia",
    }
    s = str(v).strip()
    return m.get(s, s)

# --- Mapa orden→clase para sanity fixes ---
ORDER_TO_CLASS = {
    "squamata": "Reptilia",
    "crocodylia": "Reptilia",
    "crocodilia": "Reptilia",
    "testudines": "Reptilia",
    "anura": "Amphibia",
    "caudata": "Amphibia",
    # tiburones y rayas
    "carcharhiniformes": "Chondrichthyes",
    "lamniformes": "Chondrichthyes",
    "squaliformes": "Chondrichthyes",
    "rajiformes": "Chondrichthyes",
    "myliobatiformes": "Chondrichthyes",
    "chimaeriformes": "Chondrichthyes",
    # peces óseos comunes
    "perciformes": "Actinopterygii",
    "pomacentriformes": "Actinopterygii",
    # fósiles/otros
    "ichthyosauria": "Reptilia",
    # --- mamíferos y aves (extras seguros) ---
    "artiodactyla": "Mammalia",
    "perissodactyla": "Mammalia",
    "rodentia": "Mammalia",
    "chiroptera": "Mammalia",
    "passeriformes": "Aves",
    "accipitriformes": "Aves",
    "charadriiformes": "Aves",
    "columbiformes": "Aves",
    "piciformes": "Aves",
}

def _fix_class_order(base: dict, prov: dict, itis_hint: dict | None = None):
    """
    Repara incoherencias clase/orden (p. ej. class_name == order_name),
    prefiriendo la pista de ITIS si está disponible.
    También corrige cuando la "clase" viene con un clado común (Teleostei/Elasmobranchii).
    """
    o = (base.get("order_name") or "").strip()
    c = (base.get("class_name") or "").strip()
    itis_class = (itis_hint or {}).get("class_name") if isinstance(itis_hint, dict) else None

    # 1) clase ausente o igual al orden -> intenta inferir
    if not c or (o and c.lower() == o.lower()):
        fix = itis_class or ORDER_TO_CLASS.get(o.lower())
        if fix and fix != c:
            _set_if(base, prov, "class_name", fix, "ITIS" if itis_class else "RULE")

    # 2) clase viene como clado frecuente -> remapea
    c2 = (base.get("class_name") or "").strip().lower()
    if c2 in ("teleostei", "elasmobranchii"):
        target = "actinopterygii" if c2 == "teleostei" else "chondrichthyes"
        if c2 != target:
            _set_if(base, prov, "class_name", target.capitalize(), "RULE:clado→clase")

# ----------------------- Avisos / helpers de fuentes -----------------------
UNCERTAIN_STATUSES = {"DOUBTFUL", "UNCERTAIN", "PROVISIONAL"}

def _build_warnings(base: dict, provenance: dict) -> list[str]:
    warns: list[str] = []

    # 1) Campos inferidos por regla (admite "RULE" o prefijo "RULE:*")
    rule_fields = sorted(
        f for f, src in (provenance or {}).items()
        if isinstance(src, str) and src.upper().startswith("RULE") and base.get(f)
    )
    if rule_fields:
        warns.append(
            "Campos inferidos por regla interna sin respaldo explícito de fuente: "
            + ", ".join(rule_fields) + ". Recomendado validar con GBIF/ITIS/CoL/WoRMS."
        )

    # 2) Estatus dudoso
    st = (base.get("status") or "").upper()
    if st in UNCERTAIN_STATUSES:
        warns.append(f"El estatus del taxón está marcado como {st} en GBIF; revisar aceptación/sinonimia.")

    # 3) Sin clave GBIF (registro mínimo)
    if not base.get("gbif_key"):
        warns.append("No se obtuvo usageKey de GBIF; se guardó un registro mínimo con fuentes alternativas.")

    return warns

def _collapse_base(src: str) -> str:
    s = src.split(":", 1)[0] if ":" in src else src
    return {"CoL": "Catalogue of Life"}.get(s, s)

def _collect_sources(prov: dict | None) -> list[str]:
    """
    Extrae etiquetas de fuente desde provenance sin romper si hay dicts/listas.
    Normaliza 'PREFERRED:Fuente' -> 'Fuente'. Filtra RULE*. Colapsa prefijos "X:detalle".
    Soporta etiquetas compuestas separadas por comas.
    """
    if not isinstance(prov, dict):
        return []
    raw: list[str] = []
    for k, v in prov.items():
        if isinstance(v, str) and v:
            raw.append(v)
        elif k == "_overrides" and isinstance(v, dict):
            for ov in v.values():
                if isinstance(ov, dict):
                    frm = ov.get("from")
                    to = ov.get("to")
                    if isinstance(frm, str) and frm:
                        raw.append(frm)
                    if isinstance(to, str) and to:
                        raw.append(to)
    cleaned: set[str] = set()
    for s in raw:
        if s.startswith("PREFERRED:"):
            s = s.split(":", 1)[1]
        if s.startswith("RULE"):
            continue  # no meter reglas en fuentes
        # partir por comas y normalizar cada parte
        for part in (p.strip() for p in s.split(",") if p.strip()):
            cleaned.add(_collapse_base(part))
    return sorted(cleaned)

# Helper global para normalizar etiquetas de fuente
def _norm_src_label(x: str) -> Optional[str]:
    if not x:
        return None
    if x.startswith("RULE"):
        return None
    # colapsar "Fuente:subtag" a "Fuente" (ej. "GBIF:detail" → "GBIF")
    if ":" in x:
        x = x.split(":", 1)[0]
    alias = {
        "CoL": "Catalogue of Life",
        "reptile_db": "Reptile Database",
        "batrachia": "Batrachia",
        "cites_public": "CITES (Species+)",
        "aco_aves": "ACO Aves",
        "mamiferos_2024": "Mamíferos 2024 (SIB)",
        "SIB": "SIB Colombia",
        "sib": "SIB Colombia",
        "SIB-Colombia": "SIB Colombia",
    }
    return alias.get(x, x)

# ----------------------- NUEVO: whitelist de fuentes por clase -----------------------
ALLOWED_BY_CLASS: Dict[str, set[str]] = {
    "Aves": {"Catalogue of Life", "ITIS", "GBIF", "ACO Aves", "SIB Colombia", "CITES (Species+)"},
    "Mammalia": {"Catalogue of Life", "ITIS", "GBIF", "SIB Colombia", "Mamíferos 2024 (SIB)", "CITES (Species+)"},
    "Reptilia": {"Catalogue of Life", "ITIS", "GBIF", "Reptile Database", "SIB Colombia", "CITES (Species+)"},
    "Amphibia": {"Catalogue of Life", "ITIS", "GBIF", "Batrachia", "SIB Colombia", "CITES (Species+)"},
    # Fallback si no se conoce la clase
    "*": {"Catalogue of Life", "ITIS", "GBIF", "SIB Colombia", "CITES (Species+)"},
}

def filter_sources_by_class(found: List[str], class_name: Optional[str]) -> List[str]:
    """
    Filtra etiquetas de presencia según la clase resuelta.
    - Normaliza etiquetas con _norm_src_label
    - Aplica ALLOWED_BY_CLASS usando la clase (o '*' si no hay)
    """
    allowed = ALLOWED_BY_CLASS.get(class_name or "", ALLOWED_BY_CLASS["*"])
    normed = [_norm_src_label(s) for s in (found or [])]
    normed = [s for s in normed if s]  # quita None
    return [s for s in normed if s in allowed]

# ----------------------- Campos que intentaremos completar -----------------------
TAXO_FIELDS = [
    "kingdom", "phylum", "class_name", "order_name",
    "superfamily", "family", "subfamily", "tribe", "subtribe",
    "genus", "subgenus",
]

# mapeos ad-hoc por fuente → nuestros nombres de columna
MAP_WRMS = {
    "kingdom":"kingdom","phylum":"phylum","class":"class_name","order":"order_name",
    "superfamily":"superfamily","family":"family","subfamily":"subfamily",
    "tribe":"tribe","subtribe":"subtribe","genus":"genus","subgenus":"subgenus",
}
MAP_COL = {  # CoL usa "class" y "order" (no *_name)
    "kingdom":"kingdom","phylum":"phylum","class":"class_name","order":"order_name",
    "superfamily":"superfamily","family":"family","subfamily":"subfamily",
    "tribe":"tribe","subtribe":"subtribe","genus":"genus","subgenus":"subgenus",
}
MAP_ITIS = {
    # ITIS trae jerarquía por "rankName" → "taxonName"
    "kingdom":"kingdom","phylum":"phylum","class":"class_name","order":"order_name",
    "superfamily":"superfamily","family":"family","subfamily":"subfamily",
    "tribe":"tribe","subtribe":"subtribe","genus":"genus","subgenus":"subgenus",
}

# --- Mapeo genérico DWC/local → columnas internas ---
MAP_DWC_LOCAL = {
    "kingdom": "kingdom",
    "phylum": "phylum",
    "class": "class_name",
    "order": "order_name",
    "superfamily": "superfamily",
    "family": "family",
    "subfamily": "subfamily",
    "tribe": "tribe",
    "subtribe": "subtribe",
    "genus": "genus",
    "subgenus": "subgenus",
    # otros DWC ignorados para columnas internas
    "specificEpithet": None,
    "infraspecificEpithet": None,
    "taxonRank": None,
    "taxonomicStatus": None,
}

# ----------------------- Helpers genéricos de taxonomía -----------------------
def _taxonomy_from_generic(item: dict, mapping: dict[str, str|None] = MAP_DWC_LOCAL) -> dict:
    """Mapea campos de item → nuestros nombres, normalizando valores."""
    out: Dict[str, Any] = {k: None for k in TAXO_FIELDS}
    if not isinstance(item, dict):
        return out
    for src, dst in mapping.items():
        if not dst:
            continue
        v = item.get(src)
        if v:
            out[dst] = _norm_taxon_value(v)
    return out

async def _safe_tax(label: str, coro) -> tuple[str, dict]:
    """Envoltorio seguro: devuelve (label, dict_tax) o (label, {})."""
    try:
        d = await coro
        if isinstance(d, dict):
            return (label, d)
    except Exception as e:
        if log.isEnabledFor(logging.DEBUG):
            log.debug("Extractor %s falló: %s", label, e)
    return (label, {})

# ---------- extractores por fuente (flexibles) ----------
async def _tax_from_reptile_db(nombre: str) -> dict:
    try:
        from ..clients import reptile_db
        for fn in ("taxonomy", "get", "fetch", "detail", "lookup"):
            f = getattr(reptile_db, fn, None)
            if callable(f):
                raw = await f(nombre)
                if isinstance(raw, dict) and raw:
                    m = {
                        "kingdom": "kingdom", "phylum": "phylum",
                        "class": "class_name", "order": "order_name",
                        "superfamily": "superfamily", "family": "family",
                        "subfamily": "subfamily", "tribe": "tribe", "subtribe": "subtribe",
                        "genus": "genus", "subgenus": "subgenus",
                        "class_name": "class_name", "order_name": "order_name"
                    }
                    return _taxonomy_from_generic(raw, m)
    except Exception as e:
        if log.isEnabledFor(logging.DEBUG):
            log.debug("Reptile DB falló: %s", e)
    return {}

async def _tax_from_batrachia(nombre: str) -> dict:
    try:
        from ..clients import batrachia
        for fn in ("taxonomy", "get", "fetch", "detail", "lookup"):
            f = getattr(batrachia, fn, None)
            if callable(f):
                raw = await f(nombre)
                if isinstance(raw, dict) and raw:
                    m = {
                        "kingdom": "kingdom", "phylum": "phylum",
                        "class": "class_name", "order": "order_name",
                        "superfamily": "superfamily", "family": "family",
                        "subfamily": "subfamily", "tribe": "tribe", "subtribe": "subtribe",
                        "genus": "genus", "subgenus": "subgenus",
                        "class_name": "class_name", "order_name": "order_name"
                    }
                    return _taxonomy_from_generic(raw, m)
    except Exception as e:
        if log.isEnabledFor(logging.DEBUG):
            log.debug("Batrachia falló: %s", e)
    return {}

async def _tax_from_cites(nombre: str) -> dict:
    """CITES suele no aportar jerarquía completa; intentamos lo que haya."""
    try:
        from ..clients import cites_public
        for fn in ("taxonomy", "get", "fetch", "detail", "lookup"):
            f = getattr(cites_public, fn, None)
            if callable(f):
                raw = await f(nombre)
                if isinstance(raw, dict) and raw:
                    m = {
                        "class": "class_name", "order": "order_name",
                        "family": "family", "genus": "genus",
                    }
                    return _taxonomy_from_generic(raw, m)
    except Exception as e:
        if log.isEnabledFor(logging.DEBUG):
            log.debug("CITES falló: %s", e)
    return {}

async def _tax_from_aco_birds(nombre: str) -> dict:
    """De la ingesta local de ACO: intenta exponer jerarquía."""
    try:
        from ..ingestors import aco_birds as ing_aco
        for fn in ("taxonomy", "get", "lookup", "detail"):
            f = getattr(ing_aco, fn, None)
            if callable(f):
                raw = await f(nombre)
                if isinstance(raw, dict) and raw:
                    m = {
                        "kingdom": "kingdom", "phylum": "phylum",
                        "class": "class_name", "order": "order_name",
                        "superfamily": "superfamily", "family": "family",
                        "subfamily": "subfamily", "tribe": "tribe",
                        "genus": "genus", "subgenus": "subgenus",
                        "class_name": "class_name", "order_name": "order_name"
                    }
                    return _taxonomy_from_generic(raw, m)
    except Exception as e:
        if log.isEnabledFor(logging.DEBUG):
            log.debug("ACO Birds falló: %s", e)
    return {}

async def _tax_from_mammals_2024(nombre: str) -> dict:
    """De la ingesta local de Mamíferos 2024 SIB: intenta exponer jerarquía."""
    try:
        from ..ingestors import sib_mammals_2024 as ing_mam
        for fn in ("taxonomy", "get", "lookup", "detail"):
            f = getattr(ing_mam, fn, None)
            if callable(f):
                raw = await f(nombre)
                if isinstance(raw, dict) and raw:
                    m = {
                        "kingdom": "kingdom", "phylum": "phylum",
                        "class": "class_name", "order": "order_name",
                        "superfamily": "superfamily", "family": "family",
                        "subfamily": "subfamily", "tribe": "tribe",
                        "genus": "genus", "subgenus": "subgenus",
                        "class_name": "class_name", "order_name": "order_name"
                    }
                    return _taxonomy_from_generic(raw, m)
    except Exception as e:
        if log.isEnabledFor(logging.DEBUG):
            log.debug("Mamíferos 2024 falló: %s", e)
    return {}

async def _tax_from_sib(nombre: str) -> dict:
    """Extractor simple del SIB (general) desde el primer match."""
    try:
        from ..clients import sib
        j = await sib.record_search(nombre, size=1)

        items = None
        if isinstance(j, dict):
            for k in ("results", "data", "records", "items"):
                if isinstance(j.get(k), list) and j[k]:
                    items = j[k]
                    break
        elif isinstance(j, list):
            items = j

        if not items:
            return {}

        it = items[0] or {}
        m = {
            "kingdom": "kingdom", "phylum": "phylum",
            "class": "class_name", "order": "order_name",
            "superfamily": "superfamily", "family": "family",
            "subfamily": "subfamily", "tribe": "tribe", "subtribe": "subtribe",
            "genus": "genus", "subgenus": "subgenus",
        }
        out = {k: None for k in TAXO_FIELDS}
        for src, dst in m.items():
            v = it.get(src)
            if v:
                out[dst] = _norm_taxon_value(v)
        return out
    except Exception as e:
        if log.isEnabledFor(logging.DEBUG):
            log.debug("SIB general falló: %s", e)
        return {}

# ----------------------------- Compatibilidad de clasificación (A) -----------------------------
def _compatible_taxonomy(base: Dict[str, Any], cand: Dict[str, Any]) -> bool:
    """
    Coincidencias en >=2 (phylum, class, order, family, genus)
    PERO si ambos traen family y difiere, no es compatible.
    Además, exigimos match en family o genus cuando ambos existan.
    Para rank GENUS, permitimos compatibilidad si coincide family (1 match).
    """
    def norm(x):
        s = _norm_taxon_value(x)
        return (s or "").strip().lower() or None

    b = {
        "phylum": norm(base.get("phylum")),
        "class":  norm(base.get("class_name")),
        "order":  norm(base.get("order_name")),
        "family": norm(base.get("family")),
        "genus":  norm(base.get("genus")),
    }
    c = {
        "phylum": norm(cand.get("phylum")),
        "class":  norm(cand.get("class") or cand.get("class_name")),
        "order":  norm(cand.get("order") or cand.get("order_name")),
        "family": norm(cand.get("family")),
        "genus":  norm(cand.get("genus")),
    }

    if b["family"] and c["family"] and b["family"] != c["family"]:
        return False

    must = []
    if b["family"] and c["family"]:
        must.append(b["family"] == c["family"])
    if b["genus"] and c["genus"]:
        must.append(b["genus"] == c["genus"])
    if must and not any(must):
        return False

    matches = sum(1 for k in ("phylum","class","order","family","genus") if b[k] and c[k] and b[k] == c[k])

    if (base.get("rank") or "").upper() == "GENUS":
        return (b["family"] and c["family"] and b["family"] == c["family"]) or matches >= 2

    return matches >= 2

# ----------------------------- Lectores por fuente -----------------------------
async def _col_best(nombre: str, base_for_compat: Dict[str, Any] | None = None) -> Dict[str, Any] | None:
    """
    Selecciona el mejor item de CoL con la siguiente prioridad:
      1) Exact match del canónico con el normalizado.
      2) status=accepted, o si no es aceptado, que su accepted/acceptedName apunte al canónico exacto.
      3) Compatibilidad con GBIF (≥2 coincidencias entre phylum/clase/orden/familia/género).
      4) Si no hay exactos, elegir el primer aceptado compatible.
    """
    try:
        data = await _cached("col.search", nombre, lambda: col.search_name(nombre))
        if not isinstance(data, dict):
            return None
        items = data.get("result") or data.get("results") or data.get("items") or []
        if not items:
            return None

        # Enriquecer con detalle para tener 'classification'
        enriched: list[dict] = []
        for it in items[:5]:
            usage_id = it.get("id") or it.get("usageId") or it.get("nameUsageId")
            detail = None
            if usage_id:
                try:
                    # TODO: si existe un endpoint público estable, reemplazar este método privado
                    detail = await _cached(f"col.detail:{usage_id}", str(usage_id), lambda: col._detail_for_usage(usage_id))  # type: ignore[attr-defined]
                except Exception as e:
                    if log.isEnabledFor(logging.DEBUG):
                        log.debug("CoL detail fallo id=%s: %s", usage_id, e)
                    detail = None
            merged = dict(it)
            if isinstance(detail, dict) and detail:
                if detail.get("classification"):
                    merged["classification"] = detail["classification"]
                for k in ("usage", "accepted", "acceptedName", "authorship", "author", "labelAuthorship", "status"):
                    if k in detail and detail[k] is not None:
                        merged[k] = detail[k]
            enriched.append(merged)

        target = _canon(nombre)

        def item_canonical(x: dict) -> str:
            u = x.get("usage") or {}
            nm = (u.get("name") or {})
            for key in ("canonicalName", "scientificName", "name"):
                v = nm.get(key) if isinstance(nm, dict) else None
                if v:
                    return _canon(v)
            for key in ("canonicalName", "scientificName", "name", "label"):
                v = x.get(key)
                if v:
                    return _canon(v)
            return ""

        def accepted_canonical(x: dict) -> str:
            acc = x.get("acceptedName") or x.get("accepted") or (x.get("usage") or {}).get("accepted")
            if isinstance(acc, dict):
                nm = acc.get("name") or acc
                if isinstance(nm, dict):
                    for key in ("canonicalName", "scientificName", "name", "label"):
                        v = nm.get(key)
                        if v:
                            return _canon(v)
                else:
                    return _canon(str(nm))
            return ""

        def is_accepted(x: dict) -> bool:
            s = (x.get("status") or (x.get("usage") or {}).get("status") or "").strip().lower()
            return s == "accepted"

        def taxo(x: dict) -> dict:
            return _taxonomy_from_col(x)

        def compatible(x: dict) -> bool:
            if not base_for_compat:
                return True
            return _compatible_taxonomy(base_for_compat, taxo(x))

        exact = [x for x in enriched if item_canonical(x) == target]
        exact_accepted = [x for x in exact if is_accepted(x)]
        exact_nonaccepted = [x for x in exact if not is_accepted(x)]
        exact_nonacc_pointing = [x for x in exact_nonaccepted if accepted_canonical(x) == target]

        buckets = [
            [x for x in exact_accepted if compatible(x)],
            [x for x in exact_nonacc_pointing if compatible(x)],
            [x for x in enriched if is_accepted(x) and compatible(x)],
            [x for x in enriched if compatible(x)],
            [x for x in enriched if is_accepted(x)],
            enriched[:1],
        ]
        for b in buckets:
            if b:
                return b[0]
        return None
    except Exception as e:
        if log.isEnabledFor(logging.DEBUG):
            log.debug("CoL search/detail fallo para %s: %s", nombre, e)
        return None

def _taxonomy_from_col(item: dict) -> dict:
    tax = {
        "kingdom": None, "phylum": None, "class_name": None, "order_name": None,
        "superfamily": None, "family": None, "subfamily": None, "tribe": None,
        "subtribe": None, "genus": None, "subgenus": None,
    }

    cl = item.get("classification") or []
    for node in cl:
        r = (node.get("rank") or node.get("rankName") or "").strip().lower()
        n = (
            node.get("name")
            or node.get("scientificName")
            or node.get("canonicalName")
            or node.get("label")
            or node.get("labelHtml")
            or ""
        )
        if not isinstance(n, str):
            n = str(n)
        n = n.strip()
        # limpieza defensiva de HTML (p. ej. <i>Genus</i>)
        if "<" in n and ">" in n:
            n = re.sub(r"<[^>]+>", "", n)

        if not r or not n:
            continue

        if r == "kingdom":       tax["kingdom"] = n
        elif r == "phylum":      tax["phylum"] = n
        elif r == "class":       tax["class_name"] = n
        elif r == "order":       tax["order_name"] = n
        elif r == "superfamily": tax["superfamily"] = n
        elif r == "family":      tax["family"] = n
        elif r == "subfamily":   tax["subfamily"] = n
        elif r == "tribe":       tax["tribe"] = n
        elif r == "subtribe":    tax["subtribe"] = n
        elif r == "genus":       tax["genus"] = n
        elif r == "subgenus":    tax["subgenus"] = n

    return tax

def _taxonomy_from_itis_hierarchy(hierarchy_list: list[dict]) -> dict:
    tax = {
        "kingdom": None, "phylum": None, "class_name": None, "order_name": None,
        "superfamily": None, "family": None, "subfamily": None, "tribe": None,
        "subtribe": None, "genus": None, "subgenus": None,
    }
    for node in hierarchy_list or []:
        r = (node.get("rankName") or "").strip().lower()
        n = node.get("taxonName")
        if isinstance(n, str): n = n.strip()
        else: n = (str(n).strip() if n is not None else "")
        if not r or not n:
            continue

        if r == "kingdom":       tax["kingdom"] = n
        elif r == "phylum":      tax["phylum"] = n
        elif r == "class":       tax["class_name"] = n
        elif r == "order":       tax["order_name"] = n
        elif r == "superfamily": tax["superfamily"] = n
        elif r == "family":      tax["family"] = n
        elif r == "subfamily":   tax["subfamily"] = n
        elif r == "tribe":       tax["tribe"] = n
        elif r == "subtribe":    tax["subtribe"] = n
        elif r == "genus":       tax["genus"] = n
        elif r == "subgenus":    tax["subgenus"] = n

    return tax

# --------- CAMBIO: WoRMS usando fetch_best + caching ---------
async def _worms_best(nombre: str) -> Dict[str, Any] | None:
    try:
        from ..clients import worms
        best: Dict[str, Any] | None = await _cached("worms.fetch_best", nombre, lambda: worms.fetch_best(nombre))
        if isinstance(best, dict) and best:
            return best
    except Exception as e:
        if log.isEnabledFor(logging.DEBUG):
            log.debug("WoRMS fetch_best falló: %s", e)
    return None

def _taxonomy_from_worms(item: dict) -> dict:
    """
    Mapea campos de WoRMS a nuestros nombres, tolerando variaciones de clave
    como 'Class'/'Order' u objetos híbridos.
    """
    res: Dict[str, str] = {}
    if not isinstance(item, dict):
        return res
    # Normaliza claves a minúscula para evitar misses (e.g., 'Class', 'Order')
    src_item = { (k.lower() if isinstance(k, str) else k): v for k, v in item.items() }
    for src, dst in MAP_WRMS.items():
        v = src_item.get(src)  # src está en minúsculas en MAP_WRMS
        if v:
            res[dst] = v
    return res

# ----------------------------- Preferencias por "fuente más actual" -----------------------------
SOURCE_PRIORITY = {
    # mayor es "más fresco": CoL (mensual) > WoRMS (curado continuo marino) > ITIS/SIB > temáticos/locales > GBIF > RULE
    "Catalogue of Life": 4,
    "CoL": 4,
    "WoRMS": 3,
    "ITIS": 2,
    "SIB Colombia": 2,                 # ← SIB al nivel de ITIS
    "Reptile Database": 2,
    "Batrachia": 2,
    "ACO Aves": 2,
    "Mamíferos 2024 (SIB)": 2,
    # CITES es normativa
    "CITES (Species+)": 1,
    "GBIF": 1,
    "GBIF:detail": 1,
    "RULE": 0,  # cualquier prefijo RULE*
}

LOCAL_SOURCE_WEIGHTS = {
    "Catalogue of Life": 4, "WoRMS": 3, "ITIS": 2,
    "GBIF": 1, "GBIF:detail": 1,
    "ACO Aves": 2, "Mamíferos 2024 (SIB)": 2,
    "Reptile Database": 2, "Batrachia": 2, "CITES (Species+)": 1,
    "SIB Colombia": 2, "RULE": 0,
}

def _src_rank(src: str) -> int:
    if not isinstance(src, str) or not src:
        return -1
    if src.startswith("PREFERRED:"):
        src = src.split(":", 1)[1]
    # soportar etiquetas compuestas "A,B"
    parts = [p.strip() for p in src.split(",") if p.strip()]
    best = -1
    for p in parts or [""] :
        base = p.split(":", 1)[0]
        if base == "CoL":
            base = "Catalogue of Life"
        best = max(best, SOURCE_PRIORITY.get(base, 0))
    return best

def _src_weight(label: str) -> int:
    if not isinstance(label, str) or not label:
        return 0
    # Normaliza igual que _src_rank
    if label.startswith("PREFERRED:"):
        label = label.split(":", 1)[1]
    parts = [p.strip() for p in label.split(",") if p.strip()]
    best = 0
    for p in parts or [""]:
        base = p.split(":", 1)[0]
        if base == "CoL":
            base = "Catalogue of Life"
        best = max(best, LOCAL_SOURCE_WEIGHTS.get(base, SOURCE_PRIORITY.get(base, 0)))
    return best

def _record_override(prov: dict, field: str, old_val, old_src: str, new_val, new_src: str):
    prov.setdefault("_overrides", {})
    prov["_overrides"][field] = {"old": old_val, "from": old_src, "new": new_val, "to": new_src}

def _prefer_if_better(base: dict, prov: dict, field: str, value, src_label: str):
    """
    Establece/override del campo si:
      - está vacío, o
      - la nueva fuente tiene mayor prioridad que la actual, o
      - la actual es una RULE* y la nueva es autoritativa.
    Si el valor es IGUAL, aún así se permite 'subir' la fuente cuando la nueva tiene mayor prioridad
    o la actual es RULE*, registrando el override.
    """
    if value is None or value == "":
        return
    if field in {"kingdom","phylum","class_name","order_name","superfamily","family","subfamily","tribe","subtribe","genus","subgenus"}:
        value = _norm_taxon_value(value)

    cur_val = base.get(field)
    cur_src = prov.get(field)

    # Caso 1: vacío -> set directo
    if cur_val in (None, ""):
        base[field] = value
        prov[field] = src_label
        return

    # Calcula prioridades de fuente
    new_r = _src_rank(src_label)
    cur_r = _src_rank(cur_src) if isinstance(cur_src, str) else _src_rank("")

    # Caso 2: valor IGUAL -> permitir "upgrade" de fuente si nueva es mejor o la actual es RULE*
    if str(cur_val) == str(value):
        if (isinstance(cur_src, str) and cur_src.startswith("RULE")) or (new_r > cur_r):
            _record_override(prov, field, cur_val, cur_src if isinstance(cur_src, str) else "", value, src_label)
            prov[field] = f"PREFERRED:{src_label}"
        return

    # Caso 3: valor distinto -> override si nueva fuente es mejor o la actual es RULE*
    allow = new_r > cur_r or (isinstance(cur_src, str) and cur_src.startswith("RULE"))
    if allow:
        _record_override(prov, field, cur_val, cur_src if isinstance(cur_src, str) else "", value, src_label)
        base[field] = value
        prov[field] = f"PREFERRED:{src_label}"

def _lowrank_guard_ok(field: str, base: dict, src_tax: dict) -> bool:
    """Salvaguardas: superfamily requiere mismo orden; subfamily/tribe/subtribe misma familia; subgenus mismo género."""
    if field == "superfamily":
        b = (base.get("order_name") or "").strip().lower()
        s = (_norm_taxon_value(src_tax.get("order_name") or src_tax.get("order")) or "").strip().lower()
        return not (b and s) or (b == s)
    if field in {"subfamily","tribe","subtribe"}:
        b = (base.get("family") or "").strip().lower()
        s = (src_tax.get("family") or "").strip().lower()
        return not (b and s) or (b == s)
    if field == "subgenus":
        b = (base.get("genus") or "").strip().lower()
        s = (src_tax.get("genus") or "").strip().lower()
        return not (b and s) or (b == s)
    return True

def _apply_fresh_overrides(base: dict, prov: dict, candidates: list[tuple[str, dict]]):
    """
    Intenta sobreescribir campos con la fuente más fresca respetando compatibilidad
    y salvaguardas de niveles bajos.
    """
    for label, tax in candidates:
        if not isinstance(tax, dict) or not tax:
            continue
        if not _compatible_taxonomy(base, tax):
            continue
        for field in TAXO_FIELDS:
            if tax.get(field) and _lowrank_guard_ok(field, base, tax):
                _prefer_if_better(base, prov, field, tax[field], label)

# ---------- Ajuste: limitar a RULE* y evaluar compat con copia temporal ----------
def _apply_fresh_overrides_only_rules(base: dict, prov: dict, candidates: list[tuple[str, dict]]):
    """
    Reemplaza SOLO campos cuya fuente actual sea RULE*, y SOLO ese campo.
    Evalúa compatibilidad usando una copia temporal de base con el campo puesto en None.
    """
    for field in TAXO_FIELDS:
        cur_src = prov.get(field)
        if not (isinstance(cur_src, str) and cur_src.startswith("RULE")):
            continue

        cur_val = base.get(field)
        cur_src_val = prov.pop(field, None)
        base[field] = None  # deja vacío para evaluar fresco

        best_val, best_src_rank, best_src = None, -10**9, None
        for label, tax in candidates:
            if not isinstance(tax, dict) or not tax:
                continue
            tmp_base = dict(base)
            tmp_base[field] = None
            if not _compatible_taxonomy(tmp_base, tax):
                continue
            if not _lowrank_guard_ok(field, tmp_base, tax):
                continue
            v = tax.get(field)
            if v in (None, ""):
                continue
            rank = _src_rank(label)
            if (rank > best_src_rank) or (rank == best_src_rank and str(v) < str(best_val or "\uffff")):
                best_val, best_src_rank, best_src = v, rank, label

        if best_val is not None:
            base[field] = _norm_taxon_value(best_val) if field in {"kingdom","phylum","class_name","order_name","superfamily","family","subfamily","tribe","subtribe","genus","subgenus"} else best_val
            prov[field] = f"PREFERRED:{best_src}"
            _record_override(prov, field, cur_val, cur_src_val if isinstance(cur_src_val, str) else "", best_val, f"PREFERRED:{best_src}")
        else:
            base[field] = cur_val
            if cur_src_val is not None:
                prov[field] = cur_src_val

# ----------------------------- Adopción laxa -----------------------------
def _norm_lc(x):
    s = _norm_taxon_value(x)
    return (s or "").strip().lower() or None

def _lenient_highrank_merge(base: dict, prov: dict, src_tax: dict, label: str):
    """
    Adopta clase/orden con regla laxa cuando falten en base:
    - Para class_name: basta phylum coincidente (o phylum ausente en alguna parte).
    - Para order_name: basta class_name coincidente; si no hay clase, se acepta por phylum coincidente.
    """
    ph_b = _norm_lc(base.get("phylum"))
    ph_s = _norm_lc(src_tax.get("phylum"))
    same_ph = bool(ph_b and ph_s and ph_b == ph_s) or (not ph_b or not ph_s)

    # Clase
    if not base.get("class_name") and src_tax.get("class_name") and same_ph:
        _prefer_if_better(base, prov, "class_name", src_tax["class_name"], label)

    # Orden
    cls_b = _norm_lc(base.get("class_name"))
    cls_s = _norm_lc(src_tax.get("class_name"))
    same_cls = bool(cls_b and cls_s and cls_b == cls_s)
    if not base.get("order_name") and src_tax.get("order_name") and (same_cls or same_ph):
        _prefer_if_better(base, prov, "order_name", src_tax["order_name"], label)

# ----------------------------- Enriquecimiento ITIS (full/partial) -----------------------------
async def _enrich_with_itis(base: dict, itis_client, tsn: str | None) -> dict:
    """
    Enriquecimiento con ITIS:
    - Intenta full record; si no trae jerarquía, cae a getFullHierarchyFromTSN; si no, a getHierarchyUpFromTSN.
    - Rellena superfamily/subfamily/tribe (y el resto) solo si en base están vacíos.
    """
    if not tsn:
        return base

    fr = None
    try:
        fr = await itis_client.get_full_record(tsn)
    except Exception as e:
        if log.isEnabledFor(logging.DEBUG):
            log.debug("ITIS get_full_record falló: %s", e)
        fr = None

    hlist = None
    if isinstance(fr, dict):
        hl = fr.get("hierarchyList")
        if isinstance(hl, dict):
            hlist = hl.get("hierarchy")
        elif isinstance(hl, list):
            hlist = hl

    if not hlist:
        try:
            fh = await itis_client.get_full_hierarchy(tsn)
            if isinstance(fh, dict):
                hlist = fh.get("hierarchyList") or fh.get("hierarchy") or None
        except Exception as e:
            if log.isEnabledFor(logging.DEBUG):
                log.debug("ITIS get_full_hierarchy falló: %s", e)
            hlist = None

    if not hlist:
        try:
            up = await itis_client.get_hierarchy_up(tsn)
            if isinstance(up, dict):
                hlist = up.get("hierarchyList") or up.get("hierarchy") or None
        except Exception as e:
            if log.isEnabledFor(logging.DEBUG):
                log.debug("ITIS get_hierarchy_up falló: %s", e)
            hlist = None

    if hlist:
        extra = _taxonomy_from_itis_hierarchy(hlist)
        for k, v in extra.items():
            if v and (not base.get(k)):
                base[k] = v

    return base

# ----------------------------- ITIS: jerarquía con fallbacks -----------------------------
async def _itis_hierarchy_by_name(nombre: str) -> dict:
    try:
        from ..clients import itis
        d = await _cached("itis.search", nombre, lambda: itis.search_by_scientific_name(nombre))
        if not isinstance(d, dict):
            return {}

        # 1) Resolver TSN canónico
        tsn: Optional[str] = None
        for lst_key in ("scientificNames", "acceptedNameList", "unacceptedNames"):
            arr = d.get(lst_key)
            if isinstance(arr, list) and arr:
                c = arr[0] or {}
                tsn = (c.get("tsn") or c.get("acceptedTsn") or c.get("acceptedTSN") or "").strip() or None
                if tsn: break
            elif isinstance(arr, dict):
                for inner in ("scientificNames", "acceptedNames", "unacceptedNames"):
                    v = arr.get(inner)
                    if isinstance(v, list) and v:
                        c = v[0] or {}
                        tsn = (c.get("tsn") or c.get("acceptedTsn") or c.get("acceptedTSN") or "").strip() or None
                        if tsn: break
                if tsn: break
        if not tsn:
            return {}

        # -------- helpers locales --------
        async def _http_get(url: str, params: dict) -> dict:
            return await fetch_json(url, params=params, timeout=12, retries=2)

        def _nodes_to_maps(nodes: list[dict]) -> tuple[dict, dict]:
            """Devuelve (tax_dict, tsn_por_rango_en_minusculas)."""
            tax, tsn_by_rank = {}, {}
            for n in nodes or []:
                r = (n.get("rankName") or n.get("rank") or "").strip().lower()
                nm = (n.get("taxonName") or n.get("name") or "").strip()
                t  = (n.get("tsn") or n.get("TSN") or "")
                if r and nm:
                    if r in MAP_ITIS:
                        tax[MAP_ITIS[r]] = nm
                    if t:
                        tsn_by_rank[r] = t
            return tax, tsn_by_rank

        async def _get_up(tsn_any: str) -> list[dict]:
            jj = await _http_get("https://www.itis.gov/ITISWebService/jsonservice/getHierarchyUpFromTSN", {"tsn": tsn_any})
            hl = (jj or {}).get("hierarchyList")
            if isinstance(hl, dict): return hl.get("hierarchy") or []
            if isinstance(hl, list): return hl
            return []

        async def _get_down(tsn_any: str) -> list[dict]:
            jj = await _http_get("https://www.itis.gov/ITISWebService/jsonservice/getHierarchyDownFromTSN", {"tsn": tsn_any})
            hl = (jj or {}).get("hierarchyList")
            if isinstance(hl, dict): return hl.get("hierarchy") or []
            if isinstance(hl, list): return hl
            return []

        # 2) Jerarquía principal
        parsed: dict = {}
        tsn_by_rank: dict = {}

        j = await _http_get("https://www.itis.gov/ITISWebService/jsonservice/getFullRecordFromTSN", {"tsn": tsn})
        nodes = (((j or {}).get("fullRecord") or {}).get("hierarchyList") or {}).get("hierarchy")
        if isinstance(nodes, list) and nodes:
            parsed, tsn_by_rank = _nodes_to_maps(nodes)

        if not parsed:
            j2 = await _http_get("https://www.itis.gov/ITISWebService/jsonservice/getFullHierarchyFromTSN", {"tsn": tsn})
            nodes2 = ((j2 or {}).get("hierarchyList") or {}).get("hierarchy")
            if isinstance(nodes2, list) and nodes2:
                parsed, tsn_by_rank = _nodes_to_maps(nodes2)

        # >>> Corrección de NameError: encapsular en if not parsed <<<
        if not parsed:
            j3 = await _http_get("https://www.itis.gov/ITISWebService/jsonservice/getHierarchyUpFromTSN", {"tsn": tsn})
            nodes3 = ((j3 or {}).get("hierarchyList") or {}).get("hierarchy")
            if isinstance(nodes3, list) and nodes3:
                parsed, tsn_by_rank = _nodes_to_maps(nodes3)

        # 3) Completar superfamily/subfamily desde familia/género
        fam_tsn = tsn_by_rank.get("family")
        gen_tsn = tsn_by_rank.get("genus")

        async def _pick_by_rank_from_up(tsn_any: str, rank_name: str) -> Optional[str]:
            up = await _get_up(tsn_any)
            for n in up:
                if str(n.get("rankName","")).strip().lower() == rank_name:
                    return n.get("taxonName")
            return None

        async def _pick_by_rank_from_down(tsn_any: str, rank_name: str) -> Optional[str]:
            down = await _get_down(tsn_any)
            for n in down or []:
                if str(n.get("rankName","")).strip().lower() == rank_name:
                    return n.get("taxonName")
            return None

        if not parsed.get("superfamily") and fam_tsn:
            val = await _pick_by_rank_from_up(fam_tsn, "superfamily")
            parsed["superfamily"] = val or parsed.get("superfamily")

        if not parsed.get("subfamily") and gen_tsn:
            val = await _pick_by_rank_from_up(gen_tsn, "subfamily")
            parsed["subfamily"] = val or parsed.get("subfamily")

        if not parsed.get("superfamily") and tsn_by_rank.get("order"):
            val = await _pick_by_rank_from_down(tsn_by_rank["order"], "superfamily")
            parsed["superfamily"] = val or parsed.get("superfamily")

        if not parsed.get("subfamily") and fam_tsn:
            val = await _pick_by_rank_from_down(fam_tsn, "subfamily")
            parsed["subfamily"] = val or parsed.get("subfamily")

        return parsed
    except Exception as e:
        if log.isEnabledFor(logging.DEBUG):
            log.debug("ITIS hierarchy por nombre falló para %s: %s", nombre, e)
        return {}

# ----------------------------- Marcadores de presencia (solo etiquetas) -----------------------------
async def _found_in_worms(nombre: str) -> bool:
    try:
        from ..clients import worms
        w = await worms.search_name(nombre)
        return isinstance(w, list) and len(w) > 0
    except Exception as e:
        if log.isEnabledFor(logging.DEBUG):
            log.debug("Presence WoRMS falló: %s", e)
        return False

async def _found_in_itis(nombre: str) -> bool:
    try:
        from ..clients import itis
        d = await itis.search_by_scientific_name(nombre)
        if not isinstance(d, dict):
            return False
        return bool(d.get("scientificNames") or d.get("acceptedNameList"))
    except Exception as e:
        if log.isEnabledFor(logging.DEBUG):
            log.debug("Presence ITIS falló: %s", e)
        return False

async def _found_in_col(nombre: str) -> bool:
    try:
        data = await col.search_name(nombre)
        return isinstance(data, dict) and ((data.get("total") or 0) > 0)
    except Exception as e:
        if log.isEnabledFor(logging.DEBUG):
            log.debug("Presence CoL falló: %s", e)
        return False

async def _found_in_sib(nombre: str) -> bool:
    try:
        from ..clients import sib
        s = await sib.record_search(nombre, size=1)
        if isinstance(s, list):
            return len(s) > 0
        if isinstance(s, dict):
            for key in ("results","data","records","items"):
                v = s.get(key)
                if isinstance(v, list) and len(v) > 0:
                    return True
        return False
    except Exception as e:
        if log.isEnabledFor(logging.DEBUG):
            log.debug("Presence SIB falló: %s", e)
        return False

# --- nuevas presencias de tus conectores/ingestas ---
async def _found_in_reptile_db(nombre: str) -> bool:
    try:
        from ..clients import reptile_db
        return await reptile_db.exists(nombre)
    except Exception as e:
        if log.isEnabledFor(logging.DEBUG):
            log.debug("Presence Reptile DB falló: %s", e)
        return False

async def _found_in_batrachia(nombre: str) -> bool:
    try:
        from ..clients import batrachia
        return await batrachia.exists(nombre)
    except Exception as e:
        if log.isEnabledFor(logging.DEBUG):
            log.debug("Presence Batrachia falló: %s", e)
        return False

async def _found_in_cites_public(nombre: str) -> bool:
    try:
        from ..clients import cites_public
        return await cites_public.exists(nombre)
    except Exception as e:
        if log.isEnabledFor(logging.DEBUG):
            log.debug("Presence CITES falló: %s", e)
        return False

async def _found_in_aco_birds(nombre: str) -> bool:
    try:
        from ..ingestors import aco_birds as ing_aco
        return await ing_aco.exists(nombre)
    except Exception as e:
        if log.isEnabledFor(logging.DEBUG):
            log.debug("Presence ACO Birds falló: %s", e)
        return False

async def _found_in_sib_mammals(nombre: str) -> bool:
    try:
        from ..ingestors import sib_mammals_2024 as ing_mam
        return await ing_mam.exists(nombre)
    except Exception as e:
        if log.isEnabledFor(logging.DEBUG):
            log.debug("Presence Mamíferos 2024 falló: %s", e)
        return False

async def buscar_en_fuentes_externas(nombre: str) -> List[str]:
    nombre_n = normaliza_nombre(nombre)

    async def _with_timeout(coro, seconds=5):
        try:
            return await asyncio.wait_for(coro, timeout=seconds)
        except Exception:
            return False

    tasks: List[tuple[str, asyncio.Task]] = [
        ("Catalogue of Life", asyncio.create_task(_with_timeout(_found_in_col(nombre_n)))),
        ("WoRMS",             asyncio.create_task(_with_timeout(_found_in_worms(nombre_n)))),
        ("ITIS",              asyncio.create_task(_with_timeout(_found_in_itis(nombre_n)))),
        ("SIB Colombia",      asyncio.create_task(_with_timeout(_found_in_sib(nombre_n)))),
        ("reptile_db",        asyncio.create_task(_with_timeout(_found_in_reptile_db(nombre_n)))),
        ("batrachia",         asyncio.create_task(_with_timeout(_found_in_batrachia(nombre_n)))),
        ("cites_public",      asyncio.create_task(_with_timeout(_found_in_cites_public(nombre_n)))),
        ("aco_aves",          asyncio.create_task(_with_timeout(_found_in_aco_birds(nombre_n)))),
        ("mamiferos_2024",    asyncio.create_task(_with_timeout(_found_in_sib_mammals(nombre_n)))),
    ]

    halladas: List[str] = []
    results = await asyncio.gather(*(t for _, t in tasks), return_exceptions=True)
    for (label, _), res in zip(tasks, results):
        if isinstance(res, bool) and res:
            halladas.append(label)
    return sorted(set(halladas))

# ----------------------------- Fusor / Enriquecedor -----------------------------
def _set_if(target: dict, prov: dict, field: str, value, src: str):
    if value is None or value == "":
        return
    if field in {"kingdom","phylum","class_name","order_name","superfamily","family","subfamily","tribe","subtribe","genus","subgenus"}:
        value = _norm_taxon_value(value)
    target[field] = value
    prov[field] = src

def _copy_if_consistent(base: dict, prov: dict, src_dict: dict, src_label: str):
    """
    Copia campos taxonómicos desde src_dict hacia base con reglas:
    - NO copia superfamily si el orden no coincide cuando ambos existen.
    - subfamily/tribe/subtribe solo si family coincide (si base.family y src.family están presentes).
    - subgenus solo si genus coincide (si ambos presentes).
    """
    # Rangos altos: copiar si faltan (superfamily se maneja con guarda de orden)
    for f in ["kingdom","phylum","class_name","order_name","family","genus"]:
        if src_dict.get(f) and not base.get(f):
            _set_if(base, prov, f, src_dict.get(f), src_label)

    # superfamily requiere mismo orden si ambos existen
    same_order = True
    if base.get("order_name") and (src_dict.get("order_name") or src_dict.get("order")):
        base_ord = (_norm_taxon_value(base["order_name"]) or "").strip().lower()
        src_ord  = (_norm_taxon_value(src_dict.get("order_name") or src_dict.get("order")) or "").strip().lower()
        same_order = base_ord == src_ord
    if same_order and src_dict.get("superfamily") and not base.get("superfamily"):
        _set_if(base, prov, "superfamily", src_dict["superfamily"], src_label)

    # Validaciones para rangos bajos
    same_family = True
    if base.get("family") and src_dict.get("family"):
        same_family = (str(base["family"]).strip().lower() == str(src_dict["family"]).strip().lower())

    same_genus = True
    if base.get("genus") and src_dict.get("genus"):
        same_genus = (str(base["genus"]).strip().lower() == str(src_dict["genus"]).strip().lower())

    if same_family:
        for f in ["subfamily","tribe","subtribe"]:
            if src_dict.get(f) and not base.get(f):
                _set_if(base, prov, f, src_dict.get(f), src_label)

    if same_genus and src_dict.get("subgenus") and not base.get("subgenus"):
        _set_if(base, prov, "subgenus", src_dict["subgenus"], src_label)

def _faltantes(d: Dict[str, Any]) -> list[str]:
    return [k for k in TAXO_FIELDS if not d.get(k)]

def _extract_authorship_from_scientific(sci: Optional[str]) -> Optional[str]:
    if not sci:
        return None
    m = re.search(r"\(([^()]*)\)\s*$", sci)
    return m.group(1) if m else None

# --------- AUTORÍA ROBUSTA: extractor flexible desde la cadena final ----------
AUTH_TAIL = re.compile(
    r"""
    (?:
      ^|                # inicio de cadena o cualquier prefijo
      .*?\b[a-z\-]+     # último epíteto (minúsculas) previo a la autoría
    )
    \s+
    (?P<auth>
      (?:[A-Z][\w\.\-']+(?:\s+(?:ex|in)\s+[A-Z][\w\.\-']+)?   # Autor o "Autor ex/in Autor"
         (?:\s*(?:&|et)\s*[A-Z][\w\.\-']+)*                  # &/et otro autor
      )
      ,\s*\d{3,4}(?:[a-z])?                                  # año (1758 o 1758a)
    )
    \s*$               # fin de cadena
    """,
    re.VERBOSE
)

def _extract_authorship_flexible(sci: Optional[str]) -> Optional[str]:
    """
    Extrae autoría desde la cadena científica final:
      - con paréntesis: "... (Autor, 1758)" -> "Autor, 1758"
      - sin paréntesis (común en géneros): "Melanoides Olivier, 1804" -> "Olivier, 1804"
      - soporta "ex"/"in", "&"/"et", abreviaturas y sufijo de año con letra.
    """
    if not sci:
        return None
    s = sci.strip()

    # 1) Con paréntesis al final
    m = re.search(r"\(([^()]*)\)\s*$", s)
    if m:
        val = m.group(1).strip()
        return val or None

    # 2) Sin paréntesis: cola de autoría robusta (uso search para mayor tolerancia)
    m2 = AUTH_TAIL.search(s)
    if m2:
        return m2.group("auth").strip()

    return None

# ----------------------------- Sinónimos GBIF (paginado + dedupe robusto) -----------------------------
def _norm_syn_key(syn: dict) -> tuple[str, str, str]:
    def n(x): 
        return (x or "").strip().lower()
    return (n(syn.get("scientificName")), n(syn.get("authorship")), n(syn.get("rank")))

async def _gbif_synonyms(usage_key: int) -> list[dict]:
    try:
        base = f"https://api.gbif.org/v1/species/{usage_key}/synonyms"
        out, limit, offset = [], 500, 0
        while True:
            j = await fetch_json(base, params={"limit": limit, "offset": offset}, timeout=20, retries=2)
            rows = j.get("results") if isinstance(j, dict) else (j if isinstance(j, list) else [])
            out.extend(rows or [])
            if not isinstance(j, dict) or (offset + limit) >= (j.get("count") or len(rows or [])):
                break
            offset += limit
        # Dedupe robusto
        seen: set[tuple[str,str,str]] = set()
        uniq: list[dict] = []
        for s in out:
            key = _norm_syn_key(s)
            if key in seen:
                continue
            seen.add(key)
            uniq.append(s)
        return uniq
    except Exception as e:
        if log.isEnabledFor(logging.DEBUG):
            log.debug("GBIF synonyms falló para %s: %s", usage_key, e)
        return []

# --------- NUEVOS: Scoring y Quorum ---------
def _field_score(base: dict, cand_tax: dict, label: str, field: str, status: str | None) -> int:
    score = _src_weight(label)
    st = (status or "").upper()
    if st == "ACCEPTED":
        score += 1
    if st in UNCERTAIN_STATUSES:
        score -= 1

    # señales de compatibilidad
    fam_b = (base.get("family") or "").strip().lower()
    fam_c = (cand_tax.get("family") or "").strip().lower()
    gen_b = (base.get("genus") or "").strip().lower()
    gen_c = (cand_tax.get("genus") or "").strip().lower()
    if fam_b and fam_c and fam_b == fam_c:
        score += 1
    if gen_b and gen_c and gen_b == gen_c:
        score += 1

    highs = sum(bool(cand_tax.get(k)) for k in ("phylum","class_name","order_name","family","genus"))
    if highs >= 4:
        score += 1
    return score

def _apply_scores_and_quorum(base: dict, prov: dict, candidates: list[tuple[str, dict, dict]]):
    """
    candidates: [(label, tax_dict, meta_dict)]
    meta_dict puede incluir {"status": "..."} u otros hints.
    """
    decisions = {}

    def _best_tuple(srcs: list[tuple[str,int]]) -> tuple[int,int,int]:
        uniq = {s for s,_ in srcs}
        best_priority = max((_src_weight(s) for s,_ in srcs), default=-999)
        return (len(uniq), sum(sc for s,sc in srcs), best_priority)

    for field in TAXO_FIELDS:
        props = []
        for label, tax, meta in candidates:
            v = tax.get(field)
            if not v:
                continue
            sc = _field_score(base, tax, label, field, (meta or {}).get("status"))
            props.append((label, v, sc))

        if not props:
            continue

        # agrupa por valor
        quorum_map: dict[str, list[tuple[str,int]]] = {}
        for label, v, sc in props:
            quorum_map.setdefault(str(v), []).append((label, sc))

        best_val = None; best_srcs = []; best_key = (-1, -1, -1)
        for val, srcs in quorum_map.items():
            key = _best_tuple(srcs)
            if key > best_key or (key == best_key and str(val) < str(best_val or "\uffff")):
                best_val, best_srcs, best_key = val, srcs, key

        cur_src = prov.get(field)
        cur_weight = _src_weight(cur_src if isinstance(cur_src, str) else "")
        cur_is_rule = isinstance(cur_src, str) and cur_src.startswith("RULE")
        cur_val = base.get(field)

        if best_val is not None and (str(cur_val) != str(best_val)):
            has_quorum = len({s for s,_ in best_srcs}) >= 2
            best_top_score = max(sc for _, sc in best_srcs)
            if has_quorum or cur_is_rule or (best_top_score > cur_weight):
                _record_override(prov, field, cur_val, cur_src if isinstance(cur_src, str) else "", best_val, f"PREFERRED:{','.join(s for s,_ in best_srcs)}")
                base[field] = _norm_taxon_value(best_val) if field in {"kingdom","phylum","class_name","order_name","superfamily","family","subfamily","tribe","subtribe","genus","subgenus"} else best_val
                prov[field] = f"PREFERRED:{','.join(s for s,_ in best_srcs)}"

        decisions[field] = {
            "candidates": [{"source": s, "value": v, "score": sc} for s, v, sc in props],
            "chosen": base.get(field),
        }

    # (Opcional: truncar candidates a top-5 por campo si preocupa tamaño)
    for fld, info in decisions.items():
        cand = info.get("candidates") or []
        if len(cand) > 5:
            decisions[fld]["candidates"] = sorted(cand, key=lambda x: (-x["score"], str(x["value"])))[:5]

    prov.setdefault("_decisions", {})["_last"] = decisions

# --------- NUEVOS HELPERS PARA AUTORÍA ---------
def _col_authorship(best_col: dict) -> str | None:
    # 1) Intentos directos en la raíz
    for k in ("authorship", "author", "labelAuthorship"):
        v = best_col.get(k)
        if v:
            return str(v).strip()
    # 2) usage.name.authorship
    nm = ((best_col.get("usage") or {}).get("name") or {})
    a = nm.get("authorship")
    if a:
        return str(a).strip()
    # 3) Construir a partir de basionym/combination
    for key in ("basionymAuthorship", "combinationAuthorship", "basionymOrCombinationAuthorship"):
        ba = nm.get(key)
        if isinstance(ba, dict):
            authors = ba.get("authors")
            if isinstance(authors, list):
                authors = " & ".join(authors)
            year = ba.get("year")
            if authors and year:
                return f"{authors}, {year}"
            if authors:
                return str(authors).strip()
            if year:
                return str(year).strip()
    return None

async def _gbif_detail(usage_key: int) -> Dict[str, Any]:
    try:
        url = f"https://api.gbif.org/v1/species/{usage_key}"
        j: Dict[str, Any] = await fetch_json(url, timeout=12, retries=2)
        return j
    except Exception as e:
        if log.isEnabledFor(logging.DEBUG):
            log.debug("GBIF detail falló para %s: %s", usage_key, e)
        return {}

# --- Wrapper seguro para IUCN: intenta varias firmas y nunca rompe ---
async def _iucn_category_safe(name: str | None) -> str | None:
    if not name:
        return None
    try:
        from ..clients import iucn as iucn_client
    except Exception:
        return None

    import asyncio as _asyncio
    candidates = (
        "category_for_name",
        "category_for",
        "get_category",
        "category",
        "status_for_name",
    )
    for attr in candidates:
        fn = getattr(iucn_client, attr, None)
        if not fn:
            continue
        try:
            res = await fn(name) if _asyncio.iscoroutinefunction(fn) else fn(name)
            if isinstance(res, str):
                return res or None
            if isinstance(res, dict):
                for k in ("category", "iucn_category", "status"):
                    v = res.get(k)
                    if v:
                        return str(v)
        except Exception as e:
            if log.isEnabledFor(logging.DEBUG):
                log.debug("IUCN wrapper fallo en %s: %s", attr, e)
            continue
    return None

# --- Reparaciones/curaduría post-merge ---
def _post_sanity_repairs(base: dict, provenance: dict):
    # Imposible: Lepidoptera no puede tener Blaberoidea (superfamilia de Blattodea)
    if (base.get("order_name") == "Lepidoptera" and
        str(base.get("superfamily") or "").strip().lower() == "blaberoidea"):
        base["superfamily"] = None
        provenance.pop("superfamily", None)

    # Relleno por regla: familias de mariposas → Papilionoidea
    lep_to_super = {
        "Nymphalidae": "Papilionoidea",
        "Pieridae": "Papilionoidea",
        "Lycaenidae": "Papilionoidea",
        "Papilionidae": "Papilionoidea",
        "Hesperiidae": "Papilionoidea",
        "Riodinidae": "Papilionoidea",
    }
    if base.get("order_name") == "Lepidoptera":
        fam = base.get("family")
        should = lep_to_super.get(fam)
        if should and base.get("superfamily") != should:
            _set_if(base, provenance, "superfamily", should, "RULE")

    # Gastropoda marinos: Cerithioidea pertenece a Littorinimorpha, no a Pteropoda/incertae sedis/Cerithiida
    if (str(base.get("class_name") or "").strip().lower() == "gastropoda" and
        str(base.get("superfamily") or "").strip().lower() == "cerithioidea"):
        bad = {"pteropoda", "caenogastropoda incertae sedis", "cerithiida"}
        cur = (base.get("order_name") or "").strip().lower()
        if cur in bad:
            _set_if(base, provenance, "order_name", "Littorinimorpha", "RULE:gastropoda-cerithioidea→littorinimorpha@v1")

def _prefer_gastropoda_order_if_consensus(base: dict, prov: dict, tax_col: dict | None, tax_wrms: dict | None):
    """Si CoL y WoRMS coinciden en Gastropoda/Cerithioidea, usamos ese orden."""
    if (str(base.get("class_name") or "").lower() != "gastropoda" or
        str(base.get("superfamily") or "").lower() != "cerithioidea"):
        return
    col_ord = (tax_col or {}).get("order_name")
    wrms_ord = (tax_wrms or {}).get("order_name")
    if col_ord and wrms_ord and _norm_taxon_value(col_ord).lower() == _norm_taxon_value(wrms_ord).lower():
        if base.get("order_name") != col_ord:
            _record_override(prov, "order_name", base.get("order_name"), prov.get("order_name",""), col_ord, "PREFERRED:Catalogue of Life,WoRMS")
            base["order_name"] = col_ord
            prov["order_name"] = "PREFERRED:Catalogue of Life,WoRMS"

# --- Reglas internas con metadatos (auditables) ---
def _apply_internal_rules(base: dict, provenance: dict):
    """
    Solo rellenan si el campo objetivo está vacío (último recurso).
    """
    fam2super: Dict[str, tuple[str, str, str]] = {
        "Viperidae": ("Colubroidea", "RULE:family→superfamily@v1", "Ref: Zaher+2019"),
        "Alligatoridae": ("Alligatoroidea", "RULE:family→superfamily@v1", "Ref: Brochu 2003"),
        "Mixosauridae": ("Mixosauroidea", "RULE:family→superfamily@v1", "Ref: Ichthyosaur phylogeny"),
    }
    fam2order: Dict[str, tuple[str, str, str]] = {
        "Mixosauridae": ("Ichthyosauria", "RULE:family→order@v1", "Ref: Ichthyosauria systematics"),
    }

    fam = base.get("family")
    if fam and not base.get("superfamily") and fam in fam2super:
        value, rule_tag, _note = fam2super[fam]
        _set_if(base, provenance, "superfamily", value, rule_tag)

    if fam and not base.get("order_name") and fam in fam2order:
        value, rule_tag, _note = fam2order[fam]
        _set_if(base, provenance, "order_name", value, rule_tag)

# --- Trimming de provenance para evitar crecimiento descontrolado ---
def _trim_overrides(prov: dict, max_per_field: int = 3):
    ov = prov.get("_overrides")
    if not isinstance(ov, dict):
        return
    # Hook para cuando _overrides almacene listas por campo (slice aquí).
    return

# ----------------------------- Reconciliación principal -----------------------------
async def reconcile_name(db: Session, name: str) -> Taxon:
    """
    Flujo:
      1) GBIF species_match
      1.1) GBIF detail: completar class/order/autoría si faltan (autoridad)
      2) Enriquecimiento: CoL → WoRMS → ITIS (para cualquier campo faltante)
      2.5) Adopción laxa / Fallback por familia (ITIS + CoL + WoRMS)
      2.7) Temáticos/Locales + SIB
      3) Overrides por "fuente más actual" + limpieza RULE* + Scoring/Quorum (+ consenso Gastropoda)
      4) IUCN
      5) Epíteto
      6) Fuentes/Presencia
      7) UPSERT
      8) Sinónimos (transaccional + dedupe, sin I/O dentro de la transacción)
    """
    name_n = normaliza_nombre(name)

    # 1) GBIF base
    m: Dict[str, Any] = await gbif.species_match(name_n) or {}
    usage_key = m.get("usageKey")

    # fallback si GBIF no devuelve clave → accepted_name de CoL/WoRMS y reintento
    if not usage_key:
        accepted_from_alt: Optional[str] = None

        best_col = await _col_best(name_n)
        if best_col:
            accepted_from_alt = (
                best_col.get("acceptedName") or
                best_col.get("accepted") or
                (best_col.get("usage") or {}).get("name")
            )
            if isinstance(accepted_from_alt, dict):
                accepted_from_alt = (
                    accepted_from_alt.get("scientificName") or
                    accepted_from_alt.get("canonicalName") or
                    accepted_from_alt.get("name")
                )

        if not accepted_from_alt:
            best_wrms = await _worms_best(name_n)
            if best_wrms:
                accepted_from_alt = (
                    best_wrms.get("valid_name") or
                    best_wrms.get("scientificname") or
                    best_wrms.get("AphiaName")
                )

        if accepted_from_alt:
            m = await gbif.species_match(normaliza_nombre(accepted_from_alt)) or {}
            usage_key = m.get("usageKey")

    # Si sigue sin GBIF → guardamos mínimo con marcas de presencia
    if not usage_key:
        # Presencias (cacheadas para evitar re-consulta si vuelve a pedirse por el mismo nombre)
        fuentes = await _cached("presence", name_n, lambda: buscar_en_fuentes_externas(name_n))
        prov_json: Dict[str, Any] = {"scientific_name": "INPUT"}
        warnings_min = _build_warnings({"gbif_key": None, "status": (m or {}).get("status")}, prov_json)
        if warnings_min:
            prov_json["_warnings"] = warnings_min

        presence = { _norm_src_label(s) for s in (fuentes or []) }
        presence.discard(None)
        fuentes_sorted = sorted(presence, key=lambda x: -LOCAL_SOURCE_WEIGHTS.get(x,0))

        t = Taxon(
            scientific_name=name_n,
            status=None,
            gbif_key=None,
            sources_csv=",".join(fuentes_sorted) if fuentes_sorted else None,
            provenance_json=json.dumps(prov_json),
        )
        db.add(t); db.commit(); db.refresh(t)
        return t

    # Base GBIF
    status = m.get("status")
    accepted_key = m.get("acceptedUsageKey")
    rank = m.get("rank")
    sci_name = m.get("scientificName") or name_n

    base: Dict[str, Any] = {
        "scientific_name": sci_name,  # conservar cadena original (NFC) para display
        "canonical_name": m.get("canonicalName"),
        "authorship": m.get("authorship") or _extract_authorship_from_scientific(sci_name),
        "gbif_key": usage_key,
        "status": status,
        "accepted_gbif_key": accepted_key,
        "rank": rank,
        "kingdom": m.get("kingdom"),
        "phylum": m.get("phylum"),
        "class_name": _norm_taxon_value(m.get("class")),
        "order_name": _norm_taxon_value(m.get("order")),
        "superfamily": None,
        "family": m.get("family"),
        "subfamily": None,
        "tribe": None,
        "subtribe": None,
        "genus": m.get("genus"),
        "subgenus": None,
    }
    provenance: Dict[str, str] = {k: "GBIF" for k, v in base.items() if v not in (None, "")}

    # --- GBIF detail (orden/clase/autoría) con cache local para evitar doble fetch ---
    gbif_detail_cache: Dict[str, Any] | None = None
    if (not base.get("order_name")) or (not base.get("class_name")) or (not base.get("authorship")):
        gbif_detail_cache = await _gbif_detail(usage_key)

    if isinstance(gbif_detail_cache, dict) and gbif_detail_cache:
        if not base.get("order_name") and gbif_detail_cache.get("order"):
            _set_if(base, provenance, "order_name", gbif_detail_cache.get("order"), "GBIF:detail")
        if not base.get("class_name") and gbif_detail_cache.get("class"):
            _set_if(base, provenance, "class_name", gbif_detail_cache.get("class"), "GBIF:detail")
        if not base.get("authorship") and gbif_detail_cache.get("authorship"):
            _set_if(base, provenance, "authorship", gbif_detail_cache.get("authorship"), "GBIF:detail")

    # --- Saneamiento: si class == order, dejamos class vacío para permitir corrección
    if base.get("class_name") and base.get("order_name") and \
       str(base["class_name"]).lower() == str(base["order_name"]).lower():
        base["class_name"] = None
        provenance.pop("class_name", None)

    # 2) Enriquecimiento CoL → WoRMS → ITIS (llenado si faltan)
    tax_col: dict | None = None
    tax_wrms: dict | None = None
    tax_itis: dict | None = None

    # ---- CoL
    best_col = await _col_best(sci_name, base_for_compat=base)
    if best_col:
        tax_col = _taxonomy_from_col(best_col)
        if _faltantes(base):
            if _compatible_taxonomy(base, tax_col) or all(base.get(k) is None for k in ["phylum","class_name","order_name","family","genus"]):
                _copy_if_consistent(base, provenance, tax_col, "Catalogue of Life")
        # preferir autoría de CoL incluso si ya existe
        col_author = _col_authorship(best_col)
        if col_author:
            _prefer_if_better(base, provenance, "authorship", col_author, "Catalogue of Life")

    # ---- WoRMS
    best_wrms = await _worms_best(sci_name)
    if best_wrms:
        tax_wrms = _taxonomy_from_worms(best_wrms)
        if _faltantes(base):
            if _compatible_taxonomy(base, tax_wrms) or all(base.get(k) is None for k in ["phylum","class_name","order_name","family","genus"]):
                _copy_if_consistent(base, provenance, tax_wrms, "WoRMS")
        # preferir autoría de WoRMS incluso si ya existe
        if best_wrms.get("authority"):
            _prefer_if_better(base, provenance, "authorship", best_wrms.get("authority"), "WoRMS")

    # ---- Si tras GBIF/CoL/WoRMS aún no hay autoría, aplica extractor flexible (RULE) ----
    if not base.get("authorship"):
        flex = _extract_authorship_flexible(base.get("scientific_name"))
        if flex:
            _set_if(base, provenance, "authorship", flex, "RULE:authorship-tail@v1")

    # ---- ITIS (lookup + deep merge si faltan campos)
    try:
        from ..clients import itis
        canon_for_itis = normaliza_nombre(base.get("canonical_name") or sci_name)

        tax_itis = {}
        itis_info = await itis.lookup_taxonomy(canon_for_itis)
        if isinstance(itis_info, dict):
            tax_itis = (itis_info.get("parsed") or {})

        if any(not base.get(f) for f in ("superfamily", "subfamily")) or _faltantes(base):
            deep = await _itis_hierarchy_by_name(canon_for_itis)
            for k, v in (deep or {}).items():
                if v and not tax_itis.get(k):
                    tax_itis[k] = v
    except Exception as e:
        if log.isEnabledFor(logging.DEBUG):
            log.debug("ITIS lookup/merge falló: %s", e)
        tax_itis = {}

    if tax_itis:
        for k in ("order_name","class_name","superfamily","family","subfamily","genus"):
            if tax_itis.get(k):
                tax_itis[k] = _norm_taxon_value(tax_itis[k])

        if _faltantes(base):
            if _compatible_taxonomy(base, tax_itis) or all(base.get(k) is None for k in ["phylum","class_name","order_name","family","genus"]):
                _copy_if_consistent(base, provenance, tax_itis, "ITIS")

        # Fix-up si la clase quedó con clado, reemplaza por ITIS
        if base.get("class_name") in {"Crocodylia", "Elasmobranchii", "Teleostei"} and tax_itis.get("class_name"):
            _set_if(base, provenance, "class_name", tax_itis["class_name"], "ITIS")

    # 2.5) Adopción laxa de rangos altos
    for lbl, taxsrc in (("Catalogue of Life", tax_col), ("WoRMS", tax_wrms), ("ITIS", tax_itis)):
        if isinstance(taxsrc, dict) and taxsrc:
            _lenient_highrank_merge(base, provenance, taxsrc, lbl)

    # 2.6) Fallback por familia (ITIS + CoL + WoRMS) ANTES de reglas
    fam_col = fam_itis = fam_wrms = None
    if (not base.get("order_name") or not base.get("superfamily")) and base.get("family"):
        fam = base["family"]

        try:
            fam_itis = await _itis_hierarchy_by_name(fam)
            if fam_itis:
                _copy_if_consistent(base, provenance, fam_itis, "ITIS")
        except Exception as e:
            if log.isEnabledFor(logging.DEBUG):
                log.debug("ITIS por familia falló: %s", e)

        try:
            fam_col_best = await _col_best(fam)
            if fam_col_best:
                fam_col = _taxonomy_from_col(fam_col_best)
                _copy_if_consistent(base, provenance, fam_col, "Catalogue of Life")
        except Exception as e:
            if log.isEnabledFor(logging.DEBUG):
                log.debug("CoL por familia falló: %s", e)

        try:
            fam_wrms_raw = await _worms_best(fam)
            if fam_wrms_raw:
                fam_wrms = _taxonomy_from_worms(fam_wrms_raw)
                _copy_if_consistent(base, provenance, fam_wrms, "WoRMS")
        except Exception as e:
            if log.isEnabledFor(logging.DEBUG):
                log.debug("WoRMS por familia falló: %s", e)

    # --- Sanity fixes de clase/orden (después de todo lo anterior) ---
    _fix_class_order(base, provenance, tax_itis if isinstance(tax_itis, dict) else None)

    # 2.7) Temáticos/locales + SIB general
    tax_rep = tax_bat = tax_aco = tax_mam = tax_cites = tax_sib = {}

    label_sib = "SIB Colombia"

    more_tasks = [
        _safe_tax("Reptile Database", _tax_from_reptile_db(sci_name)),
        _safe_tax("Batrachia", _tax_from_batrachia(sci_name)),
        _safe_tax("ACO Aves", _tax_from_aco_birds(sci_name)),
        _safe_tax("Mamíferos 2024 (SIB)", _tax_from_mammals_2024(sci_name)),
        _safe_tax("CITES (Species+)", _tax_from_cites(sci_name)),
        _safe_tax(label_sib, _tax_from_sib(sci_name)),
    ]
    try:
        results = await asyncio.gather(*more_tasks, return_exceptions=True)
        for label, dtx in (r for r in results if isinstance(r, tuple)):
            if label == "Reptile Database":        tax_rep = dtx or {}
            elif label == "Batrachia":             tax_bat = dtx or {}
            elif label == "ACO Aves":              tax_aco = dtx or {}
            elif label == "Mamíferos 2024 (SIB)":  tax_mam = dtx or {}
            elif label == "CITES (Species+)":      tax_cites = dtx or {}
            elif label == label_sib:               tax_sib = dtx or {}
    except Exception as e:
        if log.isEnabledFor(logging.DEBUG):
            log.debug("gather temáticos/locales falló: %s", e)

    # Adopción laxa con estas fuentes (si falta class/order)
    for lbl, taxsrc in (("Reptile Database", tax_rep), ("Batrachia", tax_bat),
                        ("ACO Aves", tax_aco), ("Mamíferos 2024 (SIB)", tax_mam),
                        ("CITES (Species+)", tax_cites), (label_sib, tax_sib)):
        if isinstance(taxsrc, dict) and taxsrc:
            _lenient_highrank_merge(base, provenance, taxsrc, lbl)

    # --- Aplica reglas internas (después de merges/fallbacks y antes de reparaciones finales) ---
    _apply_internal_rules(base, provenance)

    # 3) Overrides por fuente más fresca (incluye temáticos/locales y SIB)
    cand_list: list[tuple[str, dict]] = []
    if isinstance(tax_col, dict) and tax_col:    cand_list.append(("Catalogue of Life", tax_col))
    if isinstance(tax_wrms, dict) and tax_wrms:  cand_list.append(("WoRMS", tax_wrms))
    if isinstance(tax_itis, dict) and tax_itis:  cand_list.append(("ITIS", tax_itis))

    # familiares (fallback previos)
    if isinstance(fam_col, dict) and fam_col:    cand_list.append(("Catalogue of Life", fam_col))
    if isinstance(fam_itis, dict) and fam_itis:  cand_list.append(("ITIS", fam_itis))
    if isinstance(fam_wrms, dict) and fam_wrms:  cand_list.append(("WoRMS", fam_wrms))

    # temáticos/locales + SIB
    if isinstance(tax_rep, dict) and tax_rep:    cand_list.append(("Reptile Database", tax_rep))
    if isinstance(tax_bat, dict) and tax_bat:    cand_list.append(("Batrachia", tax_bat))
    if isinstance(tax_aco, dict) and tax_aco:    cand_list.append(("ACO Aves", tax_aco))
    if isinstance(tax_mam, dict) and tax_mam:    cand_list.append(("Mamíferos 2024 (SIB)", tax_mam))
    if isinstance(tax_cites, dict) and tax_cites:cand_list.append(("CITES (Species+)", tax_cites))
    if isinstance(tax_sib, dict) and tax_sib:    cand_list.append((label_sib, tax_sib))

    if cand_list:
        _apply_fresh_overrides(base, provenance, cand_list)
        # --- Limpieza selectiva: reemplazar únicamente campos marcados como RULE* si hay mejores fuentes ---
        _apply_fresh_overrides_only_rules(base, provenance, cand_list)

    # --- Scoring + Quorum (pulido final con desempate determinista) ---
    scored_cands: list[tuple[str, dict, dict]] = []
    for label, tax in cand_list:
        meta = {}
        if label == "Catalogue of Life" and best_col:
            meta["status"] = (best_col.get("status") or (best_col.get("usage") or {}).get("status"))
        elif label == "WoRMS" and best_wrms:
            meta["status"] = (best_wrms.get("status") or best_wrms.get("taxonstatus") or best_wrms.get("valid_name_status"))
        elif label == "ITIS" and tax_itis:
            meta["status"] = "ACCEPTED"
        elif label == "ACO Aves" and tax_aco:
            meta["status"] = "ACCEPTED"
        elif label == "Mamíferos 2024 (SIB)" and tax_mam:
            meta["status"] = "ACCEPTED"
        elif label in {"Reptile Database", "Batrachia", "CITES (Species+)"}:
            meta["status"] = "ACCEPTED"
        elif label == "SIB Colombia":
            meta["status"] = None
        scored_cands.append((label, tax, meta))
    if scored_cands:
        _apply_scores_and_quorum(base, provenance, scored_cands)
        # Trim de decisiones y overrides para contener tamaño
        provenance["_decisions"] = {"_last": provenance.get("_decisions", {}).get("_last")}
        _trim_overrides(provenance)

    # Consenso estable para Gastropoda/Cerithioidea (CoL+WoRMS)
    _prefer_gastropoda_order_if_consensus(base, provenance, tax_col, tax_wrms)

    # 4) IUCN (usar canónico si existe) – wrapper tolerante
    iucn_cat = await _iucn_category_safe(base.get("canonical_name") or sci_name)

    # 5) Epíteto
    epiteto = obtener_epiteto_especifico(
        base.get("canonical_name") or base.get("scientific_name"),
        base.get("rank")
    )

    # 6) Fuentes (provenance + presencia) normalizadas (sin RULE, colapsando X:subtag)
    prov_sources_raw = _collect_sources(provenance)
    presence_raw = await _cached("presence", base.get("canonical_name") or sci_name, lambda: buscar_en_fuentes_externas(base.get("canonical_name") or sci_name))

    prov_sources = { _norm_src_label(s) for s in prov_sources_raw }
    prov_sources.discard(None)

    # >>> NUEVO: filtra presencia por clase resuelta antes de unir con provenance
    presence_filtered = filter_sources_by_class(list(presence_raw or []), base.get("class_name"))
    presence = set(presence_filtered)

    fuentes = sorted(prov_sources | presence, key=lambda x: -LOCAL_SOURCE_WEIGHTS.get(x,0))
    fuentes_csv = ",".join(fuentes) if fuentes else None

    # 7) Avisos + reparaciones de cordura
    _post_sanity_repairs(base, provenance)

    # --- Alinear scientific_name con la autoría elegida (solo para GENUS) ---
    if (str(base.get("rank") or "").upper() == "GENUS"
        and base.get("canonical_name") and base.get("authorship")):
        recomposed = f"{base['canonical_name']} {base['authorship']}"
        if base.get("scientific_name") != recomposed:
            base["scientific_name"] = recomposed
            # etiqueta la misma fuente que decidió la autoría (evita RULE y evita warning)
            provenance["scientific_name"] = provenance.get("authorship") or provenance.get("scientific_name") or "Catalogue of Life"

    # --- Alinear scientific_name con la autoría elegida (species-group) ---
    if (str(base.get("rank") or "").upper() in {"SPECIES", "SUBSPECIES", "VARIETY", "FORM"}
        and base.get("canonical_name") and base.get("authorship")):
        # ¿la cadena actual ya trae paréntesis?
        sci_tail = base.get("scientific_name") or ""
        had_parens = bool(re.search(r"\([^()]*\)\s*$", sci_tail))
        new_tail = f"({base['authorship']})" if had_parens else base["authorship"]
        recomposed = f"{base['canonical_name']} {new_tail}"
        if base.get("scientific_name") != recomposed:
            base["scientific_name"] = recomposed
            # etiqueta con la misma fuente que decidió la autoría
            provenance["scientific_name"] = provenance.get("authorship") or provenance.get("scientific_name") or "Catalogue of Life"

    # >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
    # RE-CÁLCULO FINAL DE _decisions PARA ALINEAR "chosen" CON EL ESTADO FINAL
    # (no toca 'base', solo refresca provenance["_decisions"])
    # >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
    if scored_cands:
        # construimos un decisions ligero usando los mismos candidatos y puntuaciones,
        # pero fijando 'chosen' = base[field] (post reglas/reparaciones)
        decisions: dict[str, dict[str, Any]] = {}
        for field in TAXO_FIELDS:
            props = []
            for label, tax, meta in scored_cands:
                v = tax.get(field)
                if not v:
                    continue
                sc = _field_score(base, tax, label, field, (meta or {}).get("status"))
                props.append({"source": label, "value": v, "score": sc})
            if not props:
                continue
            # top-5 determinista como en _apply_scores_and_quorum
            if len(props) > 5:
                props = sorted(props, key=lambda x: (-x["score"], str(x["value"])))[:5]
            decisions[field] = {"candidates": props, "chosen": base.get(field)}
        provenance["_decisions"] = {"_last": decisions}
    # <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

    warnings = _build_warnings(base, provenance)

    # 8) UPSERT
    existente: Optional[Taxon] = db.execute(
        select(Taxon).where(Taxon.gbif_key == usage_key)
    ).scalars().first()

    if existente:
        for k, v in base.items():
            if v is not None and getattr(existente, k) != v:
                setattr(existente, k, v)
        if epiteto:
            existente.epiteto_especifico = epiteto
        if iucn_cat:
            existente.iucn_category = iucn_cat
        if fuentes_csv and existente.sources_csv != fuentes_csv:
            existente.sources_csv = fuentes_csv

        old_prov: Dict[str, Any] = {}
        if existente.provenance_json:
            try:
                old_prov = json.loads(existente.provenance_json)
            except Exception:
                old_prov = {}
        # merge simple
        old_prov.update(provenance)
        # Recalcular warnings frescos según el provenance actual
        fresh_warnings = _build_warnings(base, old_prov)
        if fresh_warnings:
            old_prov["_warnings"] = fresh_warnings
        else:
            old_prov.pop("_warnings", None)
        # también aplicamos trimming al persistir
        old_prov["_decisions"] = {"_last": old_prov.get("_decisions", {}).get("_last")}
        _trim_overrides(old_prov)

        existente.provenance_json = json.dumps(old_prov) if old_prov else None

        db.commit(); db.refresh(existente)
        taxon_obj = existente
    else:
        prov_json: Dict[str, Any] = dict(provenance)
        if warnings:
            prov_json["_warnings"] = warnings
        prov_json["_decisions"] = {"_last": prov_json.get("_decisions", {}).get("_last")}
        _trim_overrides(prov_json)

        taxon_obj = Taxon(
            **base,
            epiteto_especifico=epiteto,
            iucn_category=iucn_cat,
            sources_csv=fuentes_csv,
            provenance_json=json.dumps(prov_json) if prov_json else None,
        )
        db.add(taxon_obj); db.commit(); db.refresh(taxon_obj)

    # 9) Sinónimos GBIF (sin abrir una transacción manual: usar commit/rollback explícitos)
    syns = await _gbif_synonyms(usage_key)  # << fuera de la transacción
    try:
        db.execute(delete(Synonym).where(Synonym.taxon_id == taxon_obj.id))

        if syns:
            db.add_all([
                Synonym(
                    taxon_id=taxon_obj.id,
                    name=s.get("scientificName"),
                    authorship=s.get("authorship"),
                    status=s.get("taxonomicStatus") or s.get("status") or "SYNONYM",
                    source="GBIF",
                    external_key=str(s.get("key") or ""),
                    rank=s.get("rank"),
                    accepted_name=taxon_obj.scientific_name,
                )
                for s in syns
            ])

        db.commit()
    except SQLAlchemyError:
        db.rollback()
        # Stack trace útil en WARNING/ERROR:
        log.exception("Persistencia de sinónimos falló para taxon_id=%s", getattr(taxon_obj, "id", None))

    return taxon_obj

