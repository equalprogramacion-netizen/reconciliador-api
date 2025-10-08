# app/services/reconcile.py
from __future__ import annotations

from typing import List, Optional, Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy import select, delete
import re, unicodedata, asyncio, json
import httpx

from ..models import Taxon, Synonym
from ..clients import gbif, iucn, col  # worms/itis/sib se importan lazy y solo si se necesitan


# --------------------- Utilidades de normalización ---------------------
def _quitar_tildes(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))

def normaliza_nombre(q: str) -> str:
    """
    - elimina autores finales "(Linnaeus, 1758)"
    - colapsa espacios
    - Género Capitalizado; resto minúsculas
    - elimina acentos
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
        "Crocodilia": "Crocodylia",      # normaliza a 'y'
        "Teleostei": "Actinopterygii",   # clado -> clase
        "Elasmobranchii": "Chondrichthyes",  # subclase/clado -> clase
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


# ----------------------- Avisos / disclaimers -----------------------
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
            + ", ".join(rule_fields) + ". Recomendado validar con GBIF/ITIS/CoL."
        )

    # 2) Estatus dudoso
    st = (base.get("status") or "").upper()
    if st in UNCERTAIN_STATUSES:
        warns.append(f"El estatus del taxón está marcado como {st} en GBIF; revisar aceptación/sinonimia.")

    # 3) Sin clave GBIF (registro mínimo)
    if not base.get("gbif_key"):
        warns.append("No se obtuvo usageKey de GBIF; se guardó un registro mínimo con fuentes alternativas.")

    return warns

# --- Recolector seguro de fuentes desde provenance (evita TypeError) ---
def _collect_sources(prov: dict | None) -> list[str]:
    """
    Extrae etiquetas de fuente desde provenance sin romper si hay dicts/listas.
    Normaliza 'PREFERRED:Fuente' -> 'Fuente'. Filtra RULE*.
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
    cleaned = []
    for s in raw:
        if s.startswith("PREFERRED:"):
            s = s.split(":", 1)[1]
        if s.startswith("RULE"):
            continue  # no meter reglas en fuentes
        cleaned.append("Catalogue of Life" if s == "CoL" else s)
    return sorted(set(cleaned))


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


# ----------------------------- Compatibilidad de clasificación (A) -----------------------------
def _compatible_taxonomy(base: Dict[str, Any], cand: Dict[str, Any]) -> bool:
    """
    Coincidencias en >=2 (phylum, class, order, family, genus)
    PERO si ambos traen family y difiere, no es compatible.
    Además, exigimos match en family o genus cuando ambos existan.
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
        data = await col.search_name(nombre)
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
                    detail = await col._detail_for_usage(usage_id)  # type: ignore[attr-defined]
                except Exception:
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
    except Exception:
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
        if isinstance(n, str): n = n.strip()
        else: n = str(n).strip()
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

async def _worms_best(nombre: str) -> Dict[str, Any] | None:
    try:
        from ..clients import worms
        arr = await worms.search_name(nombre)
        if isinstance(arr, list) and arr:
            return arr[0]
    except Exception:
        pass
    return None

def _taxonomy_from_worms(item: dict) -> dict:
    res: Dict[str,str] = {}
    if not isinstance(item, dict):
        return res
    for src, dst in MAP_WRMS.items():
        v = item.get(src)
        if v:
            res[dst] = v
    return res


# ----------------------------- Preferencias por "fuente más actual" -----------------------------
SOURCE_PRIORITY = {
    # mayor es "más fresco": CoL (mensual) > WoRMS (curado continuo marino) > ITIS > GBIF > RULE
    "Catalogue of Life": 4,
    "CoL": 4,  # compat
    "WoRMS": 3,
    "ITIS": 2,
    "GBIF": 1,
    "GBIF:detail": 1,
    "RULE": 0,  # cualquier prefijo RULE*
}

def _src_rank(src: str) -> int:
    if not isinstance(src, str) or not src:
        return -1
    if src.startswith("PREFERRED:"):
        src = src.split(":", 1)[1]
    base = src.split(":", 1)[0]
    if base == "CoL":
        base = "Catalogue of Life"
    return SOURCE_PRIORITY.get(base, 0)

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
    except Exception:
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
        except Exception:
            hlist = None

    if not hlist:
        try:
            up = await itis_client.get_hierarchy_up(tsn)
            if isinstance(up, dict):
                hlist = up.get("hierarchyList") or up.get("hierarchy") or None
        except Exception:
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
        d = await itis.search_by_scientific_name(nombre)
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
            async with httpx.AsyncClient(timeout=12) as client:
                r = await client.get(url, params=params)
            return r.json() if r.status_code < 400 else {}

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
    except Exception:
        return {}

# ----------------------------- Marcadores de presencia (solo etiquetas) -----------------------------
async def _found_in_worms(nombre: str) -> bool:
    try:
        from ..clients import worms
        w = await worms.search_name(nombre)
        return isinstance(w, list) and len(w) > 0
    except Exception:
        return False

async def _found_in_itis(nombre: str) -> bool:
    try:
        from ..clients import itis
        d = await itis.search_by_scientific_name(nombre)
        if not isinstance(d, dict):
            return False
        return bool(d.get("scientificNames") or d.get("acceptedNameList"))
    except Exception:
        return False

async def _found_in_col(nombre: str) -> bool:
    try:
        data = await col.search_name(nombre)
        return isinstance(data, dict) and ((data.get("total") or 0) > 0)
    except Exception:
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
    except Exception:
        return False

async def buscar_en_fuentes_externas(nombre: str) -> List[str]:
    nombre_n = normaliza_nombre(nombre)
    checks: List[tuple[str, asyncio.Task]] = [
        ("Catalogue of Life", asyncio.create_task(_found_in_col(nombre_n))),
        ("WoRMS",             asyncio.create_task(_found_in_worms(nombre_n))),
        ("ITIS",              asyncio.create_task(_found_in_itis(nombre_n))),
        ("SIB Colombia",      asyncio.create_task(_found_in_sib(nombre_n))),
    ]
    halladas: List[str] = []
    await asyncio.gather(*(t for _, t in checks), return_exceptions=True)
    for label, task in checks:
        try:
            if task.result():
                halladas.append(label)
        except Exception:
            pass
    return halladas


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

async def _gbif_synonyms(usage_key: int) -> list[dict]:
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.get(f"https://api.gbif.org/v1/species/{usage_key}/synonyms")
            if r.status_code < 400:
                j = r.json()
                if isinstance(j, dict) and "results" in j:
                    return j["results"]
                if isinstance(j, list):
                    return j
    except Exception:
        pass
    return []

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

async def _gbif_detail(usage_key: int) -> dict:
    try:
        async with httpx.AsyncClient(timeout=12) as client:
            r = await client.get(f"https://api.gbif.org/v1/species/{usage_key}")
            return r.json() if r.status_code < 400 else {}
    except Exception:
        return {}

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


# ----------------------------- Reconciliación principal -----------------------------
async def reconcile_name(db: Session, name: str) -> Taxon:
    """
    Flujo:
      1) GBIF species_match
      1.1) GBIF detail: completar class/order si faltan (autoridad)
      2) Enriquecimiento: CoL → WoRMS → ITIS (para cualquier campo faltante)
      2.5) Adopción laxa / Fallback por familia (ITIS + CoL + WoRMS)
      3) Overrides por "fuente más actual" (CoL > WoRMS > ITIS) con guardas
      4) IUCN
      5) Epíteto
      6) Marcas de otras fuentes
      7) UPSERT en DB
      8) Sinónimos GBIF
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
        fuentes = await buscar_en_fuentes_externas(name_n)
        prov_json: Dict[str, Any] = {"scientific_name": "INPUT"}
        warnings_min = _build_warnings({"gbif_key": None, "status": (m or {}).get("status")}, prov_json)
        if warnings_min:
            prov_json["_warnings"] = warnings_min

        t = Taxon(
            scientific_name=name_n,
            status=None,
            gbif_key=None,
            sources_csv=",".join(fuentes) if fuentes else None,
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
        "scientific_name": sci_name,
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

    # --- GBIF detail: completar orden/clase si faltan (autoridad)
    if (not base.get("order_name")) or (not base.get("class_name")):
        det0 = await _gbif_detail(usage_key)
        if isinstance(det0, dict):
            if not base.get("order_name") and det0.get("order"):
                _set_if(base, provenance, "order_name", det0.get("order"), "GBIF:detail")
            if not base.get("class_name") and det0.get("class"):
                _set_if(base, provenance, "class_name", det0.get("class"), "GBIF:detail")

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
                col_author = _col_authorship(best_col)
                if not base.get("authorship") and col_author:
                    _set_if(base, provenance, "authorship", col_author, "Catalogue of Life")

    # ---- WoRMS
    best_wrms = await _worms_best(sci_name)
    if best_wrms:
        tax_wrms = _taxonomy_from_worms(best_wrms)
        if _faltantes(base):
            if _compatible_taxonomy(base, tax_wrms) or all(base.get(k) is None for k in ["phylum","class_name","order_name","family","genus"]):
                _copy_if_consistent(base, provenance, tax_wrms, "WoRMS")
                _set_if(base, provenance, "authorship", best_wrms.get("authority"), "WoRMS")

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
    except Exception:
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
        except Exception:
            pass

        try:
            fam_col_best = await _col_best(fam)
            if fam_col_best:
                fam_col = _taxonomy_from_col(fam_col_best)
                _copy_if_consistent(base, provenance, fam_col, "Catalogue of Life")
        except Exception:
            pass

        try:
            fam_wrms_raw = await _worms_best(fam)
            if fam_wrms_raw:
                fam_wrms = _taxonomy_from_worms(fam_wrms_raw)
                _copy_if_consistent(base, provenance, fam_wrms, "WoRMS")
        except Exception:
            pass

    # --- Sanity fixes de clase/orden (después de todo lo anterior) ---
    _fix_class_order(base, provenance, tax_itis if isinstance(tax_itis, dict) else None)

    # 3) Overrides por fuente más fresca (CoL > WoRMS > ITIS), con guardas
    cand_list: list[tuple[str, dict]] = []
    if isinstance(tax_col, dict) and tax_col:
        cand_list.append(("Catalogue of Life", tax_col))
    if isinstance(tax_wrms, dict) and tax_wrms:
        cand_list.append(("WoRMS", tax_wrms))
    if isinstance(tax_itis, dict) and tax_itis:
        cand_list.append(("ITIS", tax_itis))
    # incluir también taxonomías derivadas por familia
    if isinstance(fam_col, dict) and fam_col:
        cand_list.append(("Catalogue of Life", fam_col))
    if isinstance(fam_itis, dict) and fam_itis:
        cand_list.append(("ITIS", fam_itis))
    if isinstance(fam_wrms, dict) and fam_wrms:
        cand_list.append(("WoRMS", fam_wrms))

    if cand_list:
        _apply_fresh_overrides(base, provenance, cand_list)

    # 4) Fallback de autoría desde detalle GBIF (si aún falta)
    if not base.get("authorship") and usage_key:
        det = await _gbif_detail(usage_key)
        a = det.get("authorship")
        if a:
            _set_if(base, provenance, "authorship", a, "GBIF:detail")

    # 5) Reglas internas auditables (último recurso)
    _apply_internal_rules(base, provenance)

    # 5.1) Segunda pasada de overrides para pisar cualquier RULE con fuentes reales
    if cand_list:
        _apply_fresh_overrides(base, provenance, cand_list)

    # 5.2) Reintentar clase a partir del orden tras todo
    _fix_class_order(base, provenance, tax_itis if isinstance(tax_itis, dict) else None)

    # 6) Reparaciones curatoriales extra
    _post_sanity_repairs(base, provenance)

    # 7) IUCN (usar canónico si existe)
    iucn_cat = await iucn.category_by_name(base.get("canonical_name") or sci_name)

    # 8) Epíteto
    epiteto = obtener_epiteto_especifico(
        base.get("canonical_name") or base.get("scientific_name"),
        base.get("rank")
    )

    # 9) Fuentes (provenance + presencia; sin RULE; normaliza CoL)
    prov_sources_raw = _collect_sources(provenance)
    presence_raw = await buscar_en_fuentes_externas(base.get("canonical_name") or sci_name)

    def _norm_src_label(x: str) -> Optional[str]:
        if not x: return None
        if x.startswith("RULE"): return None
        return "Catalogue of Life" if x == "CoL" else x

    prov_sources = { _norm_src_label(s) for s in prov_sources_raw }
    presence = { _norm_src_label(s) for s in presence_raw }
    prov_sources.discard(None); presence.discard(None)

    fuentes = sorted(prov_sources | presence)
    fuentes_csv = ",".join(fuentes) if fuentes else None

    # 10) Avisos
    warnings = _build_warnings(base, provenance)

    # 11) UPSERT
    existente: Optional[Taxon] = db.execute(
        select(Taxon).where(Taxon.gbif_key == usage_key)
    ).scalars().first()

    if existente:
        for k, v in base.items():
            if v is not None:
                setattr(existente, k, v)
        if epiteto:
            existente.epiteto_especifico = epiteto
        if iucn_cat:
            existente.iucn_category = iucn_cat
        if fuentes_csv:
            existente.sources_csv = fuentes_csv

        old_prov: Dict[str, Any] = {}
        if existente.provenance_json:
            try:
                old_prov = json.loads(existente.provenance_json)
            except Exception:
                old_prov = {}
        # merge simple
        old_prov.update(provenance)
        if warnings:
            ws = set(old_prov.get("_warnings", [])) | set(warnings)
            old_prov["_warnings"] = sorted(ws)
        existente.provenance_json = json.dumps(old_prov) if old_prov else None

        db.commit(); db.refresh(existente)
        taxon_obj = existente
    else:
        prov_json: Dict[str, Any] = dict(provenance)
        if warnings:
            prov_json["_warnings"] = warnings

        taxon_obj = Taxon(
            **base,
            epiteto_especifico=epiteto,
            iucn_category=iucn_cat,
            sources_csv=fuentes_csv,
            provenance_json=json.dumps(prov_json) if prov_json else None,
        )
        db.add(taxon_obj); db.commit(); db.refresh(taxon_obj)

    # 12) Sinónimos GBIF (limpiamos y volvemos a insertar)
    try:
        syns = await _gbif_synonyms(usage_key)
        db.execute(delete(Synonym).where(Synonym.taxon_id == taxon_obj.id))
        for s in syns or []:
            sn = Synonym(
                taxon_id=taxon_obj.id,
                name=s.get("scientificName"),
                authorship=s.get("authorship"),
                status=s.get("taxonomicStatus") or s.get("status") or "SYNONYM",
                source="GBIF",
                external_key=str(s.get("key") or ""),
                rank=s.get("rank"),
                accepted_name=taxon_obj.scientific_name,
            )
            db.add(sn)
        db.commit()
    except Exception:
        pass

    return taxon_obj
