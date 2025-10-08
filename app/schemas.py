# app/schemas.py
from __future__ import annotations

from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field

# ------------------ Modelos de apoyo ------------------

class SynonymOut(BaseModel):
    name: Optional[str] = None
    authorship: Optional[str] = None
    status: Optional[str] = None
    source: Optional[str] = None
    external_key: Optional[str] = None
    rank: Optional[str] = None
    accepted_name: Optional[str] = None

    # Pydantic v2: leer desde objetos ORM si se requiere
    model_config = {"from_attributes": True}

# Detalle completo usado por /reconciliar/detalle (no estrictamente requerido por main.py)
class TaxonDetailOut(BaseModel):
    scientific_name: Optional[str] = None
    canonical_name: Optional[str] = None
    authorship: Optional[str] = None

    rank: Optional[str] = None
    status: Optional[str] = None

    gbif_key: Optional[int] = None
    accepted_gbif_key: Optional[int] = None
    iucn_category: Optional[str] = None

    # Jerarquía
    kingdom: Optional[str] = None
    phylum: Optional[str] = None
    class_name: Optional[str] = None
    order_name: Optional[str] = None
    superfamily: Optional[str] = None
    family: Optional[str] = None
    subfamily: Optional[str] = None
    tribe: Optional[str] = None
    subtribe: Optional[str] = None
    genus: Optional[str] = None
    subgenus: Optional[str] = None

    # Otros
    epiteto_especifico: Optional[str] = None
    fuentes: List[str] = Field(default_factory=list)
    provenance: Dict[str, Any] = Field(default_factory=dict)

    synonyms: List[SynonymOut] = Field(default_factory=list)

    model_config = {"from_attributes": True}

# ------------------ Modelos que usa main.py ------------------

# /reconcile → compacto (inglés)
class TaxonOut(BaseModel):
    scientific_name: str
    status: Optional[str] = None
    accepted_gbif_key: Optional[int] = None
    gbif_key: Optional[int] = None
    rank: Optional[str] = None
    iucn_category: Optional[str] = None

    model_config = {"from_attributes": True}

# /reconciliar → compacto (español)
class TaxonESOut(BaseModel):
    nombre_cientifico: str
    estado: Optional[str] = None
    epiteto_especifico: Optional[str] = None
    clave_gbif: Optional[int] = None
    clave_aceptada_gbif: Optional[int] = None
    rango: Optional[str] = None
    categoria_iucn: Optional[str] = None
    fuentes: List[str] = Field(default_factory=list)

    model_config = {"from_attributes": True}

__all__ = ["SynonymOut", "TaxonDetailOut", "TaxonOut", "TaxonESOut"]
