# app/clients/aco_birds.py
from __future__ import annotations
from sqlalchemy import select
from ..db import SessionLocal
from ..models import Taxon

# Debe coincidir con el tag/valor que el ingestor ACO deja en Taxon.sources_csv
SOURCE_TAG = "ACO-Aves"

# Firma unificada
async def exists(q: str | None = None) -> bool:
    db = SessionLocal()
    try:
        qy = select(Taxon.id).where(Taxon.sources_csv.like(f"%{SOURCE_TAG}%")).limit(1)
        row = db.execute(qy).first()
        return bool(row)
    finally:
        db.close()
