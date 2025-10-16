# app/clients/sib_mammals_2024.py
from __future__ import annotations
from sqlalchemy import select
from ..db import SessionLocal
from ..models import Taxon

# AJUSTA este valor al que uses en el ingestor SIB mamíferos 2024
SOURCE_TAG = "SIB-Mammals-2024"

# Firma unificada
async def exists(q: str | None = None) -> bool:
    db = SessionLocal()
    try:
        qy = select(Taxon.id).where(Taxon.sources_csv.like(f"%{SOURCE_TAG}%")).limit(1)
        row = db.execute(qy).first()
        return bool(row)
    finally:
        db.close()
