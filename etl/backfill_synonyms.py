# etl/backfill_synonyms.py
import asyncio
import httpx
from sqlalchemy import select, delete
from app.db import SessionLocal
from app.models import Taxon, Synonym


async def fetch_synonyms_gbif(usage_key: int):
    """Descarga sinónimos desde GBIF para una usageKey."""
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.get(f"https://api.gbif.org/v1/species/{usage_key}/synonyms")
        r.raise_for_status()
        j = r.json()
        return j if isinstance(j, list) else j.get("results", [])


async def process_one(session, taxon: Taxon):
    """Borra y repuebla los sinónimos de un taxón."""
    syns = await fetch_synonyms_gbif(taxon.gbif_key)

    # Elimina sinónimos previos del taxón
    session.execute(delete(Synonym).where(Synonym.taxon_id == taxon.id))

    for s in syns or []:
        session.add(
            Synonym(
            taxon_id=taxon.id,
            name=s.get("scientificName"),
            authorship=s.get("authorship"),
            status=s.get("taxonomicStatus") or s.get("status") or "SYNONYM",
            source="GBIF",
            external_key=str(s.get("key") or ""),
            # 👇 clave: el atributo del modelo es 'rank'
            rank=s.get("rank"),
            accepted_name=taxon.scientific_name,
            )
        )

    session.commit()


async def main():
    # Carga “streaming” + unique() para evitar el error de eager loads
    with SessionLocal() as session:
        stmt = select(Taxon).where(Taxon.gbif_key.isnot(None))
        taxa_iter = session.execute(stmt).unique().scalars()

        count = 0
        print("Iniciando backfill de sinónimos desde GBIF…")

        for t in taxa_iter:
            try:
                await process_one(session, t)
                count += 1
                print(f"OK [{count}]: {t.scientific_name}")
                # Si quieres ser amable con la API, descomenta:
                # await asyncio.sleep(0.05)
            except Exception as e:
                print("ERR:", t.scientific_name, e)

        print(f"Terminado. Total procesados: {count}")


if __name__ == "__main__":
    asyncio.run(main())
