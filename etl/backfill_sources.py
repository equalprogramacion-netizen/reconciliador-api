from sqlalchemy import select
from app.db import SessionLocal, engine
from app.models import Base, Taxon
from app.services.reconcile import buscar_en_fuentes_externas

def run():
    Base.metadata.create_all(bind=engine)
    with SessionLocal() as db:
        rows = db.execute(select(Taxon).where(Taxon.sources_csv.is_(None))).scalars().all()
        print(f"Pendientes: {len(rows)}")
        for t in rows:
            try:
                fuentes = asyncio_run(buscar_en_fuentes_externas(t.scientific_name))
                if fuentes:
                    t.sources_csv = ",".join(fuentes)
            except Exception as e:
                print("Error con", t.scientific_name, ":", e)
        db.commit()
        print("Listo.")

def asyncio_run(coro):
    import asyncio, sys
    if sys.platform.startswith("win"):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    return asyncio.run(coro)

if __name__ == "__main__":
    run()
