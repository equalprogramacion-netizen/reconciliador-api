import sys, pandas as pd
from app.db import SessionLocal, engine
from app.models import Base
from app.services.reconcile import reconcile_name, obtener_epiteto_especifico

def run(input_path: str, output_path: str):
    Base.metadata.create_all(bind=engine)

    if input_path.lower().endswith(".csv"):
        df = pd.read_csv(input_path)
    else:
        df = pd.read_excel(input_path)

    if "scientific_name" not in df.columns and "nombre_cientifico" not in df.columns:
        raise ValueError("El archivo debe tener una columna 'scientific_name' o 'nombre_cientifico'.")

    nombres = df["scientific_name"] if "scientific_name" in df.columns else df["nombre_cientifico"]

    filas = []
    with SessionLocal() as db:
        for nombre in nombres.fillna(""):
            if not nombre:
                continue
            t = asyncio_run(reconcile_name(db, nombre))
            epiteto = obtener_epiteto_especifico(t.canonical_name or t.scientific_name, t.rank)
            filas.append({
                "nombre_original": nombre,
                "nombre_cientifico": t.scientific_name,
                "epiteto_especifico": epiteto,
                "estado": t.status,
                "clave_gbif": t.gbif_key,
                "clave_aceptada_gbif": t.accepted_gbif_key,
                "rango": t.rank,
                "categoria_iucn": t.iucn_category,
                "fuentes": t.sources_csv
            })

    out = pd.DataFrame(filas)
    if output_path.lower().endswith(".xlsx"):
        out.to_excel(output_path, index=False)
    else:
        out.to_csv(output_path, index=False)
    print(f"Exportado: {output_path}")

def asyncio_run(coro):
    import asyncio, sys
    if sys.platform.startswith("win"):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    return asyncio.run(coro)

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Uso: python etl/reconcile_file.py data/input_sample.csv data/salida.csv")
        sys.exit(1)
    run(sys.argv[1], sys.argv[2])
