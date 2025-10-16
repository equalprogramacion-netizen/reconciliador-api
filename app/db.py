# app/db.py
import os
from dotenv import load_dotenv
load_dotenv()

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL no configurada. Revise el archivo .env")

# Ajustes clave:
# - SSL vacío: le dice a PyMySQL "usa TLS" (suficiente para muchos proxies)
# - connect_timeout para no colgar el arranque
# - pool_recycle menor que el wait_timeout del server/proxy (p.ej. 280s)
connect_args = {
    "ssl": {},               # <<--- TLS requerido por el proxy
    "connect_timeout": 10,
    "charset": "utf8mb4",
}

engine = create_engine(
    DATABASE_URL,
    future=True,
    pool_pre_ping=True,
    pool_recycle=280,        # mejor que 3600 en proxies
    pool_size=5,
    max_overflow=10,
    connect_args=connect_args,
)

SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
