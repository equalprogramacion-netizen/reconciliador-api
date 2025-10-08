Archivos corregidos para:
- API en español (/reconciliar) con epíteto y fuentes adicionales (CoL, WoRMS, ITIS)
- Endpoint /fuentes/check
- Guardar 'epiteto_especifico' en BD
- Modelos actualizados
- ETL con salida en español e inclusión del epíteto
- Carga de .env en db.py

PASOS:
1) En MySQL Workbench, ejecutar 'db_migration.sql' (una vez).
2) Copiar estos archivos sobre tu proyecto existente (respetando rutas).
3) Reiniciar el servidor:
   uvicorn app.main:app --reload
4) Probar en http://localhost:8000/docs los endpoints:
   - /reconciliar?q=Ateles belzebuth
   - /fuentes/check?q=Ateles belzebuth
5) (Opcional) Añadir 'cryptography' a requirements.txt si no lo tienes:
   pip install cryptography
