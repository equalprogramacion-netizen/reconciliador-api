-- Ejecutar en MySQL 8.x (puedes ejecutar por partes si tu versión no soporta IF NOT EXISTS)

-- 1) Añadir columna de epíteto (si aún no existe)
ALTER TABLE taxon ADD COLUMN IF NOT EXISTS epiteto_especifico VARCHAR(128) NULL AFTER canonical_name;

-- 2) Índice (si ya existe, puedes ignorar el error)
CREATE INDEX ix_taxon_epiteto ON taxon (epiteto_especifico);

-- 3) Rellenar epíteto para registros existentes
UPDATE taxon
SET epiteto_especifico = TRIM(
  SUBSTRING_INDEX(
    SUBSTRING_INDEX(COALESCE(canonical_name, scientific_name), ' ', 2),
    ' ', -1
  )
)
WHERE `rank` IN ('SPECIES','SUBSPECIES')
  AND COALESCE(canonical_name, scientific_name) LIKE '% %';

-- 4) Añadir columna de fuentes (si aún no existe)
ALTER TABLE taxon ADD COLUMN IF NOT EXISTS sources_csv TEXT NULL AFTER iucn_category;
