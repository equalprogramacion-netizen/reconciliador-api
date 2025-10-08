-- Añadir columna para fuentes y poblarla opcionalmente
ALTER TABLE taxon ADD COLUMN sources_csv TEXT NULL AFTER iucn_category;
-- No se requiere índice.
