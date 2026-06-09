-- Migration: Create cadastros_sku table (missing from Shopgra database)
-- Date: 2026-06-05
-- Context: Tabela de cadastro mestre de SKUs usada em pmv, sopc, pedidos-compra, agentes.
--          Era importada via upload de planilha (sem CREATE TABLE formal).
--          Colunas identificadas por varredura do código-fonte.

CREATE TABLE IF NOT EXISTS cadastros_sku (
  id             SERIAL PRIMARY KEY,
  "Sku"          TEXT,
  "Nome Produto" TEXT,
  "Marca"        TEXT,
  "Custo Un"     NUMERIC(18,4),
  repasse        NUMERIC(18,2),
  criado_em      TIMESTAMP DEFAULT NOW()
);

-- Índice para buscas por SKU (case-insensitive via TRIM)
CREATE INDEX IF NOT EXISTS idx_cadastros_sku_sku
  ON cadastros_sku (TRIM("Sku"::text));

-- Índice para filtros por Marca
CREATE INDEX IF NOT EXISTS idx_cadastros_sku_marca
  ON cadastros_sku (TRIM("Marca"));

SELECT 'cadastros_sku created (or already exists)' AS status;
