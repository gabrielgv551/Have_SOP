-- Migration 045: Create sku_kits table for kit-to-component mapping
-- Used by S&OP pipeline (forecast, safety stock, reorder point)
-- Date: 2026-05-27

CREATE TABLE IF NOT EXISTS sku_kits (
  id             SERIAL PRIMARY KEY,
  empresa        VARCHAR(50)    NOT NULL,
  sku_kit        TEXT           NOT NULL,
  sku_componente TEXT           NOT NULL,
  quantidade     NUMERIC(10,4)  NOT NULL DEFAULT 1,
  ativo          BOOLEAN        NOT NULL DEFAULT true,
  criado_em      TIMESTAMP      DEFAULT NOW(),
  CONSTRAINT sku_kits_unique UNIQUE (empresa, sku_kit, sku_componente)
);

CREATE INDEX IF NOT EXISTS idx_sku_kits_empresa       ON sku_kits(empresa);
CREATE INDEX IF NOT EXISTS idx_sku_kits_kit           ON sku_kits(empresa, sku_kit);
CREATE INDEX IF NOT EXISTS idx_sku_kits_componente    ON sku_kits(empresa, sku_componente);

SELECT 'sku_kits table created' AS status;
