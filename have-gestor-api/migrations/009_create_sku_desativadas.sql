CREATE TABLE IF NOT EXISTS sku_desativadas (
    id         SERIAL PRIMARY KEY,
    empresa    VARCHAR(50) NOT NULL,
    sku        VARCHAR(200) NOT NULL,
    criado_em  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(empresa, sku)
);
CREATE INDEX IF NOT EXISTS idx_sku_desativadas_empresa ON sku_desativadas(empresa);
