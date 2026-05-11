-- Migration: Create pedidos_compra table for purchase orders
-- Date: 2026-04-16

CREATE TABLE IF NOT EXISTS pedidos_compra (
    id SERIAL PRIMARY KEY,
    empresa VARCHAR(50) NOT NULL,
    fornecedor VARCHAR(200),
    data_emissao DATE,
    nf VARCHAR(100),
    valor NUMERIC(18,2),
    vencimento DATE,
    vencimento_ajustado DATE,
    parcela INTEGER,
    total_parcelas INTEGER,
    linha_fluxo VARCHAR(200),
    repasse NUMERIC(18,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_pedidos_compra_empresa ON pedidos_compra(empresa);
CREATE INDEX IF NOT EXISTS idx_pedidos_compra_vencimento ON pedidos_compra(empresa, COALESCE(vencimento_ajustado, vencimento));

SELECT 'pedidos_compra table created' AS status;
