-- Migration: Create pc_fornecedores table for manual purchase order supplier catalog
-- Date: 2026-04-13

CREATE TABLE IF NOT EXISTS pc_fornecedores (
    id SERIAL PRIMARY KEY,
    empresa VARCHAR(50) NOT NULL,
    nome VARCHAR(200) NOT NULL,
    prazo_padrao_dias INTEGER NOT NULL DEFAULT 30,
    criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(empresa, nome)
);

CREATE INDEX IF NOT EXISTS idx_pc_fornecedores_empresa ON pc_fornecedores(empresa);

SELECT 'pc_fornecedores table created' AS status;
