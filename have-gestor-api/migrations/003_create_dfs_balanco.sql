-- Migration: Create dfs_balanco table for monthly financial balance data
-- Date: 2026-03-26
-- Run: psql -h 37.60.236.200 -U postgres -d Lanzi -f migrations/003_create_dfs_balanco.sql

CREATE TABLE IF NOT EXISTS dfs_balanco (
    id SERIAL PRIMARY KEY,
    empresa VARCHAR(50) NOT NULL,
    ano INTEGER NOT NULL,
    mes INTEGER NOT NULL CHECK (mes BETWEEN 1 AND 12),
    conta VARCHAR(100) NOT NULL,
    nome VARCHAR(255) NOT NULL,
    saldo_anterior NUMERIC(18,2) DEFAULT 0,
    debito NUMERIC(18,2) DEFAULT 0,
    credito NUMERIC(18,2) DEFAULT 0,
    saldo_atual NUMERIC(18,2) DEFAULT 0,
    criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    atualizado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Unique constraint: one row per company/year/month/account
CREATE UNIQUE INDEX IF NOT EXISTS idx_dfs_balanco_unique
    ON dfs_balanco(empresa, ano, mes, conta);

-- Indices for performance
CREATE INDEX IF NOT EXISTS idx_dfs_balanco_empresa ON dfs_balanco(empresa);
CREATE INDEX IF NOT EXISTS idx_dfs_balanco_periodo ON dfs_balanco(empresa, ano, mes);

-- Auto-update atualizado_em on changes
CREATE TRIGGER trigger_dfs_balanco_atualizado_em
BEFORE UPDATE ON dfs_balanco
FOR EACH ROW
EXECUTE FUNCTION update_atualizado_em();

SELECT 'dfs_balanco table created successfully' as status;
