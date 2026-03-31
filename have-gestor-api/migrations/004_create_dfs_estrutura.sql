-- Migration: Create dfs_estrutura table for balance sheet structure and mappings
-- Date: 2026-03-29
-- Run: psql -h 37.60.236.200 -U postgres -d Lanzi -f migrations/004_create_dfs_estrutura.sql

CREATE TABLE IF NOT EXISTS dfs_estrutura (
    id SERIAL PRIMARY KEY,
    empresa VARCHAR(50) NOT NULL,
    tipo VARCHAR(20) NOT NULL CHECK (tipo IN ('structure', 'mappings')),
    dados JSONB DEFAULT '{}',
    atualizado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(empresa, tipo)
);

CREATE INDEX IF NOT EXISTS idx_dfs_estrutura_empresa ON dfs_estrutura(empresa);

SELECT 'dfs_estrutura table created successfully' as status;
