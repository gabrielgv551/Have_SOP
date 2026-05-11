-- Migration: Add indexes to bd_vendas for Caixa Fluxo Diário performance
-- Run: psql -h <HOST> -U <USER> -d <DB> -f migrations/018_index_bd_vendas.sql

-- Index on "Data" for date range queries (A Receber, repasse avg)
CREATE INDEX IF NOT EXISTS idx_bd_vendas_data
    ON bd_vendas ("Data");

-- Composite index for the A Receber query pattern (Data + Status)
CREATE INDEX IF NOT EXISTS idx_bd_vendas_data_status
    ON bd_vendas ("Data", "Status");

-- Index for caixa_extrato full-year queries
CREATE INDEX IF NOT EXISTS idx_caixa_extrato_ano
    ON caixa_extrato (empresa, ano);

SELECT 'Indexes created successfully' AS status;
