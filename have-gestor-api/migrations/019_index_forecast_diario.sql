-- Migration: Add indexes to forecast_diario for Caixa Fluxo Diário performance
-- Run: psql -h <HOST> -U <USER> -d <DB> -f migrations/019_index_forecast_diario.sql

-- Index on data range queries used in consolidarAnual
CREATE INDEX IF NOT EXISTS idx_forecast_diario_data
    ON forecast_diario (data);

-- Composite for the canal+data filter pattern
CREATE INDEX IF NOT EXISTS idx_forecast_diario_canal_data
    ON forecast_diario (canal, data);

SELECT 'Indexes created successfully' AS status;
