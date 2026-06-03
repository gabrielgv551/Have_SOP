-- Migration: Expandir tipos permitidos em dfs_estrutura
-- Date: 2026-06-01

ALTER TABLE dfs_estrutura
DROP CONSTRAINT IF EXISTS dfs_estrutura_tipo_check;

ALTER TABLE dfs_estrutura
ADD CONSTRAINT dfs_estrutura_tipo_check
CHECK (tipo IN ('structure', 'mappings', 'dre_structure', 'dre_mappings', 'fcx_data', 'fcx_mappings', 'kpi_formulas'));

SELECT 'dfs_estrutura tipos expandidos' as status;
