-- Migration: Adicionar tipo manual_overrides na dfs_estrutura
-- Date: 2026-06-01

ALTER TABLE dfs_estrutura
DROP CONSTRAINT IF EXISTS dfs_estrutura_tipo_check;

ALTER TABLE dfs_estrutura
ADD CONSTRAINT dfs_estrutura_tipo_check
CHECK (tipo IN ('structure', 'mappings', 'dre_structure', 'dre_mappings', 'fcx_data', 'fcx_mappings', 'kpi_formulas', 'manual_overrides'));

SELECT 'tipo manual_overrides adicionado' as status;
