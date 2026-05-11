-- Migration 020: Add grupo_id to pedidos_compra for grouping installments
ALTER TABLE pedidos_compra ADD COLUMN IF NOT EXISTS grupo_id TEXT;
CREATE INDEX IF NOT EXISTS idx_pc_grupo ON pedidos_compra(empresa, grupo_id);
SELECT 'pedidos_compra.grupo_id added' AS status;
