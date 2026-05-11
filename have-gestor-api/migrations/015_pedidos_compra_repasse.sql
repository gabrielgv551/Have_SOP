-- Migration: Add repasse column to pedidos_compra
-- Date: 2026-04-15

ALTER TABLE pedidos_compra
  ADD COLUMN IF NOT EXISTS repasse NUMERIC(18,2);

SELECT 'pedidos_compra.repasse added' AS status;
