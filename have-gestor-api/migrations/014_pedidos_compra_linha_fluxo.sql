-- Migration: Add linha_fluxo column to pedidos_compra
-- Date: 2026-04-13

ALTER TABLE pedidos_compra
  ADD COLUMN IF NOT EXISTS linha_fluxo VARCHAR(200);

SELECT 'pedidos_compra.linha_fluxo added' AS status;
