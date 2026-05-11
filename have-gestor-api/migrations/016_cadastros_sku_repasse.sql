-- Migration: Add repasse column to cadastros_sku
-- Date: 2026-04-15

ALTER TABLE cadastros_sku
  ADD COLUMN IF NOT EXISTS repasse NUMERIC(18,2);

SELECT 'cadastros_sku.repasse added' AS status;
