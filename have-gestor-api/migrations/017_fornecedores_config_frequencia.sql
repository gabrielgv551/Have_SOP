-- Migration: Add reorder frequency columns to fornecedores_config
-- Date: 2026-04-16

ALTER TABLE fornecedores_config
  ADD COLUMN IF NOT EXISTS frequencia_tipo VARCHAR(20) DEFAULT 'mensal',
  ADD COLUMN IF NOT EXISTS dia_semana_preferido INTEGER DEFAULT 5,
  ADD COLUMN IF NOT EXISTS intervalo_dias INTEGER DEFAULT 30;

SELECT 'fornecedores_config frequency columns added' AS status;
