-- Migration: Add fornecedor column to contas_pagar
-- Date: 2026-06-01
-- Issue: column "fornecedor" does not exist in fluxo de caixa queries

ALTER TABLE contas_pagar
    ADD COLUMN IF NOT EXISTS fornecedor TEXT;

SELECT 'fornecedor column added to contas_pagar (if missing)' AS status;
