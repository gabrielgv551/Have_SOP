-- Migration 033: Adiciona colunas ausentes em bd_vendas_ml
-- Corrige schema incompleto da 031 (seller_id, seller_nickname, taxes_amount)

ALTER TABLE bd_vendas_ml
    ADD COLUMN IF NOT EXISTS seller_id       BIGINT,
    ADD COLUMN IF NOT EXISTS seller_nickname TEXT,
    ADD COLUMN IF NOT EXISTS taxes_amount    NUMERIC;

SELECT 'bd_vendas_ml: colunas seller_id, seller_nickname, taxes_amount adicionadas' AS status;
