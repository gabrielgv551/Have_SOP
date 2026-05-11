-- Migration 028: support RS-only De-Para rules (palavra_chave optional when razao_social is set)
-- and allow splitting a supplier across multiple categories via RS+keyword combined rules.

-- 1. Allow palavra_chave to be NULL (RS-only rules have no keyword)
ALTER TABLE caixa_de_para ALTER COLUMN palavra_chave DROP NOT NULL;

-- 2. Drop old unique constraint (empresa, palavra_chave, tipo)
ALTER TABLE caixa_de_para
    DROP CONSTRAINT IF EXISTS caixa_de_para_empresa_palavra_chave_tipo_key;

-- 3. New functional unique index that covers all combinations:
--    • RS-only  :  (empresa, tipo, razao_social, '')
--    • RS+kw    :  (empresa, tipo, razao_social, palavra_chave)
--    • kw-only  :  (empresa, tipo, '',           palavra_chave)
CREATE UNIQUE INDEX IF NOT EXISTS caixa_de_para_unique_idx
    ON caixa_de_para (empresa, tipo, COALESCE(razao_social, ''), COALESCE(palavra_chave, ''));

SELECT 'migration 028 complete' AS status;
