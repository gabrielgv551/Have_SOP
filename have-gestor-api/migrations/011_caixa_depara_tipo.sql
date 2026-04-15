-- Migration: Add `tipo` column to caixa_de_para to distinguish bank-extract vs contas_pagar mappings
-- Date: 2026-04-13
-- Run: psql -h <HOST> -U <USER> -d <DB> -f migrations/011_caixa_depara_tipo.sql

-- 1. Add the new column (default 'extrato' keeps all existing rows unchanged)
ALTER TABLE caixa_de_para
    ADD COLUMN IF NOT EXISTS tipo VARCHAR(20) NOT NULL DEFAULT 'extrato';

-- 2. Drop old unique constraint (empresa, palavra_chave) and replace with (empresa, palavra_chave, tipo)
--    This allows the same keyword to map differently for bank-extract vs contas_pagar
DO $$
DECLARE
    _con TEXT;
BEGIN
    SELECT conname INTO _con
    FROM pg_constraint
    WHERE conrelid = 'caixa_de_para'::regclass
      AND contype = 'u'
      AND array_to_string(
            ARRAY(SELECT attname FROM pg_attribute
                  WHERE attrelid = conrelid AND attnum = ANY(conkey)
                  ORDER BY attnum), ',') = 'empresa,palavra_chave';
    IF _con IS NOT NULL THEN
        EXECUTE 'ALTER TABLE caixa_de_para DROP CONSTRAINT ' || quote_ident(_con);
    END IF;
END$$;

-- 3. Create new unique constraint
ALTER TABLE caixa_de_para
    DROP CONSTRAINT IF EXISTS caixa_de_para_empresa_palavra_chave_tipo_key;

ALTER TABLE caixa_de_para
    ADD CONSTRAINT caixa_de_para_empresa_palavra_chave_tipo_key
    UNIQUE (empresa, palavra_chave, tipo);

-- 4. Index for fast lookup by tipo
CREATE INDEX IF NOT EXISTS idx_caixa_de_para_tipo ON caixa_de_para(empresa, tipo);

SELECT 'caixa_de_para tipo column migration complete' AS status;
SELECT tipo, COUNT(*) FROM caixa_de_para GROUP BY tipo;
