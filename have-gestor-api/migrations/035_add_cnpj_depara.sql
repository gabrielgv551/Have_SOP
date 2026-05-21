-- Migration 035: add cnpj field to caixa_de_para for Open Finance CNPJ-based matching rules

ALTER TABLE caixa_de_para ADD COLUMN IF NOT EXISTS cnpj VARCHAR(20) NULL;

-- Drop existing unique index and recreate including cnpj
DROP INDEX IF EXISTS caixa_de_para_unique_idx;

CREATE UNIQUE INDEX IF NOT EXISTS caixa_de_para_unique_idx
    ON caixa_de_para (empresa, tipo, COALESCE(razao_social,''), COALESCE(palavra_chave,''), COALESCE(cnpj,''));

SELECT 'migration 035 complete' AS status;
