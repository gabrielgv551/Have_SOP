-- Add razao_social column to caixa_extrato for Itaú-format imports
ALTER TABLE caixa_extrato
    ADD COLUMN IF NOT EXISTS razao_social TEXT NULL;

CREATE INDEX IF NOT EXISTS idx_caixa_extrato_razao_social
    ON caixa_extrato(empresa, razao_social) WHERE razao_social IS NOT NULL;

SELECT 'razao_social added to caixa_extrato' AS status;
