-- Add extract format to bank records
ALTER TABLE caixa_bancos
    ADD COLUMN IF NOT EXISTS formato VARCHAR(50) NOT NULL DEFAULT 'generico';
