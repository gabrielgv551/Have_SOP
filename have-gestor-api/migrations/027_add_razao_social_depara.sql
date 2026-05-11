-- Add razao_social matching field to caixa_de_para (optional; takes priority over palavra_chave when set)
ALTER TABLE caixa_de_para
    ADD COLUMN IF NOT EXISTS razao_social TEXT NULL;

SELECT 'razao_social added to caixa_de_para' AS status;
