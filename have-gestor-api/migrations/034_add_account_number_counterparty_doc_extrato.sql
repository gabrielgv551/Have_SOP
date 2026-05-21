-- Add account_number and counterparty_document to caixa_extrato
ALTER TABLE caixa_extrato
    ADD COLUMN IF NOT EXISTS account_number VARCHAR(100) NULL,
    ADD COLUMN IF NOT EXISTS counterparty_document VARCHAR(255) NULL;

SELECT 'account_number and counterparty_document added to caixa_extrato' AS status;
