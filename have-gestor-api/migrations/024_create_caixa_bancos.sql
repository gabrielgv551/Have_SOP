-- Caixa: bank management
CREATE TABLE IF NOT EXISTS caixa_bancos (
    id SERIAL PRIMARY KEY,
    empresa VARCHAR(50) NOT NULL,
    nome VARCHAR(100) NOT NULL,
    cor VARCHAR(20) NOT NULL DEFAULT '#007cdc',
    icone VARCHAR(10) NOT NULL DEFAULT '🏦',
    criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(empresa, nome)
);

-- Add banco_id to caixa_extrato (nullable for backwards compat)
ALTER TABLE caixa_extrato
    ADD COLUMN IF NOT EXISTS banco_id INTEGER REFERENCES caixa_bancos(id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_caixa_extrato_banco ON caixa_extrato(empresa, ano, mes, banco_id);
