-- Migration: Cria a tabela de clientes Bling no banco central 'bling'
-- Executar no banco de dados: bling

CREATE TABLE IF NOT EXISTS clientes (
    id SERIAL PRIMARY KEY,
    nome VARCHAR(100) NOT NULL,
    empresa VARCHAR(50) NOT NULL,
    client_id VARCHAR(100),
    client_secret VARCHAR(100),
    access_token TEXT,
    refresh_token TEXT,
    expires_at TIMESTAMP,
    last_sync TIMESTAMP,
    atualizado_em TIMESTAMP DEFAULT NOW(),
    UNIQUE (empresa, nome)
);

COMMENT ON TABLE clientes IS 'Armazena credenciais OAuth das contas Bling v3 integradas por empresa';
