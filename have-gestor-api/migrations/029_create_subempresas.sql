-- Subunidades / CNPJs dentro da mesma empresa
CREATE TABLE IF NOT EXISTS subempresas (
    id SERIAL PRIMARY KEY,
    empresa VARCHAR(50) NOT NULL,
    nome VARCHAR(100) NOT NULL,
    cnpj VARCHAR(20),
    cor VARCHAR(20) NOT NULL DEFAULT '#007cdc',
    criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(empresa, nome)
);
CREATE INDEX IF NOT EXISTS idx_subempresas_empresa ON subempresas(empresa);

-- Link bank accounts to a subunit (nullable = belongs to all / unassigned)
ALTER TABLE caixa_bancos
    ADD COLUMN IF NOT EXISTS subempresa_id INTEGER REFERENCES subempresas(id) ON DELETE SET NULL;

-- Link sales channel groups to a subunit (nullable = shared)
ALTER TABLE vendas_grupos_canais
    ADD COLUMN IF NOT EXISTS subempresa_id INTEGER REFERENCES subempresas(id) ON DELETE SET NULL;
