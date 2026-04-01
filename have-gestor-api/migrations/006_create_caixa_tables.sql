-- Raw bank extract
CREATE TABLE IF NOT EXISTS caixa_extrato (
    id SERIAL PRIMARY KEY,
    empresa VARCHAR(50) NOT NULL,
    ano INTEGER NOT NULL,
    mes INTEGER NOT NULL CHECK (mes BETWEEN 1 AND 12),
    dia INTEGER NOT NULL CHECK (dia BETWEEN 1 AND 31),
    descricao TEXT NOT NULL DEFAULT '',
    valor INTEGER NOT NULL DEFAULT 0,
    atualizado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_caixa_extrato_periodo ON caixa_extrato(empresa, ano, mes);

-- Cash flow model categories (configurable rows)
CREATE TABLE IF NOT EXISTS caixa_categorias (
    id SERIAL PRIMARY KEY,
    empresa VARCHAR(50) NOT NULL,
    nome VARCHAR(100) NOT NULL,
    tipo VARCHAR(20) NOT NULL DEFAULT 'item',
    parent VARCHAR(100),
    ordem INTEGER NOT NULL DEFAULT 0,
    UNIQUE(empresa, nome)
);
CREATE INDEX IF NOT EXISTS idx_caixa_categorias_empresa ON caixa_categorias(empresa);

-- De-Para: maps extract description keywords to model categories
CREATE TABLE IF NOT EXISTS caixa_de_para (
    id SERIAL PRIMARY KEY,
    empresa VARCHAR(50) NOT NULL,
    palavra_chave TEXT NOT NULL,
    categoria_nome VARCHAR(100) NOT NULL,
    UNIQUE(empresa, palavra_chave)
);
CREATE INDEX IF NOT EXISTS idx_caixa_de_para_empresa ON caixa_de_para(empresa);
