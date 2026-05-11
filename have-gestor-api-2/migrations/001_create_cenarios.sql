-- Migration: cenarios — tabela principal de cenários what-if
-- Date: 2026-04-17

CREATE TABLE IF NOT EXISTS cenarios (
    id              SERIAL PRIMARY KEY,
    empresa         VARCHAR(50)  NOT NULL,
    nome            VARCHAR(200) NOT NULL,
    descricao       TEXT,
    ano             INTEGER      NOT NULL,
    mes_inicio      INTEGER      NOT NULL CHECK (mes_inicio BETWEEN 1 AND 12),
    mes_fim         INTEGER      NOT NULL CHECK (mes_fim BETWEEN 1 AND 12),
    cenario_pai_id  INTEGER      REFERENCES cenarios(id) ON DELETE SET NULL,
    criado_por      VARCHAR(100),
    criado_em       TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    atualizado_em   TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    arquivado       BOOLEAN      DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_cenarios_empresa ON cenarios(empresa);
CREATE INDEX IF NOT EXISTS idx_cenarios_pai ON cenarios(cenario_pai_id);
CREATE INDEX IF NOT EXISTS idx_cenarios_empresa_ano ON cenarios(empresa, ano, arquivado);
