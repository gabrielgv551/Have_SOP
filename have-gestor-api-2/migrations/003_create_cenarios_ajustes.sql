-- Migration: cenarios_ajustes — ajustes do usuário sobre o snapshot
-- Date: 2026-04-17

CREATE TABLE IF NOT EXISTS cenarios_ajustes (
    id                      SERIAL PRIMARY KEY,
    cenario_id              INTEGER      NOT NULL REFERENCES cenarios(id) ON DELETE CASCADE,
    tipo                    VARCHAR(30)  NOT NULL DEFAULT 'override' CHECK (tipo IN ('override', 'lancamento_novo')),
    mes                     INTEGER      NOT NULL CHECK (mes BETWEEN 1 AND 12),
    categoria               VARCHAR(100) NOT NULL,
    dia                     INTEGER      NOT NULL CHECK (dia BETWEEN 1 AND 31),
    valor_original_centavos INTEGER,
    valor_novo_centavos     INTEGER      NOT NULL,
    descricao               TEXT,
    regra_id                INTEGER,
    criado_em               TIMESTAMP    DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_cenarios_ajustes_cenario
    ON cenarios_ajustes(cenario_id, mes, categoria, dia);

CREATE INDEX IF NOT EXISTS idx_cenarios_ajustes_regra
    ON cenarios_ajustes(regra_id);
