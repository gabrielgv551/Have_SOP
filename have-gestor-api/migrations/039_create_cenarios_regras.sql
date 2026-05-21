-- Migration: cenarios_regras — regras em lote
-- Date: 2026-04-17

CREATE TABLE IF NOT EXISTS cenarios_regras (
    id          SERIAL PRIMARY KEY,
    cenario_id  INTEGER      NOT NULL REFERENCES cenarios(id) ON DELETE CASCADE,
    nome        VARCHAR(200) NOT NULL,
    tipo        VARCHAR(30)  NOT NULL CHECK (tipo IN ('percentual', 'valor_fixo', 'substituicao')),
    parametro   NUMERIC      NOT NULL,
    escopo_json JSONB        NOT NULL DEFAULT '{}',
    criado_em   TIMESTAMP    DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_cenarios_regras_cenario
    ON cenarios_regras(cenario_id);

-- FK from cenarios_ajustes.regra_id → cenarios_regras.id
ALTER TABLE cenarios_ajustes
    ADD CONSTRAINT fk_ajustes_regra
    FOREIGN KEY (regra_id) REFERENCES cenarios_regras(id) ON DELETE CASCADE;
