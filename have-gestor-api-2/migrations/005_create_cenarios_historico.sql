-- Migration: cenarios_historico — undo/redo events
-- Date: 2026-04-17

CREATE TABLE IF NOT EXISTS cenarios_historico (
    id                  SERIAL PRIMARY KEY,
    cenario_id          INTEGER      NOT NULL REFERENCES cenarios(id) ON DELETE CASCADE,
    operacao            VARCHAR(50)  NOT NULL,
    payload_json        JSONB        NOT NULL DEFAULT '{}',
    payload_reverso_json JSONB       NOT NULL DEFAULT '{}',
    criado_em           TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    desfeito_em         TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_cenarios_historico_cenario
    ON cenarios_historico(cenario_id, criado_em DESC);
