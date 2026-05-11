-- Migration: cenarios_snapshot_base — snapshot imutável dos dados consolidados
-- Date: 2026-04-17

CREATE TABLE IF NOT EXISTS cenarios_snapshot_base (
    id              SERIAL PRIMARY KEY,
    cenario_id      INTEGER      NOT NULL REFERENCES cenarios(id) ON DELETE CASCADE,
    mes             INTEGER      NOT NULL CHECK (mes BETWEEN 1 AND 12),
    categoria       VARCHAR(100) NOT NULL,
    dia             INTEGER      NOT NULL CHECK (dia BETWEEN 1 AND 31),
    valor_centavos  INTEGER      NOT NULL DEFAULT 0,
    origem          VARCHAR(20)  NOT NULL DEFAULT 'realizado' CHECK (origem IN ('realizado', 'previsao')),
    capturado_em    TIMESTAMP    DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_cenarios_snap_unique
    ON cenarios_snapshot_base(cenario_id, mes, categoria, dia, origem);

CREATE INDEX IF NOT EXISTS idx_cenarios_snap_cenario
    ON cenarios_snapshot_base(cenario_id);
