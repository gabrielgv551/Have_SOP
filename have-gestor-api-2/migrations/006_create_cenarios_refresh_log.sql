-- Migration: cenarios_refresh_log — log de refresh do snapshot
-- Date: 2026-04-17

CREATE TABLE IF NOT EXISTS cenarios_refresh_log (
    id                  SERIAL PRIMARY KEY,
    cenario_id          INTEGER   NOT NULL REFERENCES cenarios(id) ON DELETE CASCADE,
    refreshed_em        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    ajustes_preservados INTEGER   NOT NULL DEFAULT 0,
    ajustes_orfaos_json JSONB     NOT NULL DEFAULT '[]'
);

CREATE INDEX IF NOT EXISTS idx_cenarios_refresh_cenario
    ON cenarios_refresh_log(cenario_id, refreshed_em DESC);
