-- Migration: adiciona horizonte_meses à tabela cenarios
-- Date: 2026-04-21

ALTER TABLE cenarios
    ADD COLUMN IF NOT EXISTS horizonte_meses INTEGER NOT NULL DEFAULT 12;

COMMENT ON COLUMN cenarios.horizonte_meses IS
    'Número de meses projetados pelo motor de eventos (padrão 12)';
