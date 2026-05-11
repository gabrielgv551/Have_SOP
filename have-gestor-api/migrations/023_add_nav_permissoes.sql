-- Migration: Add nav_permissoes column to usuarios table
-- Controls which sidebar sections each user can access
-- Sections: caixa, dfs, sopc, margens, ia
-- NULL = no access (admin role always bypasses this check)
-- Date: 2026-04-22
-- Run: psql -h <HOST> -U postgres -d <DB> -f migrations/023_add_nav_permissoes.sql

ALTER TABLE usuarios
  ADD COLUMN IF NOT EXISTS nav_permissoes TEXT[] DEFAULT NULL;

-- Verify
SELECT 'nav_permissoes column added' AS status;
