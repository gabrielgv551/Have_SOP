-- Migration: Create usuarios table for user management with bcrypt hashing
-- Date: 2026-03-24
-- Run: psql -h 37.60.236.200 -U postgres -d Lanzi -f migrations/001_create_usuarios.sql

-- Create usuarios table
CREATE TABLE IF NOT EXISTS usuarios (
    id SERIAL PRIMARY KEY,
    empresa VARCHAR(50) NOT NULL,
    nome VARCHAR(255) NOT NULL,
    usuario VARCHAR(100) NOT NULL,
    senha_hash VARCHAR(255) NOT NULL,
    perfil VARCHAR(50) NOT NULL CHECK (perfil IN ('admin', 'gestor', 'have')),
    ativo BOOLEAN DEFAULT TRUE,
    criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    atualizado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_empresa_usuario UNIQUE(empresa, usuario)
);

-- Create indices for performance
CREATE INDEX IF NOT EXISTS idx_usuarios_empresa ON usuarios(empresa);
CREATE INDEX IF NOT EXISTS idx_usuarios_usuario ON usuarios(usuario);
CREATE INDEX IF NOT EXISTS idx_usuarios_ativo ON usuarios(ativo);

-- Insert initial users (migrating from env vars)
-- Note: These are placeholder hashes - replace with actual bcrypt hashes
-- To generate bcrypt hashes locally, use Node.js:
--   const bcrypt = require('bcrypt');
--   bcrypt.hash('lanzi2024', 10).then(h => console.log(h));

INSERT INTO usuarios (empresa, nome, usuario, senha_hash, perfil, ativo) VALUES
  ('lanzi', 'Administrador', 'admin', '$2b$10$PLACEHOLDER_HASH_ADMIN', 'admin', TRUE),
  ('lanzi', 'Gestor', 'gestor', '$2b$10$PLACEHOLDER_HASH_GESTOR', 'gestor', TRUE),
  ('lanzi', 'Have', 'have', '$2b$10$PLACEHOLDER_HASH_HAVE', 'have', TRUE)
ON CONFLICT (empresa, usuario) DO NOTHING;

-- Add trigger for atualizado_em timestamp (auto-update on record change)
CREATE OR REPLACE FUNCTION update_atualizado_em()
RETURNS TRIGGER AS $$
BEGIN
    NEW.atualizado_em = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_usuarios_atualizado_em
BEFORE UPDATE ON usuarios
FOR EACH ROW
EXECUTE FUNCTION update_atualizado_em();

-- Verify table creation
SELECT 'Usuarios table created successfully' as status;
SELECT COUNT(*) as total_users FROM usuarios;
