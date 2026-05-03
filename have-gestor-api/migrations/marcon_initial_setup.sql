-- Setup inicial do banco Marcon
-- Cria a tabela de usuários com o schema mais recente (inclui nav_permissoes)
-- Executar: psql -h <MARCON_HOST> -U postgres -d <MARCON_DB> -f migrations/marcon_initial_setup.sql

-- Tabela de usuários
CREATE TABLE IF NOT EXISTS usuarios (
    id SERIAL PRIMARY KEY,
    empresa VARCHAR(50) NOT NULL,
    nome VARCHAR(255) NOT NULL,
    usuario VARCHAR(100) NOT NULL,
    senha_hash VARCHAR(255) NOT NULL,
    perfil VARCHAR(50) NOT NULL CHECK (perfil IN ('admin', 'gestor', 'have')),
    ativo BOOLEAN DEFAULT TRUE,
    nav_permissoes TEXT[] DEFAULT NULL,
    criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    atualizado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_empresa_usuario UNIQUE(empresa, usuario)
);

CREATE INDEX IF NOT EXISTS idx_usuarios_empresa ON usuarios(empresa);
CREATE INDEX IF NOT EXISTS idx_usuarios_usuario ON usuarios(usuario);
CREATE INDEX IF NOT EXISTS idx_usuarios_ativo ON usuarios(ativo);

-- Trigger para atualizar atualizado_em automaticamente
CREATE OR REPLACE FUNCTION update_atualizado_em()
RETURNS TRIGGER AS $$
BEGIN
    NEW.atualizado_em = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_usuarios_atualizado_em ON usuarios;
CREATE TRIGGER trigger_usuarios_atualizado_em
BEFORE UPDATE ON usuarios
FOR EACH ROW
EXECUTE FUNCTION update_atualizado_em();

SELECT 'Marcon usuarios table ready' AS status;
SELECT COUNT(*) AS total_users FROM usuarios;
