const { Pool } = require('pg');
const bcrypt = require('bcrypt');

const pool = new Pool({
  connectionString: 'postgres://postgres:131105Gv@37.60.236.200:5432/Lanzi'
});

async function run() {
  try {
    console.log('Criando tabela...');
    await pool.query(`
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

      CREATE INDEX IF NOT EXISTS idx_usuarios_empresa ON usuarios(empresa);
      CREATE INDEX IF NOT EXISTS idx_usuarios_usuario ON usuarios(usuario);
      CREATE INDEX IF NOT EXISTS idx_usuarios_ativo ON usuarios(ativo);
    `);
    
    console.log('Gerando hashes...');
    const h1 = await bcrypt.hash('lanzi2024', 10);
    const h2 = await bcrypt.hash('have2024', 10);
    const h3 = await bcrypt.hash('lanzi@2024', 10);
    
    console.log('Inserindo admin...');
    await pool.query(`
      INSERT INTO usuarios (empresa, nome, usuario, senha_hash, perfil, ativo) VALUES
      ('lanzi', 'Administrador', 'admin', $1, 'admin', TRUE),
      ('lanzi', 'Gestor', 'gestor', $2, 'gestor', TRUE),
      ('lanzi', 'Have', 'have', $3, 'have', TRUE)
      ON CONFLICT (empresa, usuario) DO NOTHING;
    `, [h1, h2, h3]);
    
    console.log('Migration pronta com sucesso!');
  } catch(e) {
    console.error('Erro:', e);
  } finally {
    pool.end();
  }
}

run();
