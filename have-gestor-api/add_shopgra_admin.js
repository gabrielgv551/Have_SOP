/**
 * Adiciona usuário admin ao banco de Shopgra
 * Execute: node add_shopgra_admin.js
 */

const { Pool } = require('pg');
const bcrypt = require('bcrypt');

const pool = new Pool({
  host: '37.60.236.200',
  port: 5432,
  database: 'shopgra',
  user: process.env.SHOPGRA_USER || 'postgres',
  password: process.env.SHOPGRA_PASSWORD || '131105Gv',
});

async function addAdmin() {
  let client;
  try {
    client = await pool.connect();
    console.log('✓ Conectado ao banco: shopgra em 37.60.236.200');

    // 1. Criar extensão pgcrypto
    await client.query('CREATE EXTENSION IF NOT EXISTS pgcrypto');
    console.log('✓ Extensão pgcrypto verificada');

    // 2. Criar tabela usuarios
    await client.query(`
      CREATE TABLE IF NOT EXISTS usuarios (
        id SERIAL PRIMARY KEY,
        empresa VARCHAR(50) NOT NULL,
        usuario VARCHAR(100) NOT NULL,
        nome VARCHAR(255),
        email VARCHAR(255),
        senha_hash VARCHAR(255) NOT NULL,
        perfil VARCHAR(50) DEFAULT 'admin',
        ativo BOOLEAN DEFAULT TRUE,
        nav_permissoes JSONB,
        criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        atualizado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );
    `);
    console.log('✓ Tabela usuarios verificada');

    // 3. Criar índice
    await client.query(`
      CREATE UNIQUE INDEX IF NOT EXISTS idx_usuario_empresa 
      ON usuarios(usuario, empresa);
    `);
    console.log('✓ Índice verificado');

    // 4. Deletar admin anterior
    await client.query(
      'DELETE FROM usuarios WHERE usuario = $1 AND empresa = $2',
      ['admin', 'shopgra']
    );
    console.log('✓ Admin anterior removido (se existia)');

    // 5. Gerar hash com bcrypt
    const senha = 'lanzi2024';
    const hash = await bcrypt.hash(senha, 12);
    console.log(`✓ Hash gerado para a senha`);

    // 6. Inserir novo admin
    const result = await client.query(
      `INSERT INTO usuarios (empresa, usuario, nome, email, senha_hash, perfil, ativo)
       VALUES ($1, $2, $3, $4, $5, $6, $7)
       RETURNING id, usuario, nome, perfil, ativo, criado_em`,
      ['shopgra', 'admin', 'Administrador Shopgra', 'admin@shopgra.com.br', hash, 'admin', true]
    );

    const user = result.rows[0];
    console.log('\n✓ USUÁRIO ADICIONADO COM SUCESSO!\n');
    console.log('Detalhes:');
    console.log(`  ID: ${user.id}`);
    console.log(`  Empresa: shopgra`);
    console.log(`  Usuário: ${user.usuario}`);
    console.log(`  Nome: ${user.nome}`);
    console.log(`  Perfil: ${user.perfil}`);
    console.log(`  Ativo: ${user.ativo}`);
    console.log(`  Criado em: ${user.criado_em}\n`);

    console.log('Credenciais para login:');
    console.log(`  Usuário: admin`);
    console.log(`  Senha: lanzi2024`);
    console.log(`  Empresa: shopgra\n`);

    // 7. Verificação final
    const verify = await client.query(
      'SELECT COUNT(*) FROM usuarios WHERE usuario = $1 AND empresa = $2',
      ['admin', 'shopgra']
    );
    if (verify.rows[0].count > 0) {
      console.log('✓ Verificação concluída - Usuário confirmado no banco!');
    }

  } catch (error) {
    console.error('❌ Erro:', error.message);
    process.exit(1);
  } finally {
    if (client) client.release();
    await pool.end();
  }
}

addAdmin().then(() => process.exit(0));
