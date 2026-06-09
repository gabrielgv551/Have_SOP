const { Client } = require('pg');

async function run() {
  const connectionStringBase = 'postgres://postgres:131105Gv@37.60.236.200:5432/postgres';
  const client = new Client({ connectionString: connectionStringBase });

  try {
    await client.connect();
    console.log('🔌 Conectado ao cluster PostgreSQL...');

    // 1. Verifica se o banco 'bling' existe
    const dbCheck = await client.query("SELECT 1 FROM pg_database WHERE datname = 'bling'");
    if (dbCheck.rows.length === 0) {
      console.log('⚙️ Criando o banco de dados [bling]...');
      await client.query('CREATE DATABASE bling');
      console.log('✅ Banco de dados [bling] criado com sucesso!');
    } else {
      console.log('ℹ️ O banco de dados [bling] já existe.');
    }
  } catch (err) {
    console.error('❌ Erro durante a inicialização do banco:', err.message);
    process.exit(1);
  } finally {
    await client.end();
  }

  // 2. Conecta ao novo banco 'bling' para criar a tabela 'clientes'
  const connectionStringBling = 'postgres://postgres:131105Gv@37.60.236.200:5432/bling';
  const blingClient = new Client({ connectionString: connectionStringBling });

  try {
    await blingClient.connect();
    console.log('🔌 Conectado ao banco de dados [bling]...');

    console.log('⚙️ Criando tabela [clientes]...');
    await blingClient.query(`
      CREATE TABLE IF NOT EXISTS clientes (
        id SERIAL PRIMARY KEY,
        nome VARCHAR(100) NOT NULL,
        empresa VARCHAR(50) NOT NULL,
        client_id VARCHAR(100),
        client_secret VARCHAR(100),
        access_token TEXT,
        refresh_token TEXT,
        expires_at TIMESTAMP,
        last_sync TIMESTAMP,
        atualizado_em TIMESTAMP DEFAULT NOW(),
        UNIQUE (empresa, nome)
      )
    `);
    console.log('✅ Tabela [clientes] criada/verificada com sucesso!');

    // Inserir os registros de exemplo ou placeholder se a tabela estiver vazia
    const countRes = await blingClient.query('SELECT count(*) FROM clientes');
    const count = parseInt(countRes.rows[0].count);
    if (count === 0) {
      console.log('⚙️ Inserindo placeholders de exemplo para Autoequip...');
      await blingClient.query(`
        INSERT INTO clientes (nome, empresa, refresh_token, expires_at)
        VALUES 
          ('cliente_1', 'autoequip', 'COLOQUE_REFRESH_TOKEN_AQUI_1', '2026-01-01T00:00:00.000Z'),
          ('cliente_2', 'autoequip', 'COLOQUE_REFRESH_TOKEN_AQUI_2', '2026-01-01T00:00:00.000Z')
      `);
      console.log('✅ Placeholders inseridos com sucesso!');
    }

  } catch (err) {
    console.error('❌ Erro ao configurar tabela no banco [bling]:', err.message);
  } finally {
    await blingClient.end();
  }
}

run();
