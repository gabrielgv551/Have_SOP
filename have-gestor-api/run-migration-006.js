#!/usr/bin/env node
const { Pool } = require('pg');
const fs = require('fs');
const path = require('path');

const pool = new Pool({
  host: '37.60.236.200',
  port: 5432,
  database: 'Lanzi',
  user: 'postgres',
  password: '131105Gv',
  ssl: { rejectUnauthorized: false }
});

async function run() {
  try {
    console.log('🔌 Conectando ao PostgreSQL...');
    await pool.query('SELECT NOW()');
    console.log('✅ Conexão estabelecida\n');

    const migrationPath = path.join(__dirname, 'migrations', '006_create_caixa_tables.sql');
    const sql = fs.readFileSync(migrationPath, 'utf-8');

    console.log('📋 Executando migration: 006_create_caixa_tables.sql\n');
    await pool.query(sql);
    console.log('✅ Migration executada com sucesso!\n');

    // Verify tables
    const tables = ['caixa_extrato', 'caixa_categorias', 'caixa_de_para'];
    for (const table of tables) {
      const r = await pool.query(
        `SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_name=$1`,
        [table]
      );
      if (r.rows.length > 0) {
        console.log(`✅ Tabela "${table}" OK`);
      } else {
        console.log(`❌ Tabela "${table}" NÃO encontrada`);
      }
    }

    await pool.end();
    process.exit(0);
  } catch (e) {
    console.error('❌ Erro:', e.message);
    await pool.end();
    process.exit(1);
  }
}

run();
