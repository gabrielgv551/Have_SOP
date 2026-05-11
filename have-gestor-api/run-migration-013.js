const { Pool } = require('pg');
const fs = require('fs');
const path = require('path');

const pool = new Pool({
  host: '37.60.236.200',
  port: 5432,
  database: 'Lanzi',
  user: 'postgres',
  password: '131105Gv',
  ssl: { rejectUnauthorized: false },
});

async function run() {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    // 013 — Create pedidos_compra table
    const sql013 = fs.readFileSync(path.join(__dirname, 'migrations/013_create_pedidos_compra.sql'), 'utf8');
    const r013 = await client.query(sql013);
    console.log('✅', r013[r013.length - 1]?.rows?.[0]?.status || 'Migration 013 executada');

    // 014 — Add linha_fluxo (idempotent, just in case)
    const sql014 = fs.readFileSync(path.join(__dirname, 'migrations/014_pedidos_compra_linha_fluxo.sql'), 'utf8');
    const r014 = await client.query(sql014);
    console.log('✅', r014[r014.length - 1]?.rows?.[0]?.status || 'Migration 014 executada');

    // 015 — Add repasse (idempotent, just in case)
    const sql015 = fs.readFileSync(path.join(__dirname, 'migrations/015_pedidos_compra_repasse.sql'), 'utf8');
    const r015 = await client.query(sql015);
    console.log('✅', r015[r015.length - 1]?.rows?.[0]?.status || 'Migration 015 executada');

    await client.query('COMMIT');
    console.log('\n✅ Migrações 013-015 concluídas com sucesso.');
  } catch (e) {
    await client.query('ROLLBACK');
    console.error('Erro:', e.message);
    process.exit(1);
  } finally {
    client.release();
    pool.end();
  }
}

run();
