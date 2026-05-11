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

    const sql = fs.readFileSync(path.join(__dirname, 'migrations/015_pedidos_compra_repasse.sql'), 'utf8');
    const result = await client.query(sql);
    console.log('✅', result[result.length - 1]?.rows?.[0]?.status || 'Migration executada');

    await client.query('COMMIT');
    console.log('\n✅ Migração 015 concluída.');
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
