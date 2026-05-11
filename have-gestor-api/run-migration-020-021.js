require('dotenv').config({ path: '.env.local' });
const { Pool } = require('pg');
const fs = require('fs');
const path = require('path');

const pool = new Pool({
  host: process.env.LANZI_HOST || '37.60.236.200',
  port: parseInt(process.env.LANZI_PORT || '5432'),
  database: process.env.LANZI_DB || 'Lanzi',
  user: process.env.LANZI_USER || 'postgres',
  password: process.env.LANZI_PASSWORD || '131105Gv',
  ssl: false,
});

async function run() {
  const client = await pool.connect();
  try {
    for (const file of ['020_pedidos_compra_grupo.sql', '021_create_pedidos_compra_itens.sql']) {
      const sql = fs.readFileSync(path.join(__dirname, 'migrations', file), 'utf8');
      console.log('Running', file, '...');
      const r = await client.query(sql);
      console.log('  ->', r[r.length - 1]?.rows?.[0] || 'done');
    }
    console.log('All migrations done.');
  } finally {
    client.release();
    await pool.end();
  }
}
run().catch(e => { console.error(e.message); process.exit(1); });
