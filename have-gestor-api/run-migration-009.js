const { Pool } = require('pg');
const pool = new Pool({ host:'37.60.236.200', port:5432, database:'Lanzi', user:'postgres', password:'131105Gv', ssl:{rejectUnauthorized:false} });
const COMPANY = 'lanzi';
async function run() {
  const client = await pool.connect();
  try {
    const r = await client.query(
      `DELETE FROM caixa_categorias WHERE empresa=$1 AND nome IN ('ANO SALDO FINAL','CHECK') RETURNING nome`,
      [COMPANY]
    );
    r.rows.forEach(row => console.log(`  ✅ Removido: ${row.nome}`));
    console.log('\nPronto.');
  } finally { client.release(); pool.end(); }
}
run();
