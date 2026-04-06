const { Pool } = require('pg');
const pool = new Pool({ host:'37.60.236.200', port:5432, database:'Lanzi', user:'postgres', password:'131105Gv', ssl:{rejectUnauthorized:false} });
async function run() {
  const client = await pool.connect();
  try {
    // Check bd_vendas structure
    const cols = await client.query(`SELECT column_name, data_type FROM information_schema.columns WHERE table_name='bd_vendas' ORDER BY ordinal_position`);
    console.log('Colunas de bd_vendas:');
    cols.rows.forEach(r => console.log(`  ${r.column_name} (${r.data_type})`));
    const sample = await client.query('SELECT * FROM bd_vendas LIMIT 2');
    console.log('\nAmostra:');
    sample.rows.forEach(r => console.log(JSON.stringify(r)));
    // Date range
    const dates = await client.query(`SELECT MIN(data) as min_data, MAX(data) as max_data FROM bd_vendas`);
    console.log('\nIntervalo de datas:', JSON.stringify(dates.rows[0]));
    // SKUs sem venda nos últimos 6 meses
    const semVenda = await client.query(`
      SELECT COUNT(DISTINCT sku) as total
      FROM cadastros_sku c
      WHERE NOT EXISTS (
        SELECT 1 FROM bd_vendas v
        WHERE v.sku = c."Sku"
        AND v.data >= CURRENT_DATE - INTERVAL '6 months'
      )
    `);
    console.log('\nSKUs sem venda nos últimos 6 meses:', semVenda.rows[0].total);
  } finally { client.release(); pool.end(); }
}
run();
