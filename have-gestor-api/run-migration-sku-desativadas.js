const { Pool } = require('pg');
const pool = new Pool({ host:'37.60.236.200', port:5432, database:'Lanzi', user:'postgres', password:'131105Gv', ssl:{rejectUnauthorized:false} });
async function run() {
  const client = await pool.connect();
  try {
    await client.query(`
      CREATE TABLE IF NOT EXISTS sku_desativadas (
        id        SERIAL PRIMARY KEY,
        empresa   VARCHAR(50) NOT NULL,
        sku       VARCHAR(200) NOT NULL,
        criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(empresa, sku)
      )
    `);
    console.log('✅ Tabela sku_desativadas criada (ou já existia).');
    await client.query(`CREATE INDEX IF NOT EXISTS idx_sku_desativadas_empresa ON sku_desativadas(empresa)`);
    console.log('✅ Índice criado.');
    console.log('\nPronto.');
  } catch(e) {
    console.error('❌ Erro:', e.message);
  } finally { client.release(); pool.end(); }
}
run();
