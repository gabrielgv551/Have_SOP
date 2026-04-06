const { Pool } = require('pg');

const pool = new Pool({
  host: '37.60.236.200',
  port: 5432,
  database: 'Lanzi',
  user: 'postgres',
  password: '131105Gv',
  ssl: { rejectUnauthorized: false },
});

const COMPANY = 'lanzi';

const NEW_CATS = [
  { nome: 'ATIVIDADES NÃO OPERACIONAIS', tipo: 'section', parent: null, ordem: 50 },
  { nome: 'ANO ENTRADAS', tipo: 'section', parent: 'ATIVIDADES NÃO OPERACIONAIS', ordem: 51 },
  { nome: 'CAPTAÇÃO DE EMPRÉSTIMOS', tipo: 'item', parent: 'ANO ENTRADAS', ordem: 52 },
  { nome: 'APORTE DOS SÓCIOS', tipo: 'item', parent: 'ANO ENTRADAS', ordem: 53 },
  { nome: 'ANO SAÍDAS', tipo: 'section', parent: 'ATIVIDADES NÃO OPERACIONAIS', ordem: 54 },
  { nome: 'DIVIDENDOS', tipo: 'item', parent: 'ANO SAÍDAS', ordem: 55 },
  { nome: 'EMPRÉSTIMOS', tipo: 'item', parent: 'ANO SAÍDAS', ordem: 56 },
  { nome: 'TAXAS DO BANCO', tipo: 'item', parent: 'ANO SAÍDAS', ordem: 57 },
  { nome: 'ANO SALDO FINAL', tipo: 'section', parent: 'ATIVIDADES NÃO OPERACIONAIS', ordem: 58 },
  { nome: 'CHECK', tipo: 'item', parent: 'ANO SALDO FINAL', ordem: 59 },
];

async function run() {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    for (const cat of NEW_CATS) {
      const r = await client.query(
        `INSERT INTO caixa_categorias (empresa, nome, tipo, parent, ordem)
         VALUES ($1,$2,$3,$4,$5)
         ON CONFLICT (empresa, nome) DO NOTHING
         RETURNING id, nome`,
        [COMPANY, cat.nome, cat.tipo, cat.parent, cat.ordem]
      );
      if (r.rows.length > 0) {
        console.log(`  ✅ Inserido: ${cat.nome}`);
      } else {
        console.log(`  ⚠️  Já existe: ${cat.nome}`);
      }
    }
    await client.query('COMMIT');
    console.log('\nMigração 007 concluída.');
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
