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
  { nome: 'CUSTO', tipo: 'section', parent: 'SAÍDAS', ordem: 21 },
  { nome: 'FORNECEDORES FÁBRICA', tipo: 'item', parent: 'CUSTO', ordem: 211 },
  { nome: 'ECOTAP', tipo: 'item', parent: 'CUSTO', ordem: 212 },
  { nome: 'TG POLI', tipo: 'item', parent: 'CUSTO', ordem: 213 },
  { nome: 'ECOFLEX', tipo: 'item', parent: 'CUSTO', ordem: 214 },
  { nome: 'GRID', tipo: 'item', parent: 'CUSTO', ordem: 215 },
  { nome: 'MAT.EMBALAGEM', tipo: 'item', parent: 'CUSTO', ordem: 216 },
  { nome: 'ADMINISTRATIVAS', tipo: 'section', parent: 'SAÍDAS', ordem: 22 },
  { nome: 'PESSOAL', tipo: 'item', parent: 'ADMINISTRATIVAS', ordem: 221 },
  { nome: 'INFRAESTRUTURA', tipo: 'item', parent: 'ADMINISTRATIVAS', ordem: 222 },
  { nome: 'SISTEMAS', tipo: 'item', parent: 'ADMINISTRATIVAS', ordem: 223 },
  { nome: 'PRESTAÇÃO SERVIÇOS', tipo: 'item', parent: 'ADMINISTRATIVAS', ordem: 224 },
  { nome: 'MENTORIAS', tipo: 'item', parent: 'ADMINISTRATIVAS', ordem: 225 },
  { nome: 'COMERCIAIS', tipo: 'section', parent: 'SAÍDAS', ordem: 23 },
  { nome: 'MARKETING', tipo: 'item', parent: 'COMERCIAIS', ordem: 231 },
  { nome: 'FRETES', tipo: 'item', parent: 'COMERCIAIS', ordem: 232 },
  { nome: 'TRIBUTÁRIAS', tipo: 'section', parent: 'SAÍDAS', ordem: 24 },
  { nome: 'IMPOSTOS ESTADUAIS', tipo: 'item', parent: 'TRIBUTÁRIAS', ordem: 241 },
  { nome: 'IMPOSTOS FEDERAIS', tipo: 'item', parent: 'TRIBUTÁRIAS', ordem: 242 },
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
         RETURNING nome`,
        [COMPANY, cat.nome, cat.tipo, cat.parent, cat.ordem]
      );
      console.log(r.rows.length > 0 ? `  ✅ Inserido: ${cat.nome}` : `  ⚠️  Já existe: ${cat.nome}`);
    }
    await client.query('COMMIT');
    console.log('\nMigração 008 concluída.');
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
