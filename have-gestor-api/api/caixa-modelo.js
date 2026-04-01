const jwt = require('jsonwebtoken');
const { Pool } = require('pg');
const companies = require('../lib/companies');

const pools = {};
function getPool(company) {
  if (pools[company]) return pools[company];
  const key = (companies[company] && companies[company].dbEnvKey) || company.toUpperCase();
  pools[company] = new Pool({
    host: process.env[`${key}_HOST`], port: parseInt(process.env[`${key}_PORT`] || '5432'),
    database: process.env[`${key}_DB`], user: process.env[`${key}_USER`],
    password: process.env[`${key}_PASSWORD`], ssl: { rejectUnauthorized: false }, max: 5,
  });
  return pools[company];
}

function verifyToken(req, res) {
  const auth = (req.headers.authorization || '').split(' ')[1];
  if (!auth) { res.status(401).json({ error: 'Token nao fornecido' }); return null; }
  try { return jwt.verify(auth, process.env.JWT_SECRET); }
  catch { res.status(401).json({ error: 'Token invalido' }); return null; }
}

const DEFAULT_CATEGORIAS = [
  { nome: 'SALDO INICIAL DO CAIXA', tipo: 'saldo_ini', parent: null, ordem: 0 },
  { nome: 'ENTRADAS', tipo: 'section', parent: null, ordem: 10 },
  { nome: 'Farmer', tipo: 'item', parent: 'ENTRADAS', ordem: 11 },
  { nome: 'Closer', tipo: 'item', parent: 'ENTRADAS', ordem: 12 },
  { nome: 'Loja Física', tipo: 'item', parent: 'ENTRADAS', ordem: 13 },
  { nome: 'Ecommerce', tipo: 'item', parent: 'ENTRADAS', ordem: 14 },
  { nome: 'Sucata', tipo: 'item', parent: 'ENTRADAS', ordem: 15 },
  { nome: 'Inativator', tipo: 'item', parent: 'ENTRADAS', ordem: 16 },
  { nome: 'Provisão', tipo: 'item', parent: 'ENTRADAS', ordem: 17 },
  { nome: 'Fiscal', tipo: 'item', parent: 'ENTRADAS', ordem: 18 },
  { nome: 'SAÍDAS', tipo: 'section', parent: null, ordem: 20 },
  { nome: 'Perfibras', tipo: 'item', parent: 'SAÍDAS', ordem: 21 },
  { nome: 'Alump', tipo: 'item', parent: 'SAÍDAS', ordem: 22 },
  { nome: 'Infiniti', tipo: 'item', parent: 'SAÍDAS', ordem: 23 },
  { nome: 'Anosul', tipo: 'item', parent: 'SAÍDAS', ordem: 24 },
  { nome: 'Max', tipo: 'item', parent: 'SAÍDAS', ordem: 25 },
  { nome: 'Pasimetal', tipo: 'item', parent: 'SAÍDAS', ordem: 26 },
  { nome: 'Sendeski', tipo: 'item', parent: 'SAÍDAS', ordem: 27 },
  { nome: 'Sydorak', tipo: 'item', parent: 'SAÍDAS', ordem: 28 },
  { nome: 'Aperam', tipo: 'item', parent: 'SAÍDAS', ordem: 29 },
  { nome: 'Tecmaf', tipo: 'item', parent: 'SAÍDAS', ordem: 30 },
  { nome: 'Outros', tipo: 'item', parent: 'SAÍDAS', ordem: 31 },
  { nome: 'Importação', tipo: 'item', parent: 'SAÍDAS', ordem: 32 },
  { nome: 'Embalagem', tipo: 'item', parent: 'SAÍDAS', ordem: 33 },
  { nome: 'Despesas de Fábrica', tipo: 'item', parent: 'SAÍDAS', ordem: 34 },
  { nome: 'Qualidade', tipo: 'item', parent: 'SAÍDAS', ordem: 35 },
  { nome: 'Fretes Compras', tipo: 'item', parent: 'SAÍDAS', ordem: 36 },
  { nome: 'Fretes Vendas', tipo: 'item', parent: 'SAÍDAS', ordem: 37 },
  { nome: 'Comissões Ecommerce', tipo: 'item', parent: 'SAÍDAS', ordem: 38 },
  { nome: 'Marketing', tipo: 'item', parent: 'SAÍDAS', ordem: 39 },
  { nome: 'Pessoal', tipo: 'item', parent: 'SAÍDAS', ordem: 40 },
  { nome: 'SALDO FINAL', tipo: 'saldo_fin', parent: null, ordem: 99 },
];

async function seedDefaults(pool, company) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    for (const cat of DEFAULT_CATEGORIAS) {
      await client.query(
        `INSERT INTO caixa_categorias (empresa, nome, tipo, parent, ordem)
         VALUES ($1,$2,$3,$4,$5) ON CONFLICT (empresa, nome) DO NOTHING`,
        [company, cat.nome, cat.tipo, cat.parent, cat.ordem]
      );
    }
    await client.query('COMMIT');
  } catch (e) { await client.query('ROLLBACK'); throw e; }
  finally { client.release(); }
}

module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, DELETE, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  if (req.method === 'OPTIONS') return res.status(200).end();

  const payload = verifyToken(req, res);
  if (!payload) return;
  const company = payload.company || 'lanzi';
  const pool = getPool(company);

  try {
    if (req.method === 'GET') {
      const { ano, mes } = req.query;

      // Ensure defaults exist
      const countR = await pool.query('SELECT COUNT(*) FROM caixa_categorias WHERE empresa=$1', [company]);
      if (parseInt(countR.rows[0].count) === 0) await seedDefaults(pool, company);

      const catsR = await pool.query(
        'SELECT id, nome, tipo, parent, ordem FROM caixa_categorias WHERE empresa=$1 ORDER BY ordem',
        [company]
      );
      const categorias = catsR.rows;

      if (!ano || !mes) return res.json({ categorias });

      // Get de-para mappings
      const dpR = await pool.query(
        'SELECT palavra_chave, categoria_nome FROM caixa_de_para WHERE empresa=$1',
        [company]
      );
      const depara = dpR.rows; // [{palavra_chave, categoria_nome}]

      // Get extract rows for the period
      const extR = await pool.query(
        'SELECT dia, descricao, valor FROM caixa_extrato WHERE empresa=$1 AND ano=$2 AND mes=$3',
        [company, parseInt(ano), parseInt(mes)]
      );
      const extractRows = extR.rows;

      // Compute: for each (dia, categoria_nome) → sum of matched extract rows
      const valores = {}; // { categoria_nome: { dia: total_centavos } }
      for (const extRow of extractRows) {
        const descLower = (extRow.descricao || '').toLowerCase();
        for (const dp of depara) {
          if (descLower.includes(dp.palavra_chave.toLowerCase())) {
            const catNome = dp.categoria_nome;
            if (!valores[catNome]) valores[catNome] = {};
            const dia = parseInt(extRow.dia);
            valores[catNome][dia] = (valores[catNome][dia] || 0) + parseInt(extRow.valor);
            break; // first matching keyword wins
          }
        }
      }

      return res.json({ categorias, valores });
    }

    if (req.method === 'POST') {
      const { action } = req.body;

      if (action === 'add') {
        const { nome, tipo, parent } = req.body;
        if (!nome) return res.status(400).json({ error: 'Nome obrigatorio' });
        // Get max ordem
        const maxR = await pool.query('SELECT MAX(ordem) as m FROM caixa_categorias WHERE empresa=$1', [company]);
        const ordem = (parseInt(maxR.rows[0].m) || 0) + 1;
        const r = await pool.query(
          `INSERT INTO caixa_categorias (empresa, nome, tipo, parent, ordem)
           VALUES ($1,$2,$3,$4,$5) ON CONFLICT (empresa, nome) DO UPDATE SET tipo=EXCLUDED.tipo, parent=EXCLUDED.parent
           RETURNING id, nome, tipo, parent, ordem`,
          [company, nome.substring(0, 100), tipo || 'item', parent || null, ordem]
        );
        return res.json({ ok: true, categoria: r.rows[0] });
      }

      return res.status(400).json({ error: 'action invalida' });
    }

    if (req.method === 'DELETE') {
      const { id } = req.query;
      if (!id) return res.status(400).json({ error: 'Informe id' });
      await pool.query('DELETE FROM caixa_categorias WHERE id=$1 AND empresa=$2', [parseInt(id), company]);
      return res.json({ ok: true });
    }

    res.status(405).json({ error: 'Method not allowed' });
  } catch (e) {
    console.error('[CAIXA-MODELO]', e.message);
    res.status(500).json({ error: e.message });
  }
};
