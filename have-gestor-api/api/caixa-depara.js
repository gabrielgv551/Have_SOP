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
      const { source, ano, mes } = req.query;

      // Return unique descriptions from extract not yet in de-para
      if (source === 'extract') {
        const query = ano && mes
          ? 'SELECT DISTINCT descricao FROM caixa_extrato WHERE empresa=$1 AND ano=$2 AND mes=$3 ORDER BY descricao'
          : 'SELECT DISTINCT descricao FROM caixa_extrato WHERE empresa=$1 ORDER BY descricao';
        const params = ano && mes ? [company, parseInt(ano), parseInt(mes)] : [company];
        const r = await pool.query(query, params);
        const dpR = await pool.query('SELECT palavra_chave FROM caixa_de_para WHERE empresa=$1', [company]);
        const mapped = new Set(dpR.rows.map(x => x.palavra_chave.toLowerCase()));
        return res.json({
          descricoes: r.rows.map(x => x.descricao),
          nao_classificadas: r.rows.map(x => x.descricao).filter(d => !mapped.has(d.toLowerCase()))
        });
      }

      // Return all mappings
      const r = await pool.query(
        'SELECT id, palavra_chave, categoria_nome FROM caixa_de_para WHERE empresa=$1 ORDER BY categoria_nome, palavra_chave',
        [company]
      );
      return res.json({ mappings: r.rows });
    }

    if (req.method === 'POST') {
      const { palavra_chave, categoria_nome } = req.body;
      if (!palavra_chave || !categoria_nome)
        return res.status(400).json({ error: 'Informe palavra_chave e categoria_nome' });
      const r = await pool.query(
        `INSERT INTO caixa_de_para (empresa, palavra_chave, categoria_nome)
         VALUES ($1,$2,$3)
         ON CONFLICT (empresa, palavra_chave) DO UPDATE SET categoria_nome=EXCLUDED.categoria_nome
         RETURNING id, palavra_chave, categoria_nome`,
        [company, palavra_chave.substring(0, 500), categoria_nome.substring(0, 100)]
      );
      return res.json({ ok: true, mapping: r.rows[0] });
    }

    if (req.method === 'DELETE') {
      const { id } = req.query;
      if (!id) return res.status(400).json({ error: 'Informe id' });
      await pool.query('DELETE FROM caixa_de_para WHERE id=$1 AND empresa=$2', [parseInt(id), company]);
      return res.json({ ok: true });
    }

    res.status(405).json({ error: 'Method not allowed' });
  } catch (e) {
    console.error('[CAIXA-DEPARA]', e.message);
    res.status(500).json({ error: e.message });
  }
};
