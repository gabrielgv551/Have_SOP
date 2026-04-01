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
      const { ano, mes } = req.query;
      if (ano && mes) {
        const r = await pool.query(
          'SELECT id, dia, descricao, valor FROM caixa_extrato WHERE empresa=$1 AND ano=$2 AND mes=$3 ORDER BY dia, id',
          [company, parseInt(ano), parseInt(mes)]
        );
        return res.json({ rows: r.rows });
      }
      // List months with data
      const r = await pool.query(
        `SELECT ano, mes, COUNT(*)::int as total_registros, MAX(atualizado_em) as ultima_atualizacao
         FROM caixa_extrato WHERE empresa=$1 GROUP BY ano, mes ORDER BY ano DESC, mes DESC`,
        [company]
      );
      return res.json({ meses: r.rows });
    }

    if (req.method === 'POST') {
      const { ano, mes, rows } = req.body;
      if (!ano || !mes || !Array.isArray(rows) || !rows.length)
        return res.status(400).json({ error: 'Informe ano, mes e rows' });

      const client = await pool.connect();
      try {
        await client.query('BEGIN');
        // Delete existing for this month then reinsert
        await client.query(
          'DELETE FROM caixa_extrato WHERE empresa=$1 AND ano=$2 AND mes=$3',
          [company, parseInt(ano), parseInt(mes)]
        );
        const CHUNK = 200;
        let inserted = 0;
        for (let i = 0; i < rows.length; i += CHUNK) {
          const chunk = rows.slice(i, i + CHUNK);
          const vals = [], params = [];
          chunk.forEach((r, idx) => {
            const b = idx * 6;
            vals.push(`($${b+1},$${b+2},$${b+3},$${b+4},$${b+5},$${b+6})`);
            params.push(company, parseInt(ano), parseInt(mes), parseInt(r.dia), String(r.descricao || '').substring(0, 500), parseInt(r.valor) || 0);
          });
          await client.query(
            `INSERT INTO caixa_extrato (empresa, ano, mes, dia, descricao, valor) VALUES ${vals.join(',')}`,
            params
          );
          inserted += chunk.length;
        }
        await client.query('COMMIT');
        return res.json({ ok: true, count: inserted });
      } catch (e) {
        await client.query('ROLLBACK'); throw e;
      } finally { client.release(); }
    }

    if (req.method === 'DELETE') {
      const { ano, mes } = req.query;
      if (!ano || !mes) return res.status(400).json({ error: 'Informe ano e mes' });
      const r = await pool.query(
        'DELETE FROM caixa_extrato WHERE empresa=$1 AND ano=$2 AND mes=$3',
        [company, parseInt(ano), parseInt(mes)]
      );
      return res.json({ ok: true, deleted: r.rowCount });
    }

    res.status(405).json({ error: 'Method not allowed' });
  } catch (e) {
    console.error('[CAIXA-EXTRATO]', e.message);
    res.status(500).json({ error: e.message });
  }
};
