const jwt = require('jsonwebtoken');
const { Pool } = require('pg');
const companies = require('../lib/companies');

const pools = {};

function getPool(company) {
  if (pools[company]) return pools[company];
  const key = (companies[company] && companies[company].dbEnvKey) || company.toUpperCase();
  pools[company] = new Pool({
    host: process.env[`${key}_HOST`],
    port: parseInt(process.env[`${key}_PORT`] || '5432'),
    database: process.env[`${key}_DB`],
    user: process.env[`${key}_USER`],
    password: process.env[`${key}_PASSWORD`],
    ssl: { rejectUnauthorized: false },
    max: 5,
  });
  return pools[company];
}

function verifyToken(req, res) {
  const auth = (req.headers.authorization || '').split(' ')[1];
  if (!auth) { res.status(401).json({ error: 'Token nao fornecido' }); return null; }
  try {
    return jwt.verify(auth, process.env.JWT_SECRET);
  } catch {
    res.status(401).json({ error: 'Token invalido ou expirado' });
    return null;
  }
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
    // GET - list months or get specific month
    if (req.method === 'GET') {
      const { ano, mes } = req.query;
      if (ano && mes) {
        const result = await pool.query(
          'SELECT conta, nome, saldo_anterior, debito, credito, saldo_atual FROM dfs_balanco WHERE empresa=$1 AND ano=$2 AND mes=$3 ORDER BY conta',
          [company, parseInt(ano), parseInt(mes)]
        );
        return res.json({ ano: parseInt(ano), mes: parseInt(mes), rows: result.rows });
      }
      const result = await pool.query(
        `SELECT ano, mes, COUNT(*)::int as total_contas,
                SUM(debito)::float as total_debito,
                SUM(credito)::float as total_credito,
                MAX(atualizado_em) as ultima_atualizacao
         FROM dfs_balanco WHERE empresa=$1
         GROUP BY ano, mes ORDER BY ano DESC, mes DESC`,
        [company]
      );
      return res.json({ meses: result.rows });
    }

    // POST - upload month data
    if (req.method === 'POST') {
      const { ano, mes, rows } = req.body;
      if (!ano || !mes || !rows || !Array.isArray(rows) || !rows.length) {
        return res.status(400).json({ error: 'Campos obrigatorios: ano, mes, rows (array)' });
      }
      const a = parseInt(ano), m = parseInt(mes);
      if (m < 1 || m > 12) return res.status(400).json({ error: 'Mes invalido' });

      const client = await pool.connect();
      try {
        await client.query('BEGIN');
        await client.query('DELETE FROM dfs_balanco WHERE empresa=$1 AND ano=$2 AND mes=$3', [company, a, m]);

        const CHUNK = 200;
        for (let i = 0; i < rows.length; i += CHUNK) {
          const chunk = rows.slice(i, i + CHUNK);
          const vals = [];
          const params = [];
          chunk.forEach((r, idx) => {
            const base = idx * 9;
            vals.push(`($${base+1},$${base+2},$${base+3},$${base+4},$${base+5},$${base+6},$${base+7},$${base+8},$${base+9})`);
            params.push(company, a, m, String(r.conta||''), String(r.nome||''), r.saldo_anterior||0, r.debito||0, r.credito||0, r.saldo_atual||0);
          });
          await client.query(
            `INSERT INTO dfs_balanco (empresa, ano, mes, conta, nome, saldo_anterior, debito, credito, saldo_atual) VALUES ${vals.join(',')}`,
            params
          );
        }

        await client.query('COMMIT');
        return res.json({ ok: true, count: rows.length });
      } catch (e) {
        await client.query('ROLLBACK');
        throw e;
      } finally {
        client.release();
      }
    }

    // DELETE - remove month
    if (req.method === 'DELETE') {
      const { ano, mes } = req.query;
      if (!ano || !mes) return res.status(400).json({ error: 'Informe ano e mes' });
      await pool.query('DELETE FROM dfs_balanco WHERE empresa=$1 AND ano=$2 AND mes=$3', [company, parseInt(ano), parseInt(mes)]);
      return res.json({ ok: true });
    }

    res.status(405).json({ error: 'Method not allowed' });
  } catch (e) {
    console.error('[DFS-BASE]', e.message);
    res.status(500).json({ error: e.message });
  }
};
