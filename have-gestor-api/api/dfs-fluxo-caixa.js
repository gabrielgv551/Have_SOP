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
    // GET - list months or get specific period
    if (req.method === 'GET') {
      const { ano, mes, dia } = req.query;

      if (ano && mes && dia) {
        const result = await pool.query(
          'SELECT tipo, valor FROM dfs_fluxo_caixa_diario WHERE empresa=$1 AND ano=$2 AND mes=$3 AND dia=$4 ORDER BY tipo',
          [company, parseInt(ano), parseInt(mes), parseInt(dia)]
        );
        return res.json({ ano: parseInt(ano), mes: parseInt(mes), dia: parseInt(dia), rows: result.rows });
      }

      if (ano && mes) {
        const result = await pool.query(
          'SELECT dia, tipo, valor FROM dfs_fluxo_caixa_diario WHERE empresa=$1 AND ano=$2 AND mes=$3 ORDER BY dia, tipo',
          [company, parseInt(ano), parseInt(mes)]
        );
        return res.json({ ano: parseInt(ano), mes: parseInt(mes), rows: result.rows });
      }

      // List all available months
      const result = await pool.query(
        `SELECT ano, mes, COUNT(DISTINCT dia)::int as total_dias, COUNT(*)::int as total_registros,
                MAX(atualizado_em) as ultima_atualizacao
         FROM dfs_fluxo_caixa_diario WHERE empresa=$1
         GROUP BY ano, mes ORDER BY ano DESC, mes DESC`,
        [company]
      );
      return res.json({ meses: result.rows });
    }

    // POST - upsert daily data for a month
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

        const CHUNK = 200;
        let upserted = 0;
        for (let i = 0; i < rows.length; i += CHUNK) {
          const chunk = rows.slice(i, i + CHUNK);
          const vals = [];
          const params = [];
          chunk.forEach((r, idx) => {
            const base = idx * 7;
            vals.push(`($${base+1},$${base+2},$${base+3},$${base+4},$${base+5},$${base+6},$${base+7})`);
            params.push(company, a, m, parseInt(r.dia), String(r.tipo), parseInt(r.valor) || 0, new Date());
          });
          await client.query(
            `INSERT INTO dfs_fluxo_caixa_diario (empresa, ano, mes, dia, tipo, valor, atualizado_em)
             VALUES ${vals.join(',')}
             ON CONFLICT (empresa, ano, mes, dia, tipo) DO UPDATE
               SET valor = EXCLUDED.valor, atualizado_em = EXCLUDED.atualizado_em`,
            params
          );
          upserted += chunk.length;
        }

        await client.query('COMMIT');
        return res.json({ ok: true, count: upserted });
      } catch (e) {
        await client.query('ROLLBACK');
        throw e;
      } finally {
        client.release();
      }
    }

    // DELETE - remove all data for a month
    if (req.method === 'DELETE') {
      const { ano, mes } = req.query;
      if (!ano || !mes) return res.status(400).json({ error: 'Informe ano e mes' });
      await pool.query(
        'DELETE FROM dfs_fluxo_caixa_diario WHERE empresa=$1 AND ano=$2 AND mes=$3',
        [company, parseInt(ano), parseInt(mes)]
      );
      return res.json({ ok: true });
    }

    res.status(405).json({ error: 'Method not allowed' });
  } catch (e) {
    console.error('[DFS-FLUXO-CAIXA]', e.message);
    res.status(500).json({ error: e.message });
  }
};
