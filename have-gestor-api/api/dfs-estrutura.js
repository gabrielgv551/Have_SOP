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
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  if (req.method === 'OPTIONS') return res.status(200).end();

  const payload = verifyToken(req, res);
  if (!payload) return;

  const company = payload.company || 'lanzi';
  const pool = getPool(company);

  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS dfs_estrutura (
        id SERIAL PRIMARY KEY,
        empresa VARCHAR(50) NOT NULL,
        tipo VARCHAR(20) NOT NULL,
        dados JSONB DEFAULT '{}',
        atualizado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(empresa, tipo)
      )
    `);

    if (req.method === 'GET') {
      const result = await pool.query(
        'SELECT tipo, dados FROM dfs_estrutura WHERE empresa = $1',
        [company]
      );
      const out = { structure: {}, mappings: {} };
      result.rows.forEach(r => { out[r.tipo] = r.dados; });
      return res.json(out);
    }

    if (req.method === 'POST') {
      const { type, data } = req.body;
      if (!type || !data) return res.status(400).json({ error: 'Campos obrigatorios: type, data' });
      if (!['structure', 'mappings', 'dre_structure', 'dre_mappings'].includes(type)) return res.status(400).json({ error: 'type invalido' });

      await pool.query(
        `INSERT INTO dfs_estrutura (empresa, tipo, dados, atualizado_em)
         VALUES ($1, $2, $3, NOW())
         ON CONFLICT (empresa, tipo) DO UPDATE SET dados = $3, atualizado_em = NOW()`,
        [company, type, JSON.stringify(data)]
      );
      return res.json({ ok: true });
    }

    res.status(405).json({ error: 'Method not allowed' });
  } catch (e) {
    console.error('[DFS-ESTRUTURA]', e.message);
    res.status(500).json({ error: e.message });
  }
};
