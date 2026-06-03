const jwt = require('jsonwebtoken');
const companies = require('../lib/companies');
const { getPool, getCompanyPool } = require('../lib/db');

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

  const { company, pool } = getCompanyPool(payload);
  const companyKey = (companies[company] && companies[company].dbEnvKey) || company.toUpperCase();
  if (!process.env[`${companyKey}_HOST`]) {
    return res.status(503).json({ error: 'Banco de dados não configurado para esta empresa.' });
  }

  try {
    if (req.method === 'GET') {
      let result = await pool.query(
        'SELECT tipo, dados FROM dfs_estrutura WHERE empresa = $1',
        [company]
      );

      // Se empresa não tem estrutura, clonar do template padrão (lanzi)
      if (result.rows.length === 0 && company !== 'lanzi') {
        const template = await pool.query(
          "SELECT tipo, dados FROM dfs_estrutura WHERE empresa = $1 AND tipo IN ('structure','dre_structure')",
          ['lanzi']
        );
        if (template.rows.length > 0) {
          for (const row of template.rows) {
            await pool.query(
              `INSERT INTO dfs_estrutura (empresa, tipo, dados, atualizado_em)
               VALUES ($1, $2, $3, NOW())
               ON CONFLICT (empresa, tipo) DO UPDATE SET dados = $3, atualizado_em = NOW()`,
              [company, row.tipo, JSON.stringify(row.dados)]
            );
          }
          // Recarregar após clonagem
          result = await pool.query(
            'SELECT tipo, dados FROM dfs_estrutura WHERE empresa = $1',
            [company]
          );
        }
      }

      const out = { structure: {}, mappings: {}, dre_structure: {}, dre_mappings: {}, fcx_data: {}, fcx_mappings: {}, kpi_formulas: null, manual_overrides: {} };
      result.rows.forEach(r => { out[r.tipo] = r.dados; });
      return res.json(out);
    }

    if (req.method === 'POST') {
      const { type, data } = req.body;
      if (!type || !data) return res.status(400).json({ error: 'Campos obrigatorios: type, data' });
      if (!['structure', 'mappings', 'dre_structure', 'dre_mappings', 'fcx_data', 'fcx_mappings', 'kpi_formulas', 'manual_overrides'].includes(type)) return res.status(400).json({ error: 'type invalido' });

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
