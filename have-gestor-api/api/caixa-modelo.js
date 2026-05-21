const jwt = require('jsonwebtoken');
const { nextBizDay, DEFAULT_CATEGORIAS, seedDefaults, consolidarMes, consolidarAnual } = require('../lib/consolidar-caixa');
const { getPool, getCompanyPool } = require('../lib/db');

function verifyToken(req, res) {
  const auth = (req.headers.authorization || '').split(' ')[1];
  if (!auth) { res.status(401).json({ error: 'Token nao fornecido' }); return null; }
  try { return jwt.verify(auth, process.env.JWT_SECRET); }
  catch { res.status(401).json({ error: 'Token invalido' }); return null; }
}

module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, DELETE, PATCH, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  if (req.method === 'OPTIONS') return res.status(200).end();

  const payload = verifyToken(req, res);
  if (!payload) return;
  const { company, pool } = getCompanyPool(payload);

  try {
    // ── BULK ANUAL: ?anual=true&ano=2026 ────────────────────────────
    if (req.method === 'GET' && req.query.anual === 'true') {
      const ano = parseInt(req.query.ano);
      if (!ano) return res.status(400).json({ error: 'Informe ano' });
      const _t0 = Date.now();
      const result = await consolidarAnual(pool, company, ano, {
        apenas_futuros: req.query.apenas_futuros !== 'false',
        fonte_realizado: req.query.fonte_realizado || 'ambos',
        subempresa_id: req.query.subempresa_id || null,
      });
      console.log(`[CAIXA-ANUAL] total: ${Date.now()-_t0}ms`);
      return res.json(result);
    }

    if (req.method === 'GET') {
      const { ano, mes } = req.query;

      if (!ano || !mes) {
        const countR = await pool.query('SELECT COUNT(*) FROM caixa_categorias WHERE empresa=$1', [company]);
        if (parseInt(countR.rows[0].count) === 0) await seedDefaults(pool, company);
        const catsR = await pool.query(
          'SELECT id, nome, tipo, parent, ordem FROM caixa_categorias WHERE empresa=$1 ORDER BY ordem',
          [company]
        );
        return res.json({ categorias: catsR.rows });
      }

      const result = await consolidarMes(pool, company, ano, mes, {
        apenas_futuros: req.query.apenas_futuros !== 'false',
        fonte_realizado: req.query.fonte_realizado || 'ambos',
        subempresa_id: req.query.subempresa_id || null,
      });
      return res.json(result);
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

      if (action === 'reset_defaults') {
        const client = await pool.connect();
        try {
          await client.query('BEGIN');
          await client.query('DELETE FROM caixa_categorias WHERE empresa=$1', [company]);
          await client.query('COMMIT');
        } catch (e) { await client.query('ROLLBACK'); throw e; }
        finally { client.release(); }
        await seedDefaults(pool, company);
        return res.json({ ok: true, count: DEFAULT_CATEGORIAS.length });
      }

      if (action === 'bulk_save') {
        const { categorias } = req.body;
        if (!Array.isArray(categorias)) return res.status(400).json({ error: 'categorias deve ser array' });
        const client = await pool.connect();
        try {
          await client.query('BEGIN');
          for (const cat of categorias) {
            if (!cat.id) continue;
            await client.query(
              'UPDATE caixa_categorias SET nome=$1, parent=$2, ordem=$3 WHERE id=$4 AND empresa=$5',
              [String(cat.nome).substring(0,100), cat.parent||null, parseInt(cat.ordem)||0, parseInt(cat.id), company]
            );
          }
          await client.query('COMMIT');
        } catch (e) { await client.query('ROLLBACK'); throw e; }
        finally { client.release(); }
        return res.json({ ok: true });
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
