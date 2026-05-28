const jwt = require('jsonwebtoken');
const { getPool } = require('../lib/db');

function verifyToken(req, res) {
  const auth = (req.headers.authorization || '').split(' ')[1];
  if (!auth) { res.status(401).json({ error: 'Token nao fornecido' }); return null; }
  try { return jwt.verify(auth, process.env.JWT_SECRET); }
  catch { res.status(401).json({ error: 'Token invalido' }); return null; }
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Headers', 'Authorization, Content-Type');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, DELETE, OPTIONS');
  if (req.method === 'OPTIONS') return res.status(200).end();

  const user = verifyToken(req, res);
  if (!user) return;

  const company = user.company || 'lanzi';
  const pool = getPool(company);

  try {

    // ── GET: list all active kit mappings ─────────────────────────────────────
    if (req.method === 'GET') {
      const result = await pool.query(`
        SELECT id, sku_kit, sku_componente, quantidade::float, criado_em
        FROM sku_kits
        WHERE empresa = $1 AND ativo = true
        ORDER BY sku_kit, sku_componente
      `, [company]);
      return res.json({ kits: result.rows });
    }

    // ── POST: upsert a kit-component mapping ──────────────────────────────────
    if (req.method === 'POST') {
      const { sku_kit, sku_componente, quantidade } = req.body || {};
      if (!sku_kit || !sku_componente || !quantidade) {
        return res.status(400).json({ error: 'sku_kit, sku_componente e quantidade são obrigatórios' });
      }
      const qty = parseFloat(quantidade);
      if (isNaN(qty) || qty <= 0) {
        return res.status(400).json({ error: 'quantidade deve ser um número positivo' });
      }

      const result = await pool.query(`
        INSERT INTO sku_kits (empresa, sku_kit, sku_componente, quantidade)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (empresa, sku_kit, sku_componente)
        DO UPDATE SET quantidade = EXCLUDED.quantidade, ativo = true, criado_em = NOW()
        RETURNING id, sku_kit, sku_componente, quantidade::float
      `, [company, sku_kit.trim().toUpperCase(), sku_componente.trim().toUpperCase(), qty]);

      return res.json({ kit: result.rows[0] });
    }

    // ── DELETE: soft-delete a mapping by id ───────────────────────────────────
    if (req.method === 'DELETE') {
      const id = parseInt(req.query.id);
      if (!id) return res.status(400).json({ error: 'id é obrigatório' });

      await pool.query(`
        UPDATE sku_kits SET ativo = false
        WHERE id = $1 AND empresa = $2
      `, [id, company]);

      return res.json({ ok: true });
    }

    return res.status(405).json({ error: 'Method not allowed' });

  } catch (err) {
    console.error('[sku-kits]', err);
    return res.status(500).json({ error: err.message });
  }
};
