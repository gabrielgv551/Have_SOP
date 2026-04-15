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

function addDays(dateStr, days) {
  const d = new Date(dateStr + 'T00:00:00Z');
  d.setUTCDate(d.getUTCDate() + days);
  return d.toISOString().slice(0, 10);
}

// ── Fornecedores sub-handler ─────────────────────────────────────────────────
async function handleFornecedores(req, res, pool, company) {
  if (req.method === 'GET') {
    const r = await pool.query(
      'SELECT id, nome, prazo_padrao_dias, criado_em FROM pc_fornecedores WHERE empresa=$1 ORDER BY nome ASC',
      [company]
    );
    return res.json({ fornecedores: r.rows });
  }
  if (req.method === 'POST') {
    const { nome, prazo_padrao_dias } = req.body;
    if (!nome) return res.status(400).json({ error: 'Informe nome' });
    const prazo = parseInt(prazo_padrao_dias) || 30;
    const r = await pool.query(
      `INSERT INTO pc_fornecedores (empresa, nome, prazo_padrao_dias)
       VALUES ($1,$2,$3)
       ON CONFLICT (empresa, nome) DO UPDATE SET prazo_padrao_dias=EXCLUDED.prazo_padrao_dias
       RETURNING id, nome, prazo_padrao_dias, criado_em`,
      [company, nome.substring(0, 200), prazo]
    );
    return res.json({ ok: true, fornecedor: r.rows[0] });
  }
  if (req.method === 'DELETE') {
    const { id } = req.query;
    if (!id) return res.status(400).json({ error: 'Informe id' });
    await pool.query('DELETE FROM pc_fornecedores WHERE id=$1 AND empresa=$2', [parseInt(id), company]);
    return res.json({ ok: true });
  }
  return res.status(405).json({ error: 'Method not allowed' });
}

// ── Main handler ─────────────────────────────────────────────────────────────
module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, DELETE, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  if (req.method === 'OPTIONS') return res.status(200).end();

  const payload = verifyToken(req, res);
  if (!payload) return;
  const company = payload.company || 'lanzi';
  const pool = getPool(company);

  if (req.query.module === 'fornecedores') return handleFornecedores(req, res, pool, company);

  try {
    if (req.method === 'GET') {
      const { ano, mes } = req.query;
      let q = `SELECT id, fornecedor, data_emissao, nf, valor, vencimento, vencimento_ajustado,
                      parcela, total_parcelas, created_at
               FROM pedidos_compra WHERE empresa=$1`;
      const params = [company];
      if (ano && mes) {
        q += ` AND (
          EXTRACT(YEAR FROM COALESCE(vencimento_ajustado, vencimento))=$2
          AND EXTRACT(MONTH FROM COALESCE(vencimento_ajustado, vencimento))=$3
        )`;
        params.push(parseInt(ano), parseInt(mes));
      }
      q += ' ORDER BY COALESCE(vencimento_ajustado, vencimento) ASC, id ASC';
      const r = await pool.query(q, params);
      return res.json({ pedidos: r.rows });
    }

    if (req.method === 'POST') {
      const { fornecedor, data_emissao, nf, valor, vencimento, vencimento_ajustado, prazos } = req.body;
      if (!fornecedor || !data_emissao || valor == null)
        return res.status(400).json({ error: 'Informe fornecedor, data_emissao e valor' });

      const valorNum = parseFloat(valor);
      if (isNaN(valorNum) || valorNum < 0)
        return res.status(400).json({ error: 'Valor invalido' });

      const client = await pool.connect();
      try {
        await client.query('BEGIN');
        const inserted = [];

        // prazos is an array of day offsets from data_emissao, e.g. [30, 60, 90] or [14, 21, 60]
        if (Array.isArray(prazos) && prazos.length > 0) {
          const validPrazos = prazos.map(p => parseInt(p)).filter(p => p > 0);
          if (!validPrazos.length) return res.status(400).json({ error: 'Informe ao menos um prazo valido' });
          const n = validPrazos.length;
          const valorParcela = Math.round((valorNum / n) * 100) / 100;
          for (let i = 0; i < n; i++) {
            const venc = addDays(data_emissao, validPrazos[i]);
            const r = await client.query(
              `INSERT INTO pedidos_compra
                 (empresa, fornecedor, data_emissao, nf, valor, vencimento, vencimento_ajustado, parcela, total_parcelas)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
               RETURNING id, fornecedor, data_emissao, nf, valor, vencimento, vencimento_ajustado, parcela, total_parcelas, created_at`,
              [company, fornecedor.substring(0, 200), data_emissao,
               nf ? nf.substring(0, 100) : null, valorParcela,
               venc, vencimento_ajustado || null, i + 1, n]
            );
            inserted.push(r.rows[0]);
          }
        } else {
          // Single record — requires explicit vencimento
          if (!vencimento) return res.status(400).json({ error: 'Informe vencimento' });
          const r = await client.query(
            `INSERT INTO pedidos_compra
               (empresa, fornecedor, data_emissao, nf, valor, vencimento, vencimento_ajustado, parcela, total_parcelas)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
             RETURNING id, fornecedor, data_emissao, nf, valor, vencimento, vencimento_ajustado, parcela, total_parcelas, created_at`,
            [company, fornecedor.substring(0, 200), data_emissao,
             nf ? nf.substring(0, 100) : null, valorNum,
             vencimento, vencimento_ajustado || null, null, null]
          );
          inserted.push(r.rows[0]);
        }

        await client.query('COMMIT');
        return res.json({ ok: true, pedidos: inserted });
      } catch (e) {
        await client.query('ROLLBACK'); throw e;
      } finally { client.release(); }
    }

    if (req.method === 'DELETE') {
      const { id } = req.query;
      if (!id) return res.status(400).json({ error: 'Informe id' });
      await pool.query('DELETE FROM pedidos_compra WHERE id=$1 AND empresa=$2', [parseInt(id), company]);
      return res.json({ ok: true });
    }

    res.status(405).json({ error: 'Method not allowed' });
  } catch (e) {
    console.error('[PEDIDOS-COMPRA]', e.message);
    res.status(500).json({ error: e.message });
  }
};
