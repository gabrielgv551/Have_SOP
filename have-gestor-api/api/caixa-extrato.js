const jwt = require('jsonwebtoken');
const { Pool } = require('pg');
const companies = require('../lib/companies');

const PLUGGY_BASE = 'https://api.pluggy.ai';

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

async function pluggyGetApiKey() {
  const r = await fetch(`${PLUGGY_BASE}/auth`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ clientId: process.env.PLUGGY_CLIENT_ID, clientSecret: process.env.PLUGGY_CLIENT_SECRET })
  });
  if (!r.ok) throw new Error(`Pluggy auth error ${r.status}: ${await r.text()}`);
  const data = await r.json();
  return data.apiKey;
}

function pluggyHeaders(apiKey) {
  return { 'X-API-KEY': apiKey, 'Content-Type': 'application/json' };
}

async function pluggyGet(apiKey, path) {
  return fetch(`${PLUGGY_BASE}${path}`, { headers: pluggyHeaders(apiKey) });
}

async function pluggyPost(apiKey, path, body = {}) {
  return fetch(`${PLUGGY_BASE}${path}`, { method: 'POST', headers: pluggyHeaders(apiKey), body: JSON.stringify(body) });
}

async function pluggyDelete(apiKey, path) {
  return fetch(`${PLUGGY_BASE}${path}`, { method: 'DELETE', headers: pluggyHeaders(apiKey) });
}

async function fetchAllPluggyTransactions(apiKey, itemId, date_from, date_to) {
  const accRes = await pluggyGet(apiKey, `/accounts?itemId=${itemId}`);
  if (!accRes.ok) throw new Error(`Pluggy accounts error ${accRes.status}: ${await accRes.text()}`);
  const accData = await accRes.json();
  const accounts = accData.results || [];
  const transactions = [];
  for (const account of accounts) {
    let page = 1;
    while (true) {
      const r = await pluggyGet(apiKey, `/transactions?accountId=${account.id}&from=${date_from}&to=${date_to}&pageSize=500&page=${page}`);
      if (!r.ok) break;
      const data = await r.json();
      transactions.push(...(data.results || []));
      if (page >= (data.totalPages || 1)) break;
      page++;
    }
  }
  return transactions;
}

async function handlePluggy(req, res, pool, company) {
  if (!process.env.PLUGGY_CLIENT_ID || !process.env.PLUGGY_CLIENT_SECRET)
    return res.status(503).json({ error: 'Pluggy nao configurado. Adicione PLUGGY_CLIENT_ID e PLUGGY_CLIENT_SECRET nas variaveis de ambiente.' });

  if (req.method === 'GET') {
    const r = await pool.query(
      'SELECT id, link_id, institution, account_type, ultimo_sync, ativo, criado_em FROM belvo_links WHERE empresa=$1 ORDER BY criado_em DESC',
      [company]
    );
    return res.json({ links: r.rows });
  }

  if (req.method === 'POST') {
    const { action } = req.body;

    if (action === 'widget_token') {
      const apiKey = await pluggyGetApiKey();
      const r = await pluggyPost(apiKey, '/connect_token');
      if (!r.ok) return res.status(r.status).json({ error: `Pluggy: ${await r.text()}` });
      const data = await r.json();
      return res.json({ access: data.accessToken });
    }

    if (action === 'register_link') {
      const { link_id, institution, account_type } = req.body;
      if (!link_id) return res.status(400).json({ error: 'link_id obrigatorio' });
      const r = await pool.query(
        `INSERT INTO belvo_links (empresa, link_id, institution, account_type)
         VALUES ($1,$2,$3,$4)
         ON CONFLICT (empresa, link_id) DO UPDATE
           SET institution=EXCLUDED.institution, account_type=EXCLUDED.account_type, ativo=true
         RETURNING id, link_id, institution, account_type, ultimo_sync, ativo, criado_em`,
        [company, link_id, institution || null, account_type || null]
      );
      return res.json({ ok: true, link: r.rows[0] });
    }

    if (action === 'sync') {
      const { link_id, date_from, date_to } = req.body;
      if (!link_id || !date_from || !date_to)
        return res.status(400).json({ error: 'link_id, date_from e date_to sao obrigatorios' });

      const apiKey = await pluggyGetApiKey();
      const transactions = await fetchAllPluggyTransactions(apiKey, link_id, date_from, date_to);
      if (!transactions.length)
        return res.json({ ok: true, count: 0, message: 'Nenhuma transacao encontrada no periodo' });

      const client = await pool.connect();
      try {
        await client.query('BEGIN');
        let imported = 0;
        for (const tx of transactions) {
          const rawDate = tx.date;
          if (!rawDate) continue;
          const d = new Date(rawDate);
          const ano = d.getUTCFullYear(), mes = d.getUTCMonth() + 1, dia = d.getUTCDate();
          const descricao = String(tx.description || tx.descriptionRaw || '').substring(0, 500);
          const sinal = (tx.type === 'DEBIT') ? -1 : 1;
          const valor = Math.round((parseFloat(tx.amount) || 0) * 100) * sinal;
          await client.query(
            `INSERT INTO caixa_extrato (empresa, ano, mes, dia, descricao, valor, belvo_tx_id)
             VALUES ($1,$2,$3,$4,$5,$6,$7)
             ON CONFLICT (empresa, belvo_tx_id) DO UPDATE
               SET ano=EXCLUDED.ano, mes=EXCLUDED.mes, dia=EXCLUDED.dia,
                   descricao=EXCLUDED.descricao, valor=EXCLUDED.valor, atualizado_em=CURRENT_TIMESTAMP`,
            [company, ano, mes, dia, descricao, valor, String(tx.id)]
          );
          imported++;
        }
        await client.query('UPDATE belvo_links SET ultimo_sync=NOW() WHERE empresa=$1 AND link_id=$2', [company, link_id]);
        await client.query('COMMIT');
        return res.json({ ok: true, count: imported });
      } catch (e) { await client.query('ROLLBACK'); throw e; }
      finally { client.release(); }
    }

    return res.status(400).json({ error: 'action invalida' });
  }

  if (req.method === 'DELETE') {
    const { link_id } = req.query;
    if (!link_id) return res.status(400).json({ error: 'Informe link_id' });
    try {
      const apiKey = await pluggyGetApiKey();
      await pluggyDelete(apiKey, `/items/${link_id}`);
    } catch(e) { console.error('[PLUGGY DELETE]', e.message); }
    await pool.query('UPDATE belvo_links SET ativo=false WHERE empresa=$1 AND link_id=$2', [company, link_id]);
    return res.json({ ok: true });
  }

  res.status(405).json({ error: 'Method not allowed' });
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

  // Roteamento: ?module=pluggy → Open Finance
  if (req.query.module === 'pluggy' || req.query.module === 'belvo') return handlePluggy(req, res, pool, company);

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
