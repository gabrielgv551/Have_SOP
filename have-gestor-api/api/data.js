const jwt = require('jsonwebtoken');
const { getPool } = require('../lib/db');
const handleTiny   = require('../lib/data-tiny');
const handleVendas = require('../lib/data-vendas');
const handleSopc   = require('../lib/data-sopc');

const TINY_MODULES  = ['tiny-oauth','tiny-debug','tiny-sync','cron-tiny','tiny-skip-old','tiny-enrich','tiny-margem','tiny-canais','import-margem'];
const VENDAS_MODULES = ['margens','vendas','forecast-canais','forecast-recebimentos','forecast-diario'];
const SOPC_MODULES  = ['sopc-config','fornecedores-config','sku-desativadas'];

module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PATCH, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');

  if (req.method === 'OPTIONS') return res.status(200).end();

  // PATCH — atualizar campo empresa de um título de contas_pagar
  if (req.method === 'PATCH') {
    const auth2 = (req.headers.authorization || '').split(' ')[1];
    if (!auth2) return res.status(401).json({ error: 'Token não fornecido' });
    let p2;
    try { p2 = jwt.verify(auth2, process.env.JWT_SECRET); }
    catch { return res.status(401).json({ error: 'Token inválido' }); }
    const pool2 = getPool(p2.company || 'lanzi');
    const { id, empresa } = req.body || {};
    if (!id) return res.status(400).json({ error: 'id obrigatorio' });
    try {
      await pool2.query('UPDATE contas_pagar SET empresa=$1 WHERE id=$2', [empresa || null, String(id)]);
      return res.json({ ok: true });
    } catch(e) { return res.status(500).json({ error: e.message }); }
  }

  // Módulo público — sem autenticação (só expõe client IDs, nunca secrets)
  if (req.query.module === 'public-config') {
    const company = req.query.company || 'lanzi';
    const pool = getPool(company);
    try {
      const r = await pool.query(`SELECT valor FROM configuracoes WHERE empresa=$1 AND chave='tiny_client_id'`, [company]);
      const dbClientId = r.rows[0]?.valor || '';
      return res.json({
        tiny_client_id: dbClientId || (process.env.TINY_CLIENT_ID || '').trim(),
      });
    } catch { return res.json({ tiny_client_id: (process.env.TINY_CLIENT_ID || '').trim() }); }
  }

  // 1. Verificar token JWT
  const auth = (req.headers.authorization || '').split(' ')[1];
  if (!auth) return res.status(401).json({ error: 'Token não fornecido' });

  let payload;
  try {
    payload = jwt.verify(auth, process.env.JWT_SECRET);
  } catch {
    return res.status(401).json({ error: 'Token inválido ou expirado. Faça login novamente.' });
  }

  const mod = req.query.module;

  // Módulo Sync Vendas
  if (mod === 'sync-vendas') {
    const company = payload.company || 'lanzi';
    const pool = getPool(company);
    try {
      const tableCheck = await pool.query(`SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name='sync_log') AS existe`);
      if (!tableCheck.rows[0].existe) {
        return res.json({ ultima_sincronizacao: null, registros: null, status: 'nunca_sincronizado', mensagem: 'Nenhuma sincronização realizada ainda.' });
      }
      const syncRes = await pool.query(`SELECT data_sync, registros, status, origem FROM sync_log WHERE tabela='bd_vendas' ORDER BY data_sync DESC LIMIT 1`);
      if (!syncRes.rows.length) return res.json({ ultima_sincronizacao: null, registros: null, status: 'nunca_sincronizado' });
      const ultimo = syncRes.rows[0];
      const historicoRes = await pool.query(`SELECT data_sync, registros, status, origem FROM sync_log WHERE tabela='bd_vendas' ORDER BY data_sync DESC LIMIT 10`);
      let registros_atuais = null;
      try { const c = await pool.query('SELECT COUNT(*) AS total FROM bd_vendas'); registros_atuais = parseInt(c.rows[0].total); } catch(_) {}
      return res.json({ ultima_sincronizacao: ultimo.data_sync, registros: parseInt(ultimo.registros), registros_atuais, status: ultimo.status, origem: ultimo.origem, historico: historicoRes.rows });
    } catch(e) { console.error('[SYNC-VENDAS]', e.message); return res.status(500).json({ error: e.message }); }
  }

  // Módulo Configurações
  if (mod === 'configuracoes') {
    const company = payload.company || 'lanzi';
    const pool = getPool(company);
    try {
      await pool.query(`CREATE TABLE IF NOT EXISTS configuracoes (empresa VARCHAR(50) NOT NULL, chave VARCHAR(100) NOT NULL, valor TEXT, atualizado_em TIMESTAMP DEFAULT NOW(), PRIMARY KEY (empresa, chave))`);
      if (req.method === 'GET') {
        const r = await pool.query(`SELECT chave, valor FROM configuracoes WHERE empresa=$1`, [company]);
        const result = {};
        r.rows.forEach(({ chave, valor }) => {
          const sensitive = chave.endsWith('_token') || chave.endsWith('_refresh') || chave.endsWith('_secret') || chave === 'gefinance_password';
          result[chave] = sensitive ? (valor ? '***' : null) : valor;
        });
        result.gefinance_password_set = !!result.gefinance_password;
        return res.json(result);
      }
      if (req.method === 'POST') {
        const body = (typeof req.body === 'string') ? JSON.parse(req.body) : (req.body || {});
        const updates = Object.entries(body).filter(([k, v]) => typeof k === 'string' && k.length > 0);
        if (!updates.length) return res.status(400).json({ error: 'Nenhum campo válido enviado.' });
        for (const [chave, valor] of updates) {
          if (valor === '' || valor === null) {
            await pool.query(`DELETE FROM configuracoes WHERE empresa=$1 AND chave LIKE $2`, [company, chave.replace(/_token$/, '') + '%']);
          } else {
            await pool.query(`INSERT INTO configuracoes (empresa, chave, valor, atualizado_em) VALUES ($1,$2,$3,NOW()) ON CONFLICT (empresa, chave) DO UPDATE SET valor=EXCLUDED.valor, atualizado_em=NOW()`, [company, chave, String(valor)]);
          }
        }
        return res.json({ ok: true, saved: updates.map(([k]) => k) });
      }
      return res.status(405).json({ error: 'Method not allowed' });
    } catch(e) { console.error('[CONFIGURACOES]', e.message); return res.status(500).json({ error: e.message }); }
  }

  // Módulo ML OAuth — troca code por tokens
  if (mod === 'ml-oauth') {
    if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });
    const _mlBody = (typeof req.body === 'string') ? (() => { try { return JSON.parse(req.body); } catch { return {}; } })() : (req.body || {});
    const { code, state } = _mlBody;
    if (!code || !state) return res.status(400).json({ error: 'code e state são obrigatórios', debug: { bodyType: typeof req.body, keys: Object.keys(_mlBody), hasCode: !!code, hasState: !!state } });
    const ML_CLIENT_ID     = process.env.ML_CLIENT_ID     || '2803787506623043';
    const ML_CLIENT_SECRET = process.env.ML_CLIENT_SECRET || 'y7HAmpTr8wWjWwTL55pJiwq3y1MNxCkE';
    const ML_REDIRECT_URI  = 'https://have-gestor-frontend.vercel.app/ml-callback';
    try {
      const tokenRes = await fetch('https://api.mercadolibre.com/oauth/token', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Accept': 'application/json' },
        body: JSON.stringify({ grant_type: 'authorization_code', client_id: ML_CLIENT_ID, client_secret: ML_CLIENT_SECRET, code, redirect_uri: ML_REDIRECT_URI }),
      });
      const tokenData = await tokenRes.json();
      if (!tokenRes.ok) return res.status(400).json({ error: tokenData.message || 'Erro ao trocar código ML' });
      let nick = state;
      try {
        const uRes = await fetch('https://api.mercadolibre.com/users/me', { headers: { 'Authorization': 'Bearer ' + tokenData.access_token } });
        const uData = await uRes.json();
        nick = uData.nickname || uData.email || state;
      } catch {}
      const company = payload.company || 'lanzi';
      const pool = getPool(company);
      const chave = state + '_token';
      const expAt = new Date(Date.now() + (tokenData.expires_in || 21600) * 1000).toISOString();
      await pool.query(`CREATE TABLE IF NOT EXISTS configuracoes (empresa VARCHAR(50) NOT NULL, chave VARCHAR(100) NOT NULL, valor TEXT, atualizado_em TIMESTAMP DEFAULT NOW(), PRIMARY KEY (empresa, chave))`);
      for (const [k, v] of [[chave, tokenData.access_token],[chave+'_refresh', tokenData.refresh_token],[chave+'_nick', nick],[chave+'_user_id', String(tokenData.user_id||'')],[chave+'_exp', expAt]]) {
        await pool.query(`INSERT INTO configuracoes (empresa,chave,valor,atualizado_em) VALUES ($1,$2,$3,NOW()) ON CONFLICT (empresa,chave) DO UPDATE SET valor=EXCLUDED.valor,atualizado_em=NOW()`, [company, k, v]);
      }
      return res.json({ ok: true, nick });
    } catch(e) { return res.status(500).json({ error: e.message }); }
  }

  // Módulo ML Remove — desconectar conta ML (apaga tokens + tabela via worker)
  if (mod === 'ml-remove') {
    if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });
    const _body = (typeof req.body === 'string') ? (() => { try { return JSON.parse(req.body); } catch { return {}; } })() : (req.body || {});
    const { account_id } = _body;
    if (!account_id) return res.status(400).json({ error: 'account_id é obrigatório' });

    const company = payload.company || 'lanzi';
    const pool = getPool(company);

    try {
      // 1. Apagar tokens da conta em configuracoes
      const del = await pool.query(
        `DELETE FROM configuracoes WHERE chave LIKE $1`,
        [`${account_id}%`]
      );

      return res.json({ ok: true, chaves_removidas: del.rowCount });
    } catch(e) { return res.status(500).json({ error: e.message }); }
  }

  // Dispatch to module handlers (see lib/)
  if (TINY_MODULES.includes(mod))   return handleTiny(req, res, payload);
  if (VENDAS_MODULES.includes(mod)) return handleVendas(req, res, payload);
  return handleSopc(req, res, payload);
};
