const { Pool } = require('pg');
const { getCompanyPool } = require('./db');
const { parseBody } = require('./data-helpers');

// Helper para aguardar (respeitar rate limit do Bling de 3 requisições por segundo)
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// Pool de conexão com o banco de dados central 'bling'
let blingPool = null;
function getBlingPool() {
  if (blingPool) return blingPool;
  
  // Utiliza as credenciais do cluster de produção
  const host = process.env.AUTOEQUIP_HOST || '37.60.236.200';
  const port = parseInt(process.env.AUTOEQUIP_PORT || '5432');
  const user = process.env.AUTOEQUIP_USER || 'postgres';
  const password = process.env.AUTOEQUIP_PASSWORD || '131105Gv';

  blingPool = new Pool({
    host,
    port,
    user,
    password,
    database: 'bling',
    ssl: { rejectUnauthorized: false },
    max: 5,
  });
  return blingPool;
}

module.exports = async function handleBling(req, res, payload) {
  const centralPool = getBlingPool();

  // 1. Módulo Bling OAuth — conectar conta Bling ERP v3 no banco central
  if (req.query.module === 'bling-oauth') {
    if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });
    const _tBody = parseBody(req.body);
    const { code, state, modules } = _tBody;
    if (!code || !state) return res.status(400).json({ error: 'code e state são obrigatórios' });

    const BLING_REDIRECT_URI  = 'https://have-gestor-frontend.vercel.app/bling-callback';
    const BLING_TOKEN_URL = 'https://api.bling.com.br/Api/v3/oauth/token';
    const { company, pool: companyPool } = getCompanyPool(payload);

    // Usa a credencial padrão fornecida pelo usuário, mas permite sobrescrever via env
    const BLING_CLIENT_ID     = (process.env.BLING_CLIENT_ID     || '7d24d3e4ab13c4e803b0441f52170ddc261395b7').trim();
    const BLING_CLIENT_SECRET = (process.env.BLING_CLIENT_SECRET || 'ebdf4f1c63020852537cef1e4bdd117175fe104b72a3ed3d9ac7aa66bb83').trim();

    try {
      const authHeader = 'Basic ' + Buffer.from(`${BLING_CLIENT_ID}:${BLING_CLIENT_SECRET}`).toString('base64');
      const tokenRes = await fetch(BLING_TOKEN_URL, {
        method: 'POST',
        headers: { 
          'Content-Type': 'application/x-www-form-urlencoded',
          'Authorization': authHeader,
          'Accept': '1.0'
        },
        body: new URLSearchParams({
          grant_type: 'authorization_code',
          code,
          redirect_uri: BLING_REDIRECT_URI,
        }).toString(),
      });
      
      const tokenData = await tokenRes.json();
      if (!tokenRes.ok) return res.status(400).json({
        error: tokenData.error_description || tokenData.error || 'Erro ao trocar código Bling',
        _debug: { status: tokenRes.status, body: tokenData }
      });

      const apiToken = tokenData.access_token;
      const nick = state; // O 'state' enviado serve como o apelido da conta
      const expAt = new Date(Date.now() + (tokenData.expires_in || 1800) * 1000).toISOString();

      // Salva no banco de dados central 'bling' na tabela 'clientes'
      await centralPool.query(`
        INSERT INTO clientes (nome, empresa, client_id, client_secret, access_token, refresh_token, expires_at, atualizado_em)
        VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
        ON CONFLICT (empresa, nome) DO UPDATE SET
          client_id = EXCLUDED.client_id,
          client_secret = EXCLUDED.client_secret,
          access_token = EXCLUDED.access_token,
          refresh_token = EXCLUDED.refresh_token,
          expires_at = EXCLUDED.expires_at,
          atualizado_em = NOW()
      `, [nick, company, BLING_CLIENT_ID, BLING_CLIENT_SECRET, apiToken, tokenData.refresh_token, expAt]);

      const safeName = nick.replace(/[^a-z0-9_]/gi, '_').toLowerCase();

      // Cria a tabela de estoque no banco da empresa correspondente (ex: Autoequip)
      await companyPool.query(`CREATE TABLE IF NOT EXISTS bd_estoque_bling_${safeName} (
        id_bling        TEXT PRIMARY KEY,
        sku             TEXT,
        nome            TEXT,
        estoque_atual   NUMERIC DEFAULT 0,
        estoque_virtual NUMERIC DEFAULT 0,
        atualizado_em   TIMESTAMP DEFAULT NOW()
      )`);

      return res.json({ ok: true, nick, modulos: (modules || 'estoque').split(',') });
    } catch(e) { return res.status(500).json({ error: e.message }); }
  }

  // 2. Módulo Bling Sync — Sincronizar o estoque utilizando dados do banco central bling
  if (req.query.module === 'bling-sync') {
    const account = req.query.account;
    if (!account) return res.status(400).json({ error: 'account é obrigatório. Ex: ?account=cliente_1' });

    const { company, pool: companyPool } = getCompanyPool(payload);
    const BLING_API_BASE = 'https://api.bling.com.br/Api/v3';
    const BLING_TOKEN_URL = 'https://api.bling.com.br/Api/v3/oauth/token';

    try {
      // Busca a credencial da tabela 'clientes' do banco central 'bling'
      const clientRes = await centralPool.query(
        `SELECT * FROM clientes WHERE empresa = $1 AND nome = $2`,
        [company, account]
      );

      if (clientRes.rows.length === 0) {
        return res.status(404).json({ error: `Cliente '${account}' não encontrado no banco central do Bling.` });
      }

      const clientData = clientRes.rows[0];
      let accessToken = clientData.access_token;
      const refreshToken = clientData.refresh_token;

      const BLING_CLIENT_ID = (clientData.client_id || process.env.BLING_CLIENT_ID || '7d24d3e4ab13c4e803b0441f52170ddc261395b7').trim();
      const BLING_CLIENT_SECRET = (clientData.client_secret || process.env.BLING_CLIENT_SECRET || 'ebdf4f1c63020852537cef1e4bdd117175fe104b72a3ed3d9ac7aa66bb83').trim();

      const expiresAt = clientData.expires_at ? new Date(clientData.expires_at) : null;
      const isExpired = expiresAt ? (expiresAt.getTime() - 60000 < Date.now()) : true;

      // Renova o token caso expirado
      if (isExpired && refreshToken) {
        console.log(`[bling-sync] Token expirado. Renovando no banco central para ${account}...`);
        const authHeader = 'Basic ' + Buffer.from(`${BLING_CLIENT_ID}:${BLING_CLIENT_SECRET}`).toString('base64');
        const rr = await fetch(BLING_TOKEN_URL, {
          method: 'POST',
          headers: { 
            'Content-Type': 'application/x-www-form-urlencoded',
            'Authorization': authHeader,
            'Accept': '1.0'
          },
          body: new URLSearchParams({ 
            grant_type: 'refresh_token', 
            refresh_token: refreshToken 
          }).toString(),
        });

        if (rr.ok) {
          const nt = await rr.json();
          accessToken = nt.access_token;
          const newExp = new Date(Date.now() + (nt.expires_in || 1800) * 1000).toISOString();
          
          // Atualiza no banco central
          await centralPool.query(`
            UPDATE clientes SET
              access_token = $1,
              refresh_token = $2,
              expires_at = $3,
              atualizado_em = NOW()
            WHERE id = $4
          `, [accessToken, nt.refresh_token || refreshToken, newExp, clientData.id]);
          console.log(`[bling-sync] Token renovado e salvo no banco central para ${account}.`);
        } else {
          const rrError = await rr.text();
          console.error(`[bling-sync] Falha ao renovar token central de ${account}:`, rrError);
          return res.status(400).json({ error: `Erro ao renovar token Bling no banco central: ${rrError}` });
        }
      }

      const safeName = account.replace(/[^a-z0-9_]/gi, '_').toLowerCase();

      // Mapeia todos os produtos do cadastro Bling (ID -> SKU/Código, Nome)
      const productsMap = {};
      let pageProd = 1;
      const limitProd = 100;
      
      while (true) {
        const url = `${BLING_API_BASE}/produtos?pagina=${pageProd}&limite=${limitProd}`;
        const pr = await fetch(url, { headers: { 'Authorization': 'Bearer ' + accessToken } });
        if (pr.status === 429) { await sleep(5000); continue; }
        if (!pr.ok) break;

        const pd = await pr.json();
        const pg = pd.data || [];
        for (const item of pg) {
          productsMap[item.id] = { codigo: item.codigo || 'Sem SKU', nome: item.nome || 'Sem Nome' };
        }
        if (pg.length < limitProd) break;
        pageProd++;
        await sleep(400);
      }

      // Busca os saldos de estoque
      const stockItems = [];
      let pageStock = 1;
      const limitStock = 100;

      while (true) {
        const url = `${BLING_API_BASE}/estoques/saldos?pagina=${pageStock}&limite=${limitStock}`;
        const sr = await fetch(url, { headers: { 'Authorization': 'Bearer ' + accessToken } });
        if (sr.status === 429) { await sleep(5000); continue; }
        if (!sr.ok) break;

        const sd = await sr.json();
        const pg = sd.data || [];
        for (const item of pg) {
          const prodId = item.produto.id;
          const pInfo = productsMap[prodId] || { codigo: 'Desconhecido', nome: 'Produto não mapeado' };
          stockItems.push({
            id_bling: String(prodId),
            sku: pInfo.codigo,
            nome: pInfo.nome,
            estoque_atual: parseFloat(item.saldoFisicoTotal || 0),
            estoque_virtual: parseFloat(item.saldoVirtualTotal || 0)
          });
        }
        if (pg.length < limitStock) break;
        pageStock++;
        await sleep(400);
      }

      // Salva no banco de dados da empresa (ex: Autoequip)
      if (stockItems.length > 0) {
        await companyPool.query(`CREATE TABLE IF NOT EXISTS bd_estoque_bling_${safeName} (
          id_bling        TEXT PRIMARY KEY,
          sku             TEXT,
          nome            TEXT,
          estoque_atual   NUMERIC DEFAULT 0,
          estoque_virtual NUMERIC DEFAULT 0,
          atualizado_em   TIMESTAMP DEFAULT NOW()
        )`);

        const rows = stockItems.map(s => [
          s.id_bling, s.sku, s.nome, s.estoque_atual, s.estoque_virtual
        ]);

        await companyPool.query(`
          INSERT INTO bd_estoque_bling_${safeName} (id_bling, sku, nome, estoque_atual, estoque_virtual, atualizado_em)
          SELECT * FROM UNNEST(
            $1::text[], $2::text[], $3::text[], $4::numeric[], $5::numeric[],
            (SELECT array_agg(NOW()) FROM generate_series(1, $6))
          )
          ON CONFLICT (id_bling) DO UPDATE SET
            sku = EXCLUDED.sku,
            nome = EXCLUDED.nome,
            estoque_atual = EXCLUDED.estoque_atual,
            estoque_virtual = EXCLUDED.estoque_virtual,
            atualizado_em = NOW()
        `, [
          rows.map(r => r[0]), rows.map(r => r[1]), rows.map(r => r[2]), rows.map(r => r[3]), rows.map(r => r[4]),
          rows.length
        ]);
      }

      // Atualiza a data de sincronização no banco central
      await centralPool.query(
        `UPDATE clientes SET last_sync = NOW() WHERE id = $1`,
        [clientData.id]
      );

      return res.json({ 
        ok: true, 
        account, 
        synced_at: new Date().toISOString(), 
        results: { estoque: stockItems.length } 
      });
    } catch(e) { 
      console.error(`[bling-sync] Erro na sincronização da conta ${account}:`, e.message);
      return res.status(500).json({ error: e.message }); 
    }
  }

  // 3. Módulo Cron Bling — Sincronizar todos os clientes cadastrados no banco central bling
  if (req.query.module === 'cron-bling') {
    try {
      const { company, pool: companyPool } = getCompanyPool(payload);
      
      // Busca todas as contas Bling cadastradas para esta empresa no banco central
      const accountsRes = await centralPool.query(
        `SELECT * FROM clientes WHERE empresa = $1`,
        [company]
      );

      const log = [];
      const BLING_API_BASE = 'https://api.bling.com.br/Api/v3';
      const BLING_TOKEN_URL = 'https://api.bling.com.br/Api/v3/oauth/token';

      for (const clientData of accountsRes.rows) {
        const account = clientData.nome;
        let accessToken = clientData.access_token;
        const refreshToken = clientData.refresh_token;

        const BLING_CLIENT_ID = (clientData.client_id || process.env.BLING_CLIENT_ID || '7d24d3e4ab13c4e803b0441f52170ddc261395b7').trim();
        const BLING_CLIENT_SECRET = (clientData.client_secret || process.env.BLING_CLIENT_SECRET || 'ebdf4f1c63020852537cef1e4bdd117175fe104b72a3ed3d9ac7aa66bb83').trim();

        const expiresAt = clientData.expires_at ? new Date(clientData.expires_at) : null;
        const isExpired = expiresAt ? (expiresAt.getTime() - 60000 < Date.now()) : true;

        if (isExpired && refreshToken) {
          const authHeader = 'Basic ' + Buffer.from(`${BLING_CLIENT_ID}:${BLING_CLIENT_SECRET}`).toString('base64');
          const tr = await fetch(BLING_TOKEN_URL, {
            method: 'POST', 
            headers: { 
              'Content-Type': 'application/x-www-form-urlencoded',
              'Authorization': authHeader,
              'Accept': '1.0'
            },
            body: new URLSearchParams({ 
              grant_type: 'refresh_token', 
              refresh_token: refreshToken 
            }),
          });
          
          if (tr.ok) {
            const nt = await tr.json();
            accessToken = nt.access_token;
            const newExp = new Date(Date.now() + (nt.expires_in || 1800) * 1000).toISOString();
            
            await centralPool.query(`
              UPDATE clientes SET
                access_token = $1,
                refresh_token = $2,
                expires_at = $3,
                atualizado_em = NOW()
              WHERE id = $4
            `, [accessToken, nt.refresh_token || refreshToken, newExp, clientData.id]);
          } else {
            console.error(`[cron-bling] Falha ao renovar token de ${account}`);
            continue;
          }
        }

        const safeName = account.replace(/[^a-z0-9_]/gi, '_').toLowerCase();

        // 1. Busca produtos
        const productsMap = {};
        let pageProd = 1;
        while (true) {
          const url = `${BLING_API_BASE}/produtos?pagina=${pageProd}&limite=100`;
          const pr = await fetch(url, { headers: { 'Authorization': 'Bearer ' + accessToken } });
          if (!pr.ok) break;
          const pd = await pr.json();
          const pg = pd.data || [];
          for (const item of pg) {
            productsMap[item.id] = { codigo: item.codigo || 'Sem SKU', nome: item.nome || 'Sem Nome' };
          }
          if (pg.length < 100) break;
          pageProd++;
          await sleep(400);
        }

        // 2. Busca estoques
        const stockItems = [];
        let pageStock = 1;
        while (true) {
          const url = `${BLING_API_BASE}/estoques/saldos?pagina=${pageStock}&limite=100`;
          const sr = await fetch(url, { headers: { 'Authorization': 'Bearer ' + accessToken } });
          if (!sr.ok) break;
          const sd = await sr.json();
          const pg = sd.data || [];
          for (const item of pg) {
            const prodId = item.produto.id;
            const pInfo = productsMap[prodId] || { codigo: 'Desconhecido', nome: 'Produto não mapeado' };
            stockItems.push({
              id_bling: String(prodId),
              sku: pInfo.codigo,
              nome: pInfo.nome,
              estoque_atual: parseFloat(item.saldoFisicoTotal || 0),
              estoque_virtual: parseFloat(item.saldoVirtualTotal || 0)
            });
          }
          if (pg.length < 100) break;
          pageStock++;
          await sleep(400);
        }

        // 3. Salva no banco da empresa
        if (stockItems.length > 0) {
          await companyPool.query(`CREATE TABLE IF NOT EXISTS bd_estoque_bling_${safeName} (
            id_bling        TEXT PRIMARY KEY,
            sku             TEXT,
            nome            TEXT,
            estoque_atual   NUMERIC DEFAULT 0,
            estoque_virtual NUMERIC DEFAULT 0,
            atualizado_em   TIMESTAMP DEFAULT NOW()
          )`);

          const rows = stockItems.map(s => [
            s.id_bling, s.sku, s.nome, s.estoque_atual, s.estoque_virtual
          ]);

          await companyPool.query(`
            INSERT INTO bd_estoque_bling_${safeName} (id_bling, sku, nome, estoque_atual, estoque_virtual, atualizado_em)
            SELECT * FROM UNNEST(
              $1::text[], $2::text[], $3::text[], $4::numeric[], $5::numeric[],
              (SELECT array_agg(NOW()) FROM generate_series(1, $6))
            )
            ON CONFLICT (id_bling) DO UPDATE SET
              sku = EXCLUDED.sku,
              nome = EXCLUDED.nome,
              estoque_atual = EXCLUDED.estoque_atual,
              estoque_virtual = EXCLUDED.estoque_virtual,
              atualizado_em = NOW()
          `, [
            rows.map(r => r[0]), rows.map(r => r[1]), rows.map(r => r[2]), rows.map(r => r[3]), rows.map(r => r[4]),
            rows.length
          ]);
        }

        // Atualiza last_sync no banco central
        await centralPool.query(
          `UPDATE clientes SET last_sync = NOW() WHERE id = $1`,
          [clientData.id]
        );
        log.push({ account, synced: stockItems.length });
      }

      return res.json({ ok: true, ran_at: new Date().toISOString(), log });
    } catch(e) {
      return res.status(500).json({ error: e.message });
    }
  }

  // 4. Módulo Bling Accounts — Retorna a lista de contas integradas no banco central bling
  if (req.query.module === 'bling-accounts') {
    try {
      const { company } = getCompanyPool(payload);
      
      const accountsRes = await centralPool.query(
        `SELECT nome, client_id, access_token, expires_at, last_sync FROM clientes WHERE empresa = $1`,
        [company]
      );

      const result = accountsRes.rows.map(row => {
        const expStr = row.expires_at;
        let status = 'active';
        if (expStr) {
          const expDate = new Date(expStr);
          if (expDate < new Date()) status = 'expired';
        }
        return {
          id: row.nome,
          nick: row.nome,
          status: status,
          expires_at: expStr || null,
          last_sync: row.last_sync || null
        };
      });

      return res.json({ ok: true, accounts: result });
    } catch(e) {
      return res.status(500).json({ error: e.message });
    }
  }

  return res.status(400).json({ error: 'Módulo Bling não identificado ou inválido' });
};
