const { getPool, getCompanyPool } = require('./db');
const { parseBody, upsertConfig } = require('./data-helpers');

module.exports = async function handleTiny(req, res, payload) {

  // Módulo Tiny OAuth — conectar conta Tiny ERP v3
  if (req.query.module === 'tiny-oauth') {
    if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });
    const _tBody = parseBody(req.body);
    const { code, state, modules } = _tBody;
    if (!code || !state) return res.status(400).json({ error: 'code e state são obrigatórios' });

    const TINY_REDIRECT_URI  = 'https://have-gestor-frontend.vercel.app/tiny-callback';
    const TINY_TOKEN_URL = 'https://accounts.tiny.com.br/realms/tiny/protocol/openid-connect/token';
    const TINY_API_BASE  = 'https://erp.tiny.com.br/public-api/v3';
    const { company, pool } = getCompanyPool(payload);
    const _creds = await pool.query(`SELECT chave, valor FROM configuracoes WHERE empresa=$1 AND chave IN ('tiny_client_id','tiny_client_secret')`, [company]);
    const _credMap = {}; _creds.rows.forEach(r => { _credMap[r.chave] = r.valor; });
    const TINY_CLIENT_ID     = (_credMap.tiny_client_id     || process.env.TINY_CLIENT_ID     || '').trim();
    const TINY_CLIENT_SECRET = (_credMap.tiny_client_secret || process.env.TINY_CLIENT_SECRET || '').trim();

    if (!TINY_CLIENT_ID) return res.status(500).json({ error: 'TINY_CLIENT_ID não configurado no servidor' });

    try {
      const tokenRes = await fetch(TINY_TOKEN_URL, {
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        body: new URLSearchParams({
          grant_type: 'authorization_code',
          client_id: TINY_CLIENT_ID,
          client_secret: TINY_CLIENT_SECRET,
          code,
          redirect_uri: TINY_REDIRECT_URI,
        }).toString(),
      });
      const tokenData = await tokenRes.json();
      if (!tokenRes.ok) return res.status(400).json({
        error: tokenData.error_description || tokenData.error || 'Erro ao trocar código Tiny',
        _debug: { status: tokenRes.status, body: tokenData, client_id_len: TINY_CLIENT_ID.length, redirect_uri: TINY_REDIRECT_URI }
      });

      const apiToken = tokenData.access_token;

      let nick = state;
      try {
        const uRes = await fetch(`${TINY_API_BASE}/empresas`, {
          headers: { 'Authorization': 'Bearer ' + apiToken },
        });
        const uData = await uRes.json();
        const emp = (uData.itens || uData.data || [])[0];
        if (emp) nick = emp.nomeFantasia || emp.razaoSocial || emp.nome || state;
      } catch {}

      const expAt   = new Date(Date.now() + (tokenData.expires_in || 21600) * 1000).toISOString();
      const modsStr = modules || 'vendas,estoque,pedidos';
      for (const [k, v] of [
        [`${state}_token`,   apiToken],
        [`${state}_refresh`, tokenData.refresh_token],
        [`${state}_nick`,    nick],
        [`${state}_exp`,     expAt],
        [`${state}_modulos`, modsStr],
      ]) {
        await upsertConfig(pool, company, k, v);
      }

      const mods = modsStr.split(',').map(m => m.trim());
      const safeName = state.replace(/[^a-z0-9_]/gi, '_').toLowerCase();

      if (mods.includes('vendas')) {
        await pool.query(`CREATE TABLE IF NOT EXISTS bd_vendas_tiny_${safeName} (
          numero_pedido      TEXT NOT NULL,
          numero_ecommerce   TEXT,
          data_pedido        DATE,
          situacao           TEXT,
          canal_venda        TEXT,
          plataforma         TEXT,
          cliente_nome       TEXT,
          cliente_cpf_cnpj   TEXT,
          cliente_uf         TEXT,
          sku                TEXT NOT NULL,
          nome_produto       TEXT,
          quantidade         NUMERIC,
          preco_unitario     NUMERIC,
          preco_custo        NUMERIC,
          preco_final        NUMERIC,
          desconto_item      NUMERIC,
          total_produtos     NUMERIC,
          valor_frete        NUMERIC,
          valor_desconto     NUMERIC,
          total_pedido       NUMERIC,
          forma_pagamento    TEXT,
          numero_parcelas    INT,
          transportadora     TEXT,
          codigo_rastreamento TEXT,
          atualizado_em      TIMESTAMP DEFAULT NOW(),
          PRIMARY KEY (numero_pedido, sku)
        )`);
      }

      if (mods.includes('estoque')) {
        await pool.query(`CREATE TABLE IF NOT EXISTS bd_estoque_tiny_${safeName} (
          sku            TEXT PRIMARY KEY,
          nome           TEXT,
          unidade        TEXT,
          estoque_atual  NUMERIC DEFAULT 0,
          estoque_minimo NUMERIC DEFAULT 0,
          preco_custo    NUMERIC DEFAULT 0,
          preco_venda    NUMERIC DEFAULT 0,
          marca          TEXT,
          categoria      TEXT,
          atualizado_em  TIMESTAMP DEFAULT NOW()
        )`);
      }

      if (mods.includes('pedidos')) {
        await pool.query(`CREATE TABLE IF NOT EXISTS po_tiny_${safeName} (
          numero_pedido   TEXT NOT NULL,
          sku             TEXT NOT NULL,
          fornecedor      TEXT,
          data_pedido     DATE,
          data_prevista   DATE,
          situacao        TEXT,
          nome_produto    TEXT,
          quantidade      NUMERIC,
          preco_unitario  NUMERIC,
          total_pedido    NUMERIC,
          atualizado_em   TIMESTAMP DEFAULT NOW(),
          PRIMARY KEY (numero_pedido, sku)
        )`);
      }

      return res.json({ ok: true, nick, modulos: mods });
    } catch(e) { return res.status(500).json({ error: e.message }); }
  }

  // Módulo Tiny Debug — inspeciona resposta bruta da Tiny API
  if (req.query.module === 'tiny-debug') {
    const account = req.query.account;
    if (!account) return res.status(400).json({ error: 'account obrigatório' });
    const { company, pool } = getCompanyPool(payload);
    try {
      const cfgRes = await pool.query(`SELECT chave, valor FROM configuracoes WHERE empresa=$1 AND chave LIKE $2`, [company, account + '%']);
      const cfg = {};
      cfgRes.rows.forEach(({ chave, valor }) => { cfg[chave] = valor; });
      let accessToken = cfg[account + '_token'];
      const refreshToken = cfg[account + '_refresh'];
      if (!accessToken) return res.status(400).json({ error: 'Conta não autenticada' });

      const TINY_API       = 'https://erp.tiny.com.br/public-api/v3';
      const TINY_TOKEN_URL = 'https://accounts.tiny.com.br/realms/tiny/protocol/openid-connect/token';
      const _dbCreds = await pool.query(`SELECT chave, valor FROM configuracoes WHERE empresa=$1 AND chave IN ('tiny_client_id','tiny_client_secret')`, [company]);
      const _credMap2 = {}; _dbCreds.rows.forEach(r => { _credMap2[r.chave] = r.valor; });
      const TINY_CLIENT_ID     = (_credMap2.tiny_client_id     || process.env.TINY_CLIENT_ID     || '').trim();
      const TINY_CLIENT_SECRET = (_credMap2.tiny_client_secret || process.env.TINY_CLIENT_SECRET || '').trim();

      let refreshResult = null;
      if (refreshToken && TINY_CLIENT_ID) {
        const rr = await fetch(TINY_TOKEN_URL, {
          method: 'POST',
          headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
          body: new URLSearchParams({ grant_type: 'refresh_token', client_id: TINY_CLIENT_ID, client_secret: TINY_CLIENT_SECRET, refresh_token: refreshToken }).toString(),
        });
        const rrData = await rr.json().catch(() => ({}));
        refreshResult = { status: rr.status, expires_in: rrData.expires_in, ok: rr.ok };
        if (rr.ok && rrData.access_token) {
          accessToken = rrData.access_token;
          const newExp = new Date(Date.now() + (rrData.expires_in || 300) * 1000).toISOString();
          for (const [k, v] of [[account+'_token', rrData.access_token],[account+'_refresh', rrData.refresh_token||refreshToken],[account+'_exp', newExp]]) {
            await upsertConfig(pool, company, k, v);
          }
        }
      }

      const dataFinal   = new Date().toISOString().split('T')[0];
      const dataInicial = new Date(Date.now() - 7 * 86400000).toISOString().split('T')[0];
      async function tinyFetch(url, tok) {
        const r = await fetch(url, { headers: { 'Authorization': 'Bearer ' + (tok || accessToken) } });
        const text = await r.text();
        let body; try { body = JSON.parse(text); } catch { body = text.substring(0, 300); }
        return { status: r.status, body };
      }
      let tokenClaims = null;
      try {
        const parts = accessToken.split('.');
        if (parts.length === 3) tokenClaims = JSON.parse(Buffer.from(parts[1], 'base64').toString('utf8'));
      } catch {}

      let ccToken = null, ccResult = null;
      if (TINY_CLIENT_ID && TINY_CLIENT_SECRET) {
        const ccRes = await fetch(TINY_TOKEN_URL, {
          method: 'POST',
          headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
          body: new URLSearchParams({ grant_type: 'client_credentials', client_id: TINY_CLIENT_ID, client_secret: TINY_CLIENT_SECRET }).toString(),
        });
        const ccData = await ccRes.json().catch(() => ({}));
        ccResult = { status: ccRes.status, ok: ccRes.ok, scope: ccData.scope, expires_in: ccData.expires_in };
        if (ccRes.ok && ccData.access_token) ccToken = ccData.access_token;
      }

      const ccFetch = ccToken ? await tinyFetch(`${TINY_API}/conta/info`, ccToken) : null;

      async function tinyFetchFull(url, tok, scheme) {
        const r = await fetch(url, { headers: { 'Authorization': (scheme||'Bearer') + ' ' + (tok||accessToken) } });
        const text = await r.text();
        let body; try { body = JSON.parse(text); } catch { body = text.substring(0, 200) || null; }
        const wwwAuth = r.headers.get('www-authenticate');
        return { status: r.status, body, wwwAuth };
      }

      let dbCounts = {};
      try {
        const safeName = account.replace(/[^a-z0-9_]/gi,'_').toLowerCase();
        const [cp, ce] = await Promise.all([
          pool.query(`SELECT COUNT(*) AS total, MIN(data_criacao) AS mais_antigo, MAX(data_criacao) AS mais_recente FROM bd_pedidos_tiny_${safeName}`).catch(()=>null),
          pool.query(`SELECT COUNT(*) AS total FROM bd_estoque_tiny_${safeName}`).catch(()=>null),
        ]);
        dbCounts = {
          pedidos: cp?.rows[0] || null,
          estoque: ce?.rows[0] || null,
        };
      } catch {}

      return res.json({
        token_roles_count: tokenClaims?.roles?.['tiny-api']?.length || 0,
        token_email_verified: tokenClaims?.email_verified,
        refresh: refreshResult,
        db: dbCounts,
        endpoints: {
          pedidos: await tinyFetchFull(`${TINY_API}/pedidos?dataInicial=${dataInicial}&dataFinal=${dataFinal}&pagina=1&limite=1`),
        },
        spa_discovery: await (async () => {
          try {
            const htmlRes = await fetch('https://erp.olist.com/margem_contribuicao', {
              headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124', Accept: 'text/html' },
              redirect: 'follow',
            });
            const html = await htmlRes.text();
            const scriptSrcs = [...html.matchAll(/src=["']([^"']+\.js[^"']*?)["']/g)].map(m => m[1]);
            const appScripts = scriptSrcs.filter(s => !s.includes('gtm') && !s.includes('analytics') && !s.includes('hotjar'));
            const findings = [];
            for (const src of appScripts.slice(0, 5)) {
              const fullUrl = src.startsWith('http') ? src : 'https://erp.olist.com' + src;
              const jsRes = await fetch(fullUrl, { headers: { 'User-Agent': 'Mozilla/5.0' } }).catch(() => null);
              if (!jsRes?.ok) continue;
              const js = await jsRes.text();
              const patterns = [...js.matchAll(/["'](\/[a-z0-9/_-]*(?:services|margem|api|contribuicao)[a-z0-9/_.-]*(?:php|\?)[^"']{0,80})["']/gi)];
              if (patterns.length > 0) findings.push({ script: src, urls: patterns.slice(0,10).map(m=>m[1]) });
            }
            return { html_size: html.length, scripts_found: scriptSrcs.length, app_scripts: appScripts.slice(0,8), findings };
          } catch(e) { return { error: e.message }; }
        })(),
        webapp_session_test: await (async () => {
          const kTokenUrl = 'https://accounts.tiny.com.br/realms/tiny/protocol/openid-connect/token';
          const results = {};
          const uiRes = await fetch('https://accounts.tiny.com.br/realms/tiny/protocol/openid-connect/userinfo', {
            headers: { Authorization: 'Bearer ' + accessToken }
          }).catch(() => null);
          results.keycloak_userinfo = uiRes ? { status: uiRes.status, body: await uiRes.json().catch(()=>null) } : null;
          const exchRes = await fetch(kTokenUrl, {
            method: 'POST',
            headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
            body: new URLSearchParams({
              grant_type: 'urn:ietf:params:oauth:grant-type:token-exchange',
              client_id: 'tiny-webapp',
              subject_token: accessToken,
              subject_token_type: 'urn:ietf:params:oauth:token-type:access_token',
              requested_token_type: 'urn:ietf:params:oauth:token-type:access_token',
            }).toString(),
          }).catch(()=>null);
          if (exchRes) {
            const exchData = await exchRes.json().catch(()=>{});
            results.token_exchange = { status: exchRes.status, data: exchData };
            if (exchRes.ok && exchData.access_token) {
              const pingRes = await fetch('https://erp.olist.com/services/auth.services.php?a=ping', {
                headers: { Authorization: 'Bearer ' + exchData.access_token, 'X-Requested-With': 'XMLHttpRequest' }
              }).catch(()=>null);
              if (pingRes) {
                const pingBody = await pingRes.text();
                results.php_ping_with_webapp_token = { status: pingRes.status, preview: pingBody.substring(0,200) };
              }
            }
          }
          const rfrRes = await fetch(kTokenUrl, {
            method: 'POST',
            headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
            body: new URLSearchParams({
              grant_type: 'refresh_token',
              client_id: 'tiny-webapp',
              refresh_token: refreshToken,
            }).toString(),
          }).catch(()=>null);
          results.webapp_refresh = rfrRes ? { status: rfrRes.status, body: await rfrRes.json().catch(()=>null) } : null;
          return results;
        })(),
      });
    } catch(e) { return res.status(500).json({ error: e.message }); }
  }

  // Módulo Tiny Sync — sincroniza pedidos e estoque via API v3 (OAuth Bearer)
  if (req.query.module === 'tiny-sync') {
    const account = req.query.account;
    if (!account) return res.status(400).json({ error: 'account é obrigatório. Ex: ?account=tiny_marcon' });

    const { company, pool } = getCompanyPool(payload);
    const TINY_API       = 'https://erp.tiny.com.br/public-api/v3';
    const TINY_TOKEN_URL = 'https://accounts.tiny.com.br/realms/tiny/protocol/openid-connect/token';

    try {
      const cfgRes = await pool.query(
        `SELECT chave, valor FROM configuracoes WHERE empresa=$1 AND (chave LIKE $2 OR chave IN ('tiny_client_id','tiny_client_secret'))`,
        [company, account + '%']
      );
      const cfg = {};
      cfgRes.rows.forEach(({ chave, valor }) => { cfg[chave] = valor; });

      let accessToken    = cfg[account + '_token'];
      const refreshToken = cfg[account + '_refresh'];
      const modulos      = (cfg[account + '_modulos'] || 'vendas,estoque').split(',').map(m => m.trim());
      const TINY_CLIENT_ID     = (cfg.tiny_client_id     || process.env.TINY_CLIENT_ID     || '').trim();
      const TINY_CLIENT_SECRET = (cfg.tiny_client_secret || process.env.TINY_CLIENT_SECRET || '').trim();

      if (!accessToken) return res.status(400).json({ error: `Conta ${account} não autenticada.` });

      if (refreshToken && TINY_CLIENT_ID) {
        const rr = await fetch(TINY_TOKEN_URL, {
          method: 'POST',
          headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
          body: new URLSearchParams({ grant_type: 'refresh_token', client_id: TINY_CLIENT_ID, client_secret: TINY_CLIENT_SECRET, refresh_token: refreshToken }).toString(),
        });
        if (rr.ok) {
          const nt = await rr.json();
          accessToken = nt.access_token;
          const newExp = new Date(Date.now() + (nt.expires_in || 300) * 1000).toISOString();
          for (const [k, v] of [[account+'_token', nt.access_token],[account+'_refresh', nt.refresh_token||refreshToken],[account+'_exp', newExp]]) {
            await upsertConfig(pool, company, k, v);
          }
        }
      }

      const safeName = account.replace(/[^a-z0-9_]/gi, '_').toLowerCase();
      const results  = {};

      async function tinyPages(endpoint, params = {}) {
        const items = [];
        let offset = 0;
        const limit = 100;
        while (true) {
          const qs = new URLSearchParams({ ...params, limit: String(limit), offset: String(offset) }).toString();
          const r  = await fetch(`${TINY_API}${endpoint}?${qs}`, { headers: { 'Authorization': 'Bearer ' + accessToken } });
          if (!r.ok) { console.error('[tiny-sync] ' + endpoint + ' offset' + offset + ' → ' + r.status); break; }
          const d   = await r.json();
          const pg  = d.itens || d.data || [];
          items.push(...pg);
          const total = d.paginacao?.total ?? d.total ?? 0;
          offset += pg.length;
          if (pg.length === 0 || offset >= total) break;
        }
        return items;
      }

      if (modulos.includes('vendas')) {
        const lastSync = cfg[account + '_token_sync'];
        const forceReset = req.query.force === 'true';
        if (!lastSync || forceReset) await pool.query(`DROP TABLE IF EXISTS bd_pedidos_tiny_${safeName}`);
        await pool.query(`CREATE TABLE IF NOT EXISTS bd_pedidos_tiny_${safeName} (
          id_tiny TEXT PRIMARY KEY,
          numero TEXT, numero_ecommerce TEXT, numero_canal_venda TEXT,
          data_criacao DATE, data_previsao DATE,
          situacao INT,
          nome_cliente TEXT, cpf_cnpj TEXT, tipo_pessoa TEXT,
          email_cliente TEXT, telefone_cliente TEXT, celular_cliente TEXT, codigo_cliente TEXT,
          uf TEXT, municipio TEXT, bairro TEXT, cep TEXT, endereco TEXT,
          total_pedido NUMERIC DEFAULT 0,
          marketplace TEXT, canal_venda TEXT,
          transportadora TEXT, forma_envio TEXT, frete_por_conta TEXT,
          codigo_rastreamento TEXT, url_rastreamento TEXT,
          vendedor TEXT, origem_pedido INT,
          atualizado_em TIMESTAMP DEFAULT NOW()
        )`);
        const dataFinal   = req.query.to   || new Date().toISOString().split('T')[0];
        const daysParam   = parseInt(req.query.days || '0');
        const days        = daysParam > 0 ? daysParam : (lastSync && !forceReset ? 2 : 30);
        const dataInicial = req.query.from || new Date(Date.now() - days * 86400000).toISOString().split('T')[0];
        const pedidos = await tinyPages('/pedidos', { dataInicial, dataFinal });
        if (pedidos.length > 0) {
          const rows = pedidos.map(p => [
            String(p.id||''),
            p.numeroPedido ? String(p.numeroPedido) : null,
            p.ecommerce?.numeroPedidoEcommerce||null,
            p.ecommerce?.numeroPedidoCanalVenda||null,
            p.dataCriacao ? p.dataCriacao.split('T')[0] : null,
            p.dataPrevisao ? p.dataPrevisao.split('T')[0] : null,
            typeof p.situacao === 'number' ? p.situacao : null,
            p.cliente?.nome||null,
            p.cliente?.cpfCnpj||null,
            p.cliente?.tipoPessoa||null,
            p.cliente?.email||null,
            p.cliente?.telefone||null,
            p.cliente?.celular||null,
            p.cliente?.codigo ? String(p.cliente.codigo) : null,
            p.cliente?.endereco?.uf||null,
            p.cliente?.endereco?.municipio||null,
            p.cliente?.endereco?.bairro||null,
            p.cliente?.endereco?.cep||null,
            p.cliente?.endereco?.endereco||null,
            parseFloat(p.valor||0)||0,
            p.ecommerce?.nome||null,
            p.ecommerce?.canalVenda||null,
            p.transportador?.nome||null,
            p.transportador?.formaEnvio?.nome||null,
            p.transportador?.fretePorConta||null,
            p.transportador?.codigoRastreamento||null,
            p.transportador?.urlRastreamento||null,
            p.vendedor?.nome||null,
            typeof p.origemPedido === 'number' ? p.origemPedido : null,
          ]);
          await pool.query(`
            INSERT INTO bd_pedidos_tiny_${safeName}
              (id_tiny,numero,numero_ecommerce,numero_canal_venda,
               data_criacao,data_previsao,situacao,
               nome_cliente,cpf_cnpj,tipo_pessoa,email_cliente,telefone_cliente,celular_cliente,codigo_cliente,
               uf,municipio,bairro,cep,endereco,
               total_pedido,marketplace,canal_venda,
               transportadora,forma_envio,frete_por_conta,codigo_rastreamento,url_rastreamento,
               vendedor,origem_pedido,atualizado_em)
            SELECT * FROM UNNEST(
              $1::text[],$2::text[],$3::text[],$4::text[],
              $5::date[],$6::date[],$7::int[],
              $8::text[],$9::text[],$10::text[],$11::text[],$12::text[],$13::text[],$14::text[],
              $15::text[],$16::text[],$17::text[],$18::text[],$19::text[],
              $20::numeric[],$21::text[],$22::text[],
              $23::text[],$24::text[],$25::text[],$26::text[],$27::text[],
              $28::text[],$29::int[],
              (SELECT array_agg(NOW()) FROM generate_series(1,$30))
            )
            ON CONFLICT (id_tiny) DO UPDATE SET
              situacao=EXCLUDED.situacao, total_pedido=EXCLUDED.total_pedido,
              data_previsao=EXCLUDED.data_previsao, marketplace=EXCLUDED.marketplace,
              codigo_rastreamento=EXCLUDED.codigo_rastreamento, url_rastreamento=EXCLUDED.url_rastreamento,
              forma_envio=EXCLUDED.forma_envio, vendedor=EXCLUDED.vendedor, atualizado_em=NOW()
          `, [
            rows.map(r=>r[0]),  rows.map(r=>r[1]),  rows.map(r=>r[2]),  rows.map(r=>r[3]),
            rows.map(r=>r[4]),  rows.map(r=>r[5]),  rows.map(r=>r[6]),
            rows.map(r=>r[7]),  rows.map(r=>r[8]),  rows.map(r=>r[9]),  rows.map(r=>r[10]),
            rows.map(r=>r[11]), rows.map(r=>r[12]), rows.map(r=>r[13]),
            rows.map(r=>r[14]), rows.map(r=>r[15]), rows.map(r=>r[16]), rows.map(r=>r[17]), rows.map(r=>r[18]),
            rows.map(r=>r[19]), rows.map(r=>r[20]), rows.map(r=>r[21]),
            rows.map(r=>r[22]), rows.map(r=>r[23]), rows.map(r=>r[24]), rows.map(r=>r[25]), rows.map(r=>r[26]),
            rows.map(r=>r[27]), rows.map(r=>r[28]),
            rows.length,
          ]);
        }
        results.vendas = pedidos.length;
      }

      if (modulos.includes('estoque')) {
        const lastSyncEst = cfg[account + '_token_sync'];
        if (!lastSyncEst) await pool.query(`DROP TABLE IF EXISTS bd_estoque_tiny_${safeName}`);
        await pool.query(`CREATE TABLE IF NOT EXISTS bd_estoque_tiny_${safeName} (
          id_tiny TEXT PRIMARY KEY, sku TEXT, nome TEXT, unidade TEXT,
          estoque_atual NUMERIC DEFAULT 0, estoque_minimo NUMERIC DEFAULT 0,
          preco_custo NUMERIC DEFAULT 0, preco_venda NUMERIC DEFAULT 0,
          marca TEXT, categoria TEXT, atualizado_em TIMESTAMP DEFAULT NOW()
        )`);
        const produtos = await tinyPages('/produtos');
        if (produtos.length > 0) {
          const BATCH = 20;
          for (let i = 0; i < produtos.length; i += BATCH) {
            const batch = produtos.slice(i, i + BATCH);
            const details = await Promise.all(batch.map(p =>
              fetch(`${TINY_API}/produtos/${p.id}`, { headers: { 'Authorization': 'Bearer ' + accessToken } })
                .then(r => r.ok ? r.json() : null).catch(() => null)
            ));
            details.forEach((d, j) => {
              if (!d) return;
              produtos[i + j]._det = d;
            });
          }
          const rows = produtos.map(p => {
            const d = p._det || p;
            return [
              String(p.id || ''),
              p.sku  || p.codigo || null,
              p.descricao || p.nome || null,
              p.unidade || null,
              parseFloat(d.estoque?.quantidade ?? d.saldo?.total ?? 0) || 0,
              parseFloat(d.estoque?.minimo     ?? d.saldo?.minimo ?? 0) || 0,
              parseFloat(p.precos?.precoCusto  ?? p.precoCusto ?? 0)   || 0,
              parseFloat(p.precos?.preco       ?? p.preco ?? 0)        || 0,
              d.marca?.nome || d.marca || null,
              d.categoria?.nome || d.categoria || null,
            ];
          });
          await pool.query(`
            INSERT INTO bd_estoque_tiny_${safeName} (id_tiny,sku,nome,unidade,estoque_atual,estoque_minimo,preco_custo,preco_venda,marca,categoria,atualizado_em)
            SELECT * FROM UNNEST(
              $1::text[],$2::text[],$3::text[],$4::text[],
              $5::numeric[],$6::numeric[],$7::numeric[],$8::numeric[],
              $9::text[],$10::text[],
              (SELECT array_agg(NOW()) FROM generate_series(1,$11))
            )
            ON CONFLICT (id_tiny) DO UPDATE SET
              sku=EXCLUDED.sku, nome=EXCLUDED.nome, unidade=EXCLUDED.unidade,
              estoque_atual=EXCLUDED.estoque_atual, estoque_minimo=EXCLUDED.estoque_minimo,
              preco_custo=EXCLUDED.preco_custo, preco_venda=EXCLUDED.preco_venda,
              marca=EXCLUDED.marca, categoria=EXCLUDED.categoria, atualizado_em=NOW()
          `, [
            rows.map(r=>r[0]), rows.map(r=>r[1]), rows.map(r=>r[2]), rows.map(r=>r[3]),
            rows.map(r=>r[4]), rows.map(r=>r[5]), rows.map(r=>r[6]), rows.map(r=>r[7]),
            rows.map(r=>r[8]), rows.map(r=>r[9]), rows.length,
          ]);
        }
        results.estoque = produtos.length;
      }

      await upsertConfig(pool, company, account + '_token_sync', new Date().toLocaleString('pt-BR'));

      return res.json({ ok: true, account, synced_at: new Date().toISOString(), results });
    } catch(e) { return res.status(500).json({ error: e.message }); }
  }

  // ── cron-tiny: sync + enrich automático ──
  if (req.query.module === 'cron-tiny') {
    try {
      const { company, pool } = getCompanyPool(payload);
      const cfgRes  = await pool.query(`SELECT chave, valor FROM configuracoes WHERE empresa=$1`, [company]);
      const cfg = {};
      for (const r of cfgRes.rows) cfg[r.chave] = r.valor;

      const accounts = Object.keys(cfg)
        .filter(k => k.endsWith('_token') && !k.includes('_refresh') && !k.includes('_exp'))
        .map(k => k.replace(/_token$/, ''))
        .filter(k => k.startsWith('tiny'));

      const log = [];
      const today      = new Date().toISOString().split('T')[0];
      const twoDaysAgo = new Date(Date.now() - 2 * 86400000).toISOString().split('T')[0];
      const TINY_API_BASE = 'https://erp.tiny.com.br/public-api/v3';

      for (const account of accounts) {
        let accessToken = cfg[account + '_token'];
        if (!accessToken) continue;

        const exp = cfg[account + '_exp'] ? new Date(cfg[account + '_exp']) : null;
        if (exp && new Date() >= exp) {
          const tr = await fetch('https://accounts.tiny.com.br/realms/tiny/protocol/openid-connect/token', {
            method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
            body: new URLSearchParams({ grant_type: 'refresh_token', client_id: cfg['tiny_client_id'] || '', client_secret: cfg['tiny_client_secret'] || '', refresh_token: cfg[account + '_refresh'] || '' }),
          });
          if (tr.ok) {
            const nt = await tr.json();
            accessToken = nt.access_token;
            const newExp = new Date(Date.now() + (nt.expires_in || 300) * 1000).toISOString();
            for (const [k, v] of [[account + '_token', accessToken], [account + '_refresh', nt.refresh_token || cfg[account + '_refresh']], [account + '_exp', newExp]])
              await upsertConfig(pool, company, k, v);
          }
        }

        const safeName = account.replace(/[^a-z0-9_]/gi, '_').toLowerCase();

        const items = [];
        let offset = 0;
        while (true) {
          const qs = new URLSearchParams({ dataInicial: twoDaysAgo, dataFinal: today, limit: '100', offset: String(offset) }).toString();
          const pr = await fetch(`${TINY_API_BASE}/pedidos?${qs}`, { headers: { Authorization: 'Bearer ' + accessToken } });
          if (!pr.ok) break;
          const pd = await pr.json();
          const pg = pd.itens || pd.data || [];
          items.push(...pg);
          const tot = pd.paginacao?.total ?? pd.total ?? 0;
          offset += pg.length;
          if (pg.length === 0 || offset >= tot) break;
        }

        if (items.length > 0) {
          const rows = items.map(p => [
            String(p.id||''), p.numeroPedido?String(p.numeroPedido):null,
            p.ecommerce?.numeroPedidoEcommerce||null, p.ecommerce?.numeroPedidoCanalVenda||null,
            p.dataCriacao?p.dataCriacao.split('T')[0]:null, p.dataPrevisao?p.dataPrevisao.split('T')[0]:null,
            typeof p.situacao==='number'?p.situacao:null,
            p.cliente?.nome||null, p.cliente?.cpfCnpj||null, p.cliente?.tipoPessoa||null,
            p.cliente?.email||null, p.cliente?.telefone||null, p.cliente?.celular||null,
            p.cliente?.codigo?String(p.cliente.codigo):null,
            p.cliente?.endereco?.uf||null, p.cliente?.endereco?.municipio||null,
            p.cliente?.endereco?.bairro||null, p.cliente?.endereco?.cep||null, p.cliente?.endereco?.endereco||null,
            parseFloat(p.valor||0)||0, p.ecommerce?.nome||null, p.ecommerce?.canalVenda||null,
            p.transportador?.nome||null, p.transportador?.formaEnvio?.nome||null, p.transportador?.fretePorConta||null,
            p.transportador?.codigoRastreamento||null, p.transportador?.urlRastreamento||null,
            p.vendedor?.nome||null, typeof p.origemPedido==='number'?p.origemPedido:null,
          ]);
          await pool.query(`CREATE TABLE IF NOT EXISTS bd_pedidos_tiny_${safeName} (
            id_tiny TEXT PRIMARY KEY, numero TEXT, numero_ecommerce TEXT, numero_canal_venda TEXT,
            data_criacao DATE, data_previsao DATE, situacao INT,
            nome_cliente TEXT, cpf_cnpj TEXT, tipo_pessoa TEXT, email_cliente TEXT, telefone_cliente TEXT, celular_cliente TEXT, codigo_cliente TEXT,
            uf TEXT, municipio TEXT, bairro TEXT, cep TEXT, endereco TEXT,
            total_pedido NUMERIC DEFAULT 0, marketplace TEXT, canal_venda TEXT,
            transportadora TEXT, forma_envio TEXT, frete_por_conta TEXT, codigo_rastreamento TEXT, url_rastreamento TEXT,
            vendedor TEXT, origem_pedido INT, atualizado_em TIMESTAMP DEFAULT NOW()
          )`);
          await pool.query(`
            INSERT INTO bd_pedidos_tiny_${safeName}
              (id_tiny,numero,numero_ecommerce,numero_canal_venda,data_criacao,data_previsao,situacao,
               nome_cliente,cpf_cnpj,tipo_pessoa,email_cliente,telefone_cliente,celular_cliente,codigo_cliente,
               uf,municipio,bairro,cep,endereco,total_pedido,marketplace,canal_venda,
               transportadora,forma_envio,frete_por_conta,codigo_rastreamento,url_rastreamento,vendedor,origem_pedido,atualizado_em)
            SELECT * FROM UNNEST($1::text[],$2::text[],$3::text[],$4::text[],$5::date[],$6::date[],$7::int[],
              $8::text[],$9::text[],$10::text[],$11::text[],$12::text[],$13::text[],$14::text[],
              $15::text[],$16::text[],$17::text[],$18::text[],$19::text[],
              $20::numeric[],$21::text[],$22::text[],$23::text[],$24::text[],$25::text[],$26::text[],$27::text[],
              $28::text[],$29::int[],(SELECT array_agg(NOW()) FROM generate_series(1,$30)))
            ON CONFLICT (id_tiny) DO UPDATE SET situacao=EXCLUDED.situacao,total_pedido=EXCLUDED.total_pedido,
              data_previsao=EXCLUDED.data_previsao,codigo_rastreamento=EXCLUDED.codigo_rastreamento,atualizado_em=NOW()
          `, [rows.map(r=>r[0]),rows.map(r=>r[1]),rows.map(r=>r[2]),rows.map(r=>r[3]),rows.map(r=>r[4]),rows.map(r=>r[5]),rows.map(r=>r[6]),
              rows.map(r=>r[7]),rows.map(r=>r[8]),rows.map(r=>r[9]),rows.map(r=>r[10]),rows.map(r=>r[11]),rows.map(r=>r[12]),rows.map(r=>r[13]),
              rows.map(r=>r[14]),rows.map(r=>r[15]),rows.map(r=>r[16]),rows.map(r=>r[17]),rows.map(r=>r[18]),
              rows.map(r=>r[19]),rows.map(r=>r[20]),rows.map(r=>r[21]),rows.map(r=>r[22]),rows.map(r=>r[23]),rows.map(r=>r[24]),rows.map(r=>r[25]),rows.map(r=>r[26]),
              rows.map(r=>r[27]),rows.map(r=>r[28]),rows.length]);
        }

        for (const [col, type] of [['financeiro_ok','BOOLEAN DEFAULT FALSE'],['total_produtos','NUMERIC'],['total_desconto','NUMERIC'],['total_frete','NUMERIC'],['total_impostos','NUMERIC'],['total_outras_despesas','NUMERIC'],['frete_pago','NUMERIC'],['custo_produtos','NUMERIC'],['margem_contribuicao','NUMERIC'],['margem_pct','NUMERIC'],['qtd_itens','INT'],['comissoes','NUMERIC'],['taxas_tarifas','NUMERIC']])
          await pool.query(`ALTER TABLE bd_pedidos_tiny_${safeName} ADD COLUMN IF NOT EXISTS ${col} ${type}`).catch(()=>{});

        const threeDaysAgo = new Date(Date.now() - 3 * 86400000).toISOString().split('T')[0];
        const toEnrich = await pool.query(`SELECT id_tiny FROM bd_pedidos_tiny_${safeName} WHERE financeiro_ok IS NOT TRUE AND data_criacao >= $1 ORDER BY data_criacao DESC LIMIT 40`, [threeDaysAgo]);
        let enriched = 0;
        for (const { id_tiny } of toEnrich.rows) {
          const er = await fetch(`${TINY_API_BASE}/pedidos/${id_tiny}`, { headers: { Authorization: 'Bearer ' + accessToken } });
          if (er.status === 429) { await new Promise(r => setTimeout(r, 5000)); continue; }
          if (!er.ok) { await pool.query(`UPDATE bd_pedidos_tiny_${safeName} SET financeiro_ok=TRUE WHERE id_tiny=$1`, [id_tiny]); continue; }
          const ed = await er.json(); const ep = ed.data || ed;
          const tp=parseFloat(ep.valorTotalProdutos||ep.totalProdutos||0)||0, td=parseFloat(ep.valorDesconto||ep.totalDesconto||0)||0, tf=parseFloat(ep.valorFrete||ep.totalFrete||0)||0;
          const ti=0, to2=parseFloat(ep.valorOutrasDespesas||ep.totalOutrasDespesas||0)||0, fp=0, com=0, tax=0;
          let custo=0, qtd=0;
          await pool.query(`CREATE TABLE IF NOT EXISTS bd_pedidos_tiny_itens_${safeName} (id BIGSERIAL PRIMARY KEY, id_pedido TEXT NOT NULL, sku TEXT, nome_produto TEXT, quantidade NUMERIC DEFAULT 0, valor_unit NUMERIC DEFAULT 0, desconto_unit NUMERIC DEFAULT 0, preco_custo NUMERIC DEFAULT 0, total_item NUMERIC DEFAULT 0, UNIQUE (id_pedido, sku))`).catch(()=>{});
          if (Array.isArray(ep.itens)) {
            for (const it of ep.itens) {
              const qty=parseFloat(it.quantidade||1)||1, vc=parseFloat(it.precoCusto||it.produto?.precoCusto||0)||0;
              const sku=it.produto?.sku||it.sku||it.produto?.codigo||String(it.produto?.id||'');
              const valUnit=parseFloat(it.valorUnitario||it.valor||0)||0;
              const nome=it.produto?.nome||it.descricao||null;
              custo+=vc*qty; qtd+=qty;
              await pool.query(`INSERT INTO bd_pedidos_tiny_itens_${safeName} (id_pedido,sku,nome_produto,quantidade,valor_unit,preco_custo,total_item) VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT (id_pedido,sku) DO UPDATE SET quantidade=EXCLUDED.quantidade,valor_unit=EXCLUDED.valor_unit,preco_custo=EXCLUDED.preco_custo,total_item=EXCLUDED.total_item`, [id_tiny, sku||null, nome, qty, valUnit, vc, qty*valUnit]).catch(()=>{});
            }
          }
          const receita=tp-td+tf; const margem=receita-custo-fp-ti-to2-com-tax;
          await pool.query(`UPDATE bd_pedidos_tiny_${safeName} SET total_produtos=$2,total_desconto=$3,total_frete=$4,total_impostos=$5,total_outras_despesas=$6,frete_pago=$7,custo_produtos=$8,margem_contribuicao=$9,margem_pct=$10,qtd_itens=$11,comissoes=$12,taxas_tarifas=$13,financeiro_ok=TRUE,atualizado_em=NOW() WHERE id_tiny=$1`,
            [id_tiny,tp,td,tf,ti,to2,fp,custo,margem,receita>0?Math.round(margem/receita*10000)/100:0,qtd,com,tax]);
          enriched++;
          await new Promise(r => setTimeout(r, 1000));
        }

        await upsertConfig(pool, company, account + '_token_sync', new Date().toLocaleString('pt-BR'));
        log.push({ account, synced: items.length, enriched });
      }
      return res.json({ ok: true, ran_at: new Date().toISOString(), log });
    } catch(e) { return res.status(500).json({ error: e.message }); }
  }

  // ── tiny-skip-old ──
  if (req.query.module === 'tiny-skip-old') {
    try {
      const account  = req.query.account;
      const before   = req.query.before;
      if (!account || !before) return res.status(400).json({ error: 'account e before obrigatórios' });
      const { company, pool } = getCompanyPool(payload);
      const safeName = account.replace(/[^a-z0-9_]/gi, '_').toLowerCase();
      await pool.query(`ALTER TABLE bd_pedidos_tiny_${safeName} ADD COLUMN IF NOT EXISTS financeiro_ok BOOLEAN DEFAULT FALSE`).catch(()=>{});
      const r = await pool.query(
        `UPDATE bd_pedidos_tiny_${safeName} SET financeiro_ok=TRUE WHERE (data_criacao < $1 OR data_criacao IS NULL) AND financeiro_ok IS NOT TRUE`,
        [before]
      );
      const pending = await pool.query(`SELECT COUNT(*) AS c FROM bd_pedidos_tiny_${safeName} WHERE financeiro_ok IS NOT TRUE`);
      return res.json({ ok: true, skipped: r.rowCount, pending_to_enrich: parseInt(pending.rows[0].c) });
    } catch(e) { return res.status(500).json({ error: e.message }); }
  }

  // ── tiny-enrich ──
  if (req.query.module === 'tiny-enrich') {
    try {
      const account = req.query.account;
      if (!account) return res.status(400).json({ error: 'account obrigatório' });

      const { company, pool } = getCompanyPool(payload);

      const cfg = {};
      const cfgRows = await pool.query(`SELECT chave, valor FROM configuracoes WHERE empresa=$1`, [company]);
      for (const r of cfgRows.rows) cfg[r.chave] = r.valor;

      let accessToken = cfg[account + '_token'];
      const exp = cfg[account + '_exp'] ? new Date(cfg[account + '_exp']) : null;
      if (!accessToken) return res.status(401).json({ error: 'Conta não autenticada. Conecte via OAuth primeiro.' });
      if (exp && new Date() >= exp) {
        const refreshToken = cfg[account + '_refresh'];
        const clientId     = cfg['tiny_client_id']     || process.env.TINY_CLIENT_ID;
        const clientSecret = cfg['tiny_client_secret'] || process.env.TINY_CLIENT_SECRET;
        if (refreshToken && clientId && clientSecret) {
          const tr = await fetch('https://accounts.tiny.com.br/realms/tiny/protocol/openid-connect/token', {
            method: 'POST',
            headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
            body: new URLSearchParams({ grant_type:'refresh_token', client_id:clientId, client_secret:clientSecret, refresh_token:refreshToken }),
          });
          if (tr.ok) {
            const nt = await tr.json();
            accessToken = nt.access_token;
            const newExp = new Date(Date.now() + (nt.expires_in || 300) * 1000).toISOString();
            for (const [k, v] of [[account+'_token', accessToken],[account+'_refresh', nt.refresh_token||refreshToken],[account+'_exp', newExp]])
              await upsertConfig(pool, company, k, v);
          }
        }
      }

      const safeName  = account.replace(/[^a-z0-9_]/gi, '_').toLowerCase();
      const batchSize = parseInt(req.query.batch || '200');
      const delayMs   = parseInt(req.query.delay || '600');
      const TINY_API  = 'https://erp.tiny.com.br/public-api/v3';

      await pool.query(`CREATE TABLE IF NOT EXISTS bd_pedidos_tiny_itens_${safeName} (
        id            BIGSERIAL PRIMARY KEY,
        id_pedido     TEXT NOT NULL,
        sku           TEXT,
        nome_produto  TEXT,
        quantidade    NUMERIC DEFAULT 0,
        valor_unit    NUMERIC DEFAULT 0,
        desconto_unit NUMERIC DEFAULT 0,
        preco_custo   NUMERIC DEFAULT 0,
        total_item    NUMERIC DEFAULT 0,
        UNIQUE (id_pedido, sku)
      )`).catch(()=>{});

      const financialCols = [
        ['total_produtos','NUMERIC'],['total_desconto','NUMERIC'],['total_frete','NUMERIC'],
        ['total_impostos','NUMERIC'],['total_outras_despesas','NUMERIC'],['frete_pago','NUMERIC'],
        ['comissoes','NUMERIC'],['taxas_tarifas','NUMERIC'],['custo_produtos','NUMERIC'],
        ['margem_contribuicao','NUMERIC'],['margem_pct','NUMERIC'],['qtd_itens','INT'],
        ['financeiro_ok','BOOLEAN DEFAULT FALSE'],
      ];
      for (const [col, type] of financialCols)
        await pool.query(`ALTER TABLE bd_pedidos_tiny_${safeName} ADD COLUMN IF NOT EXISTS ${col} ${type}`).catch(()=>{});

      const canaisCfgRows = await pool.query(
        `SELECT canal, pct_comissao, pct_taxa, pct_imposto FROM tiny_canais_config WHERE empresa=$1`,
        [company]
      ).catch(()=>({rows:[]}));
      const canaisCfg = Object.fromEntries(canaisCfgRows.rows.map(r=>[r.canal.toLowerCase().trim(),r]));

      const enrichFrom = req.query.from || null;
      const enrichTo   = req.query.to   || null;
      let dateClause = '';
      const dateParams = [batchSize];
      if (enrichFrom) { dateParams.push(enrichFrom); dateClause += ` AND data_criacao >= $${dateParams.length}`; }
      if (enrichTo)   { dateParams.push(enrichTo);   dateClause += ` AND data_criacao <= $${dateParams.length}`; }
      const pendentes = await pool.query(
        `SELECT id_tiny, LOWER(TRIM(COALESCE(marketplace,''))) AS canal FROM bd_pedidos_tiny_${safeName} WHERE financeiro_ok IS NOT TRUE${dateClause} ORDER BY data_criacao DESC LIMIT $1`,
        dateParams
      );
      const pedidosMap = Object.fromEntries(pendentes.rows.map(r=>[r.id_tiny, r.canal]));
      const ids = pendentes.rows.map(r => r.id_tiny);
      if (ids.length === 0) return res.json({ ok: true, account, enriched: 0, pending: 0, message: 'Todos os pedidos já enriquecidos!' });

      let enriched = 0;
      let rateLimited = 0;
      for (const id of ids) {
        try {
          const r = await fetch(`${TINY_API}/pedidos/${id}`, { headers: { 'Authorization': 'Bearer ' + accessToken } });
          if (r.status === 429) { rateLimited++; if (rateLimited >= 5) break; await new Promise(res => setTimeout(res, 5000)); continue; }
          if (!r.ok) { await pool.query(`UPDATE bd_pedidos_tiny_${safeName} SET financeiro_ok=TRUE WHERE id_tiny=$1`, [id]); continue; }
          rateLimited = 0;
          const d = await r.json();
          const p = d.data || d;

          const totalProdutos = parseFloat(p.valorTotalProdutos || p.totalProdutos || 0) || 0;
          const totalDesconto = parseFloat(p.valorDesconto      || p.totalDesconto || 0) || 0;
          const totalFrete    = parseFloat(p.valorFrete         || p.totalFrete    || 0) || 0;
          const totalOutras   = parseFloat(p.valorOutrasDespesas|| p.totalOutrasDespesas || 0) || 0;
          const totalImpostos = 0;
          const fretePago     = 0;
          const comissoes     = 0;
          const taxasTarifas  = 0;

          let qtdItens = 0;
          const skusQtd = {};
          const itensList = [];
          if (Array.isArray(p.itens)) {
            for (const item of p.itens) {
              const qty      = parseFloat(item.quantidade || 1) || 1;
              const valUnit  = parseFloat(item.valorUnitario || item.valor || 0) || 0;
              const descUnit = parseFloat(item.desconto || item.valorDesconto || 0) || 0;
              const custo    = parseFloat(item.precoCusto || item.produto?.precoCusto || 0) || 0;
              const sku      = item.produto?.sku || item.sku || item.produto?.codigo || String(item.produto?.id || '');
              const nome     = item.produto?.nome || item.descricao || null;
              qtdItens += qty;
              if (sku) skusQtd[String(sku)] = (skusQtd[String(sku)] || 0) + qty;
              itensList.push({ sku: sku || null, nome, qty, valUnit, descUnit, custo, total: qty * valUnit });
            }
            for (const it of itensList) {
              await pool.query(`
                INSERT INTO bd_pedidos_tiny_itens_${safeName}
                  (id_pedido, sku, nome_produto, quantidade, valor_unit, desconto_unit, preco_custo, total_item)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                ON CONFLICT (id_pedido, sku) DO UPDATE SET
                  quantidade=EXCLUDED.quantidade, valor_unit=EXCLUDED.valor_unit,
                  desconto_unit=EXCLUDED.desconto_unit, preco_custo=EXCLUDED.preco_custo,
                  total_item=EXCLUDED.total_item
              `, [id, it.sku, it.nome, it.qty, it.valUnit, it.descUnit, it.custo, it.total]).catch(()=>{});
            }
          }

          let custoProdutos = 0;
          const skuList = Object.keys(skusQtd);
          if (skuList.length > 0) {
            const costRes = await pool.query(
              `SELECT sku, preco_custo FROM bd_estoque_tiny_${safeName} WHERE sku = ANY($1)`,
              [skuList]
            ).catch(() => ({ rows: [] }));
            for (const row of costRes.rows) {
              custoProdutos += (parseFloat(row.preco_custo) || 0) * (skusQtd[row.sku] || 0);
            }
          }

          const canalKey  = (pedidosMap[id] || '').toLowerCase().trim();
          const canalConf = canaisCfg[canalKey] || {};
          const pctComissao = parseFloat(canalConf.pct_comissao || 0) / 100;
          const pctTaxa     = parseFloat(canalConf.pct_taxa     || 0) / 100;
          const pctImposto  = parseFloat(canalConf.pct_imposto  || 0) / 100;
          const comissaoCalc    = totalProdutos * pctComissao;
          const taxaCalc        = totalProdutos * pctTaxa;
          const impostoCalc     = totalProdutos * pctImposto;

          const receita   = totalProdutos - totalDesconto + totalFrete;
          const margem    = receita - custoProdutos - comissaoCalc - taxaCalc - impostoCalc - totalOutras;
          const margemPct = receita > 0 ? Math.round((margem / receita) * 10000) / 100 : 0;

          await pool.query(`
            UPDATE bd_pedidos_tiny_${safeName} SET
              total_produtos=$2, total_desconto=$3, total_frete=$4,
              total_outras_despesas=$5, frete_pago=$6,
              comissoes=$7, taxas_tarifas=$8, total_impostos=$9,
              custo_produtos=$10, margem_contribuicao=$11, margem_pct=$12, qtd_itens=$13,
              financeiro_ok=TRUE, atualizado_em=NOW()
            WHERE id_tiny=$1
          `, [id, totalProdutos, totalDesconto, totalFrete, totalOutras, fretePago, comissaoCalc, taxaCalc, impostoCalc, custoProdutos, margem, margemPct, qtdItens]);
          enriched++;
          await new Promise(res => setTimeout(res, delayMs));
        } catch { /* pula este pedido */ }
      }

      const restantes = await pool.query(`SELECT COUNT(*) AS c FROM bd_pedidos_tiny_${safeName} WHERE financeiro_ok IS NOT TRUE`);
      return res.json({ ok: true, account, enriched, rate_limited: rateLimited, pending: parseInt(restantes.rows[0].c) });
    } catch(e) { return res.status(500).json({ error: e.message }); }
  }

  // ── tiny-margem ──
  if (req.query.module === 'tiny-margem') {
    try {
      const { company, pool } = getCompanyPool(payload);
      const account  = req.query.account;
      if (!account) return res.status(400).json({ error: 'account obrigatório' });
      const safeName = account.replace(/[^a-z0-9_]/gi, '_').toLowerCase();

      let sessionCookie, csrfToken;
      if (req.method === 'POST' && req.body?.sessionCookie) {
        sessionCookie = req.body.sessionCookie;
        csrfToken     = req.body.csrfToken || '';
        for (const [k,v] of [[account+'_olist_cookie', sessionCookie],[account+'_olist_csrf', csrfToken]]) {
          await upsertConfig(pool, company, k, v);
        }
      } else {
        const cfgRes = await pool.query(`SELECT chave,valor FROM configuracoes WHERE empresa=$1 AND chave IN ($2,$3)`, [company, account+'_olist_cookie', account+'_olist_csrf']);
        const cfg = Object.fromEntries(cfgRes.rows.map(r=>[r.chave,r.valor]));
        sessionCookie = cfg[account+'_olist_cookie'];
        csrfToken     = cfg[account+'_olist_csrf'] || '';
      }

      const from = req.query.from || req.body?.from || new Date(Date.now() - 30*86400000).toISOString().split('T')[0];
      const to   = req.query.to   || req.body?.to   || new Date().toISOString().split('T')[0];
      const perPage = 200;

      for (const [col, type] of [['comissoes','NUMERIC'],['taxas_tarifas','NUMERIC'],['total_impostos','NUMERIC'],['margem_contribuicao','NUMERIC'],['margem_pct','NUMERIC'],['custo_produtos','NUMERIC'],['total_produtos','NUMERIC'],['total_desconto','NUMERIC'],['total_frete','NUMERIC'],['financeiro_ok','BOOLEAN DEFAULT FALSE']])
        await pool.query(`ALTER TABLE bd_pedidos_tiny_${safeName} ADD COLUMN IF NOT EXISTS ${col} ${type}`).catch(()=>{});

      const _cfgKeys = [account+'_token', account+'_refresh', account+'_exp', 'tiny_client_id', 'tiny_client_secret'];
      const _cfgRows = await pool.query(
        `SELECT chave,valor FROM configuracoes WHERE empresa=$1 AND chave=ANY($2)`,
        [company, _cfgKeys]
      ).catch(() => ({ rows: [] }));
      const _cfg = Object.fromEntries(_cfgRows.rows.map(r => [r.chave, r.valor]));
      let bearerToken = _cfg[account+'_token'] || '';
      const _expAt    = _cfg[account+'_exp'] ? new Date(_cfg[account+'_exp']) : null;
      const _needsRefresh = !bearerToken || (_expAt && _expAt.getTime() - Date.now() < 60_000);
      if (_needsRefresh && _cfg[account+'_refresh']) {
        const TINY_TOKEN_URL = 'https://accounts.tiny.com.br/realms/tiny/protocol/openid-connect/token';
        const _clientId  = (_cfg['tiny_client_id']     || process.env.TINY_CLIENT_ID     || '').trim();
        const _clientSec = (_cfg['tiny_client_secret']  || process.env.TINY_CLIENT_SECRET || '').trim();
        try {
          const _rr = await fetch(TINY_TOKEN_URL, {
            method: 'POST',
            headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
            body: new URLSearchParams({ grant_type: 'refresh_token', client_id: _clientId, client_secret: _clientSec, refresh_token: _cfg[account+'_refresh'] }).toString(),
          });
          const _rd = await _rr.json();
          if (_rr.ok && _rd.access_token) {
            bearerToken = _rd.access_token;
            const _newExp = new Date(Date.now() + (_rd.expires_in || 21600) * 1000).toISOString();
            for (const [k,v] of [[account+'_token', bearerToken],[account+'_refresh', _rd.refresh_token || _cfg[account+'_refresh']],[account+'_exp', _newExp]]) {
              await upsertConfig(pool, company, k, v);
            }
            console.log(`[tiny-margem] Token renovado para ${account}, expira ${_newExp}`);
          } else {
            console.warn(`[tiny-margem] Falha no refresh: ${JSON.stringify(_rd)}`);
          }
        } catch(_re) { console.warn(`[tiny-margem] Erro no refresh: ${_re.message}`); }
      }

      const olistHeaders = {
        'Content-Type': 'application/json',
        'Accept': 'application/json, text/plain, */*',
        'Origin': 'https://erp.olist.com',
        'Referer': 'https://erp.olist.com/margem_contribuicao',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124',
        'X-Requested-With': 'XMLHttpRequest',
        ...(bearerToken ? { 'Authorization': 'Bearer ' + bearerToken } : {}),
        ...(sessionCookie ? { 'Cookie': sessionCookie } : {}),
        ...(csrfToken ? { 'X-CSRF-TOKEN': csrfToken } : {}),
      };

      function toISO(dateStr, endOfDay = false) {
        const d = new Date(dateStr.includes('/') ? dateStr.split('/').reverse().join('-') : dateStr);
        if (endOfDay) d.setUTCHours(2, 59, 59, 999);
        else d.setUTCHours(3, 0, 0, 0);
        return d.toISOString();
      }

      let totalFetched = 0, totalUpdated = 0;
      let firstResponse = null;

      const preloadedItems = Array.isArray(req.body?.items) ? req.body.items : null;
      if (preloadedItems) {
        console.log(`[tiny-margem] ${preloadedItems.length} itens recebidos diretamente, pulando fetch do Olist`);
      }

      const itemBatches = preloadedItems
        ? [preloadedItems]
        : await (async () => {
            const batches = [];
            let page = 1;
            while (true) {
              const r = await fetch('https://erp.olist.com/api/v1/contribution-margin/list', {
                method: 'POST', headers: olistHeaders,
                body: JSON.stringify({ page, perPage, sort: 'date', order: 'desc',
                  filters: { period: { start: toISO(from, false), end: toISO(to, true) },
                    search: '', channels: [], products: [], categories: [], tags: [] } }),
              });
              if (!r.ok) { const e = await r.text(); throw Object.assign(new Error(`Olist API ${r.status}`), { detail: e.substring(0,300), page }); }
              const json = await r.json();
              if (page === 1) firstResponse = { keys: json.data?.[0] ? Object.keys(json.data[0]) : [], meta: json.meta || json.pagination || {} };
              const items = json.data || json.itens || json.items || [];
              if (items.length === 0) break;
              batches.push(items);
              if (items.length < perPage) break;
              page++;
            }
            return batches;
          })().catch(e => { return res.status(500).json({ error: e.message, detail: e.detail, page: e.page }), null; });

      if (!itemBatches) return;

      for (const items of itemBatches) {
        totalFetched += items.length;
        for (const item of items) {
          const numeroPedido    = String(item.orderNumber || item.numero_pedido || item.order_number || item.orderId || '');
          const numeroEcommerce = String(item.orderEcommerceNumber || item.numero_ecommerce || '');
          const faturamento     = parseFloat(item.revenue        || item.faturamento    || 0) || 0;
          const cmv             = parseFloat(item.cmv            || item.custo          || 0) || 0;
          const comissao        = parseFloat(item.commission     || item.comissao       || 0) || 0;
          const taxas           = parseFloat(item.taxes          || item.taxas          || 0) || 0;
          const impostos        = parseFloat(item.taxes          || item.impostos       || 0) || 0;
          const frete           = parseFloat(item.freight        || item.frete          || 0) || 0;
          const desconto        = parseFloat(item.discount       || item.desconto       || 0) || 0;
          const margem          = parseFloat(item.contributionMargin || item.margem     || 0) || 0;
          const margemPct       = parseFloat(item.contributionMarginPercentage || item.margem_pct || 0) || 0;

          const upd = await pool.query(`
            UPDATE bd_pedidos_tiny_${safeName} SET
              total_produtos=$2, total_desconto=$3, total_frete=$4,
              total_impostos=$5, custo_produtos=$6,
              comissoes=$7, taxas_tarifas=$8,
              margem_contribuicao=$9, margem_pct=$10,
              financeiro_ok=TRUE, atualizado_em=NOW()
            WHERE numero=$1 OR numero_ecommerce=$1 OR numero_canal_venda=$1
          `, [numeroPedido||numeroEcommerce, faturamento, desconto, frete, impostos, cmv, comissao, taxas, margem, margemPct]);
          if (upd.rowCount > 0) totalUpdated++;
        }
      }

      return res.json({ ok: true, account, from, to, fetched: totalFetched, updated: totalUpdated, schema_sample: firstResponse });
    } catch(e) { return res.status(500).json({ error: e.message }); }
  }

  // ── tiny-canais ──
  if (req.query.module === 'tiny-canais') {
    try {
      const { company, pool } = getCompanyPool(payload);

      if (req.method !== 'POST') {
        const account  = req.query.account;
        const safeName = account ? account.replace(/[^a-z0-9_]/gi,'_').toLowerCase() : null;
        let canaisDb = [];
        if (safeName) {
          canaisDb = (await pool.query(
            `SELECT DISTINCT COALESCE(NULLIF(marketplace,''),'Sem canal') AS canal
             FROM bd_pedidos_tiny_${safeName} ORDER BY 1`
          ).catch(()=>({rows:[]}))).rows.map(r=>r.canal);
        }
        const cfg = (await pool.query(
          `SELECT canal, pct_comissao, pct_taxa, pct_imposto FROM tiny_canais_config WHERE empresa=$1 ORDER BY canal`,
          [company]
        )).rows;
        const cfgMap = Object.fromEntries(cfg.map(r=>[r.canal,r]));
        const result = canaisDb.map(canal => ({
          canal,
          pct_comissao: parseFloat(cfgMap[canal]?.pct_comissao || 0),
          pct_taxa:     parseFloat(cfgMap[canal]?.pct_taxa     || 0),
          pct_imposto:  parseFloat(cfgMap[canal]?.pct_imposto  || 0),
        }));
        return res.json({ canais: result, config_db: cfg });
      }

      const rows = Array.isArray(req.body) ? req.body : [req.body];
      for (const { canal, pct_comissao=0, pct_taxa=0, pct_imposto=0 } of rows) {
        if (!canal) continue;
        await pool.query(`
          INSERT INTO tiny_canais_config (empresa,canal,pct_comissao,pct_taxa,pct_imposto,atualizado_em)
          VALUES ($1,$2,$3,$4,$5,NOW())
          ON CONFLICT (empresa,canal) DO UPDATE SET
            pct_comissao=EXCLUDED.pct_comissao, pct_taxa=EXCLUDED.pct_taxa,
            pct_imposto=EXCLUDED.pct_imposto, atualizado_em=NOW()
        `, [company, canal, pct_comissao, pct_taxa, pct_imposto]);
      }
      return res.json({ ok: true, saved: rows.length });
    } catch(e) { return res.status(500).json({ error: e.message }); }
  }

  // ── import-margem ──
  if (req.query.module === 'import-margem') {
    try {
      const account = req.query.account;
      if (!account) return res.status(400).json({ error: 'account obrigatório' });
      const { company, pool } = getCompanyPool(payload);
      const safeName = account.replace(/[^a-z0-9_]/gi, '_').toLowerCase();

      for (const [col, type] of [
        ['comissoes','NUMERIC'], ['taxas_tarifas','NUMERIC'], ['frete_pago','NUMERIC'],
        ['custo_produtos','NUMERIC'], ['margem_contribuicao','NUMERIC'], ['margem_pct','NUMERIC'],
        ['qtd_itens','INT'], ['total_produtos','NUMERIC'], ['financeiro_ok','BOOLEAN DEFAULT FALSE'],
      ]) await pool.query(`ALTER TABLE bd_pedidos_tiny_${safeName} ADD COLUMN IF NOT EXISTS ${col} ${type}`).catch(()=>{});

      let csvText = '';
      if (typeof req.body === 'string') {
        csvText = req.body;
      } else if (req.body?.csv) {
        csvText = req.body.csv;
      } else {
        return res.status(400).json({ error: 'Envie o CSV no body (Content-Type: text/plain) ou JSON { csv: "..." }' });
      }

      const lines = csvText.replace(/\r/g, '').split('\n').filter(l => l.trim());
      if (lines.length < 2) return res.status(400).json({ error: 'CSV vazio ou inválido' });

      const sep = lines[0].includes(';') ? ';' : ',';
      const parseRow = l => l.split(sep).map(c => c.replace(/^"|"$/g, '').trim());
      const parseNum = v => {
        if (!v || v === '-') return 0;
        return parseFloat(v.replace(/\./g, '').replace(',', '.')) || 0;
      };

      const headers = parseRow(lines[0]).map(h => h.toLowerCase()
        .normalize('NFD').replace(/[\u0300-\u036f]/g, '')
        .replace(/[^a-z0-9]/g, '_').replace(/_+/g, '_').replace(/^_|_$/g, '')
      );

      const idx = name => headers.findIndex(h => h.includes(name));
      const iNumero     = idx('pedido') !== -1 ? idx('pedido') : idx('numero');
      const iFaturamento= idx('faturamento');
      const iComissoes  = idx('comissao');
      const iTaxas      = idx('taxa');
      const iFrete      = idx('frete');
      const iCusto      = idx('custo');
      const iMargem     = idx('margem_de') !== -1 ? idx('margem_de') : idx('margem_c');
      const iIndice     = idx('indice');
      const iQtd        = idx('qtd') !== -1 ? idx('qtd') : idx('quantidade');

      if (iNumero === -1) return res.status(400).json({ error: 'Coluna de número do pedido não encontrada', headers });

      let updated = 0, notFound = 0;
      const notFoundList = [];

      for (let i = 1; i < lines.length; i++) {
        const row = parseRow(lines[i]);
        if (row.length < 2) continue;

        const numeroPedido = row[iNumero]?.replace(/\D/g, '');
        if (!numeroPedido) continue;

        const faturamento = iFaturamento !== -1 ? parseNum(row[iFaturamento]) : 0;
        const comissoes   = iComissoes   !== -1 ? parseNum(row[iComissoes])   : 0;
        const taxas       = iTaxas       !== -1 ? parseNum(row[iTaxas])       : 0;
        const frete       = iFrete       !== -1 ? parseNum(row[iFrete])       : 0;
        const custo       = iCusto       !== -1 ? parseNum(row[iCusto])       : 0;
        const margem      = iMargem      !== -1 ? parseNum(row[iMargem])      : 0;
        const indice      = iIndice      !== -1 ? parseNum(row[iIndice])      : 0;
        const qtd         = iQtd         !== -1 ? parseNum(row[iQtd])         : 0;

        const r = await pool.query(`
          UPDATE bd_pedidos_tiny_${safeName} SET
            total_produtos    = $2,
            comissoes         = $3,
            taxas_tarifas     = $4,
            frete_pago        = $5,
            custo_produtos    = $6,
            margem_contribuicao = $7,
            margem_pct        = $8,
            qtd_itens         = $9,
            financeiro_ok     = TRUE,
            atualizado_em     = NOW()
          WHERE numero = $1
        `, [numeroPedido, faturamento, comissoes, taxas, frete, custo, margem, indice, qtd]);

        if (r.rowCount > 0) {
          updated++;
        } else {
          notFound++;
          if (notFoundList.length < 10) notFoundList.push(numeroPedido);
        }
      }

      return res.json({
        ok: true, total_rows: lines.length - 1,
        updated, not_found: notFound,
        not_found_sample: notFoundList,
        headers_detected: headers,
      });
    } catch(e) { return res.status(500).json({ error: e.message }); }
  }
};
