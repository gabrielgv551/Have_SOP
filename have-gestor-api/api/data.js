const companies = require('../lib/companies');
const jwt = require('jsonwebtoken');
const { Pool } = require('pg');

// Whitelist de tabelas permitidas — segurança contra SQL injection via nome de tabela
const TABELAS_PERMITIDAS = [
  'curva_abc',
  'ponto_pedido',
  'estoque_seguranca',
  'ppr_sku',
  'forecast_12m',
  'semana_pedidos',
  'cadastros_sku',
  'sku_discontinued',
  'pmv',
  'pmv_months',
  'pmv_canais',
  'monthly_revenue',
  'dashboard_kpis',
  'sopc',
  'sku_atividade',
  'categoria_vendas',
  'contas_pagar',
  'forecast_diario',
];

// Cache simples de pools por empresa (evita criar nova conexão a cada request)
const pools = {};

function getPool(company) {
  if (pools[company]) return pools[company];
  const key = companies[company].dbEnvKey;
  pools[company] = new Pool({
    host:     process.env[`${key}_HOST`],
    port:     parseInt(process.env[`${key}_PORT`] || '5432'),
    database: process.env[`${key}_DB`],
    user:     process.env[`${key}_USER`],
    password: process.env[`${key}_PASSWORD`],
    ssl:      { rejectUnauthorized: false },
    max:      5,
  });
  return pools[company];
}

// ── SOPC helpers ──────────────────────────────────────────────
async function getTableColumns(pool, tableName) {
  try {
    const r = await pool.query(
      `SELECT column_name FROM information_schema.columns WHERE table_name=$1 ORDER BY ordinal_position`,
      [tableName]
    );
    return r.rows.map(c => c.column_name);
  } catch { return []; }
}
function findSkuCol(cols) {
  return cols.find(c => c.toLowerCase() === 'sku')
    || cols.find(c => c.toLowerCase().includes('sku')) || null;
}
function findStockCol(cols, skuCol) {
  const kws = ['estoque','disponivel','disponível','quantidade','qty','inventory','stock'];
  for (const kw of kws) {
    const f = cols.find(c => c !== skuCol && c.toLowerCase().includes(kw));
    if (f) return f;
  }
  return cols.find(c => c !== skuCol) || null;
}
async function lerEstoqueFullMap(pool, tableName) {
  const cols = await getTableColumns(pool, tableName);
  if (!cols.length) return {};
  const skuCol = findSkuCol(cols);
  const stockCol = findStockCol(cols, skuCol);
  if (!skuCol || !stockCol) return {};
  try {
    const r = await pool.query(
      `SELECT "${skuCol}" AS sku, SUM("${stockCol}"::numeric) AS qtd FROM ${tableName} WHERE "${skuCol}" IS NOT NULL AND TRIM("${skuCol}"::text) != '' GROUP BY "${skuCol}"`
    );
    const map = {};
    r.rows.forEach(row => { const s = String(row.sku||'').trim(); if(s) map[s]=(map[s]||0)+(parseFloat(row.qtd)||0); });
    return map;
  } catch { return {}; }
}
// ──────────────────────────────────────────────────────────────

module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');

  if (req.method === 'OPTIONS') return res.status(200).end();

  // 1. Verificar token JWT
  const auth = (req.headers.authorization || '').split(' ')[1];
  if (!auth) return res.status(401).json({ error: 'Token não fornecido' });

  let payload;
  try {
    payload = jwt.verify(auth, process.env.JWT_SECRET);
  } catch {
    return res.status(401).json({ error: 'Token inválido ou expirado. Faça login novamente.' });
  }

  // Módulo Margens · DRE Gerencial
  if (req.query.module === 'margens') {
    const company = payload.company || 'lanzi';
    const pool = getPool(company);
    const { ano, mes, todos_meses } = req.query;
    try {
      if (!ano || !mes) {
        const r = await pool.query(`
          SELECT DISTINCT "Ano" AS ano, "Mes" AS mes
          FROM bd_vendas
          WHERE "Ano" IS NOT NULL AND "Mes" IS NOT NULL
          ORDER BY "Ano" DESC, "Mes" DESC
          LIMIT 24
        `);
        return res.json({ meses: r.rows });
      }
      if (todos_meses === 'true') {
        const [rMeses, rbMeses, rSkus] = await Promise.all([
          pool.query(`
            SELECT
              "Mes"::int AS mes,
              SUM("Total Venda") AS receita_bruta,
              SUM(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN "Total Venda" ELSE 0 END) AS receita_liquida,
              SUM(CASE WHEN "Status"  ~* '(cancel|devol|n[aã]o.?pago)' THEN "Total Venda" ELSE 0 END) AS devolucoes,
              SUM(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN COALESCE("Margem Produto",0) ELSE 0 END) AS margem_contribuicao,
              SUM(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN "Quantidade Vendida" ELSE 0 END) AS qtd_liquida
            FROM bd_vendas
            WHERE "Ano"::int = $1
            GROUP BY "Mes"
            ORDER BY "Mes"::int
          `, [parseInt(ano)]),
          pool.query(`
            SELECT "Mes"::int AS mes, SUM(tvp) AS receita_bruta_global
            FROM (
              SELECT "Mes", "Order ID", MAX("Total Venda Pedido") AS tvp
              FROM bd_vendas
              WHERE "Ano"::int = $1
              GROUP BY "Mes", "Order ID"
            ) t
            GROUP BY "Mes"
            ORDER BY "Mes"::int
          `, [parseInt(ano)]),
          pool.query(`
            SELECT
              "Sku"                                                                                AS sku,
              MAX("Nome Produto")                                                                  AS nome_produto,
              MAX("Categoria")                                                                     AS categoria,
              SUM("Total Venda")                                                                   AS receita_bruta,
              SUM(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN "Total Venda"            ELSE 0 END) AS receita_liquida,
              SUM(CASE WHEN "Status"  ~* '(cancel|devol|n[aã]o.?pago)' THEN "Total Venda" ELSE 0 END) AS devolucoes,
              SUM(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN COALESCE("Margem Produto",0) ELSE 0 END) AS margem_contribuicao,
              SUM(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN "Quantidade Vendida"     ELSE 0 END) AS qtd_liquida,
              ROUND(
                (CASE WHEN SUM(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN "Total Venda" ELSE 0 END) > 0
                  THEN SUM(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN COALESCE("Margem Produto",0) ELSE 0 END)
                     / NULLIF(SUM(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN "Total Venda" ELSE 0 END),0) * 100
                  ELSE 0 END)::numeric
              , 1) AS margem_pct
            FROM bd_vendas
            WHERE "Ano"::int = $1
              AND "Sku" IS NOT NULL AND TRIM("Sku"::text) != ''
            GROUP BY "Sku"
            HAVING SUM("Total Venda") > 0
            ORDER BY receita_liquida DESC
          `, [parseInt(ano)])
        ]);
        const rbMap = {};
        rbMeses.rows.forEach(r => { rbMap[r.mes] = parseFloat(r.receita_bruta_global) || 0; });
        const meses_dre = rMeses.rows.map(r => ({
          mes: r.mes,
          receita_bruta_global: rbMap[r.mes] || parseFloat(r.receita_bruta) || 0,
          receita_liquida:     parseFloat(r.receita_liquida)     || 0,
          devolucoes:          parseFloat(r.devolucoes)          || 0,
          margem_contribuicao: parseFloat(r.margem_contribuicao) || 0,
          qtd_liquida:         parseFloat(r.qtd_liquida)         || 0,
        }));
        return res.json({ meses_dre, skus_ano: rSkus.rows });
      }
      const [r, rbRow] = await Promise.all([
        pool.query(`
          SELECT
            "Sku"                                                                                AS sku,
            MAX("Nome Produto")                                                                  AS nome_produto,
            MAX("Categoria")                                                                     AS categoria,
            SUM("Total Venda")                                                                   AS receita_bruta,
            SUM(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN "Total Venda"            ELSE 0 END) AS receita_liquida,
            SUM(CASE WHEN "Status"  ~* '(cancel|devol|n[aã]o.?pago)' THEN "Total Venda" ELSE 0 END) AS devolucoes,
            SUM(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN COALESCE("Margem Produto",0) ELSE 0 END) AS margem_contribuicao,
            SUM(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN "Quantidade Vendida"     ELSE 0 END) AS qtd_liquida,
            ROUND(
              (CASE WHEN SUM(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN "Total Venda" ELSE 0 END) > 0
                THEN SUM(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN COALESCE("Margem Produto",0) ELSE 0 END)
                   / NULLIF(SUM(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN "Total Venda" ELSE 0 END),0) * 100
                ELSE 0 END)::numeric
            , 1) AS margem_pct
          FROM bd_vendas
          WHERE DATE_TRUNC('month', "Data"::date) = DATE_TRUNC('month', MAKE_DATE($1::int, $2::int, 1))
            AND "Sku" IS NOT NULL AND TRIM("Sku"::text) != ''
          GROUP BY "Sku"
          HAVING SUM("Total Venda") > 0
          ORDER BY receita_liquida DESC
        `, [parseInt(ano), parseInt(mes)]),
        pool.query(`
          SELECT SUM(tvp) AS receita_bruta_global
          FROM (
            SELECT "Order ID", MAX("Total Venda Pedido") AS tvp
            FROM bd_vendas
            WHERE DATE_TRUNC('month', "Data"::date) = DATE_TRUNC('month', MAKE_DATE($1::int, $2::int, 1))
            GROUP BY "Order ID"
          ) t
        `, [parseInt(ano), parseInt(mes)])
      ]);
      return res.json({ skus: r.rows, receita_bruta_global: parseFloat(rbRow.rows[0]?.receita_bruta_global) || 0 });
    } catch(e) {
      console.error('[MARGENS]', e.message);
      return res.status(500).json({ error: e.message });
    }
  }

  // Módulo S&OP Config
  if (req.query.module === 'sopc-config') {
    const company = payload.company || 'lanzi';
    const pool = getPool(company);
    try {
      if (req.method === 'GET') {
        await pool.query(`
          CREATE TABLE IF NOT EXISTS sopc_config (
            empresa VARCHAR(50), modulo VARCHAR(50), chave VARCHAR(100), valor TEXT,
            PRIMARY KEY (empresa, modulo, chave)
          )
        `);
        const r = await pool.query(
          'SELECT modulo, chave, valor FROM sopc_config WHERE empresa=$1 ORDER BY modulo, chave',
          [company]
        );
        return res.json({ config: r.rows });
      }
      if (req.method === 'POST') {
        const { modulo, chave, valor } = req.body || {};
        if (!modulo || !chave || valor === undefined) {
          return res.status(400).json({ error: 'modulo, chave e valor são obrigatórios' });
        }
        await pool.query(`
          INSERT INTO sopc_config (empresa, modulo, chave, valor)
          VALUES ($1,$2,$3,$4)
          ON CONFLICT (empresa, modulo, chave) DO UPDATE SET valor = EXCLUDED.valor
        `, [company, modulo, chave, String(valor)]);
        return res.json({ ok: true });
      }
    } catch(e) {
      console.error('[SOPC-CONFIG]', e.message);
      return res.status(500).json({ error: e.message });
    }
  }

  // Módulo Fornecedores Config (lead time por Marca)
  if (req.query.module === 'fornecedores-config') {
    const company = payload.company || 'lanzi';
    const pool = getPool(company);
    try {
      await pool.query(`
        CREATE TABLE IF NOT EXISTS fornecedores_config (
          empresa        VARCHAR(50) NOT NULL,
          marca          TEXT        NOT NULL,
          lead_time_dias INTEGER     NOT NULL DEFAULT 30,
          PRIMARY KEY (empresa, marca)
        )
      `);
      if (req.method === 'GET') {
        const r = await pool.query(`
          SELECT m.marca,
                 f.lead_time_dias
          FROM (
            SELECT DISTINCT "Marca" AS marca FROM cadastros_sku
            WHERE "Marca" IS NOT NULL AND TRIM("Marca") <> ''
          ) m
          LEFT JOIN fornecedores_config f ON f.marca = m.marca AND f.empresa = $1
          ORDER BY m.marca
        `, [company]);
        return res.json({ marcas: r.rows });
      }
      if (req.method === 'POST') {
        const { marca, lead_time_dias } = req.body || {};
        if (!marca || lead_time_dias == null) {
          return res.status(400).json({ error: 'marca e lead_time_dias são obrigatórios' });
        }
        const dias = parseInt(lead_time_dias);
        if (isNaN(dias) || dias < 1) {
          return res.status(400).json({ error: 'lead_time_dias deve ser inteiro >= 1' });
        }
        await pool.query(`
          INSERT INTO fornecedores_config (empresa, marca, lead_time_dias)
          VALUES ($1, $2, $3)
          ON CONFLICT (empresa, marca) DO UPDATE SET lead_time_dias = EXCLUDED.lead_time_dias
        `, [company, marca, dias]);
        return res.json({ ok: true });
      }
    } catch(e) {
      console.error('[FORNECEDORES-CONFIG]', e.message);
      return res.status(500).json({ error: e.message });
    }
  }

  // Módulo SKU Desativadas
  if (req.query.module === 'sku-desativadas') {
    const company = payload.company || 'lanzi';
    const pool = getPool(company);
    try {
      if (req.method === 'GET') {
        const r = await pool.query(
          'SELECT sku FROM sku_desativadas WHERE empresa=$1 ORDER BY sku',
          [company]
        );
        return res.json({ skus: r.rows.map(r => r.sku) });
      }
      if (req.method === 'POST') {
        const { action, sku, skus } = req.body || {};
        if (action === 'toggle') {
          if (!sku) return res.status(400).json({ error: 'sku obrigatorio' });
          const exists = await pool.query(
            'SELECT 1 FROM sku_desativadas WHERE empresa=$1 AND sku=$2', [company, sku]
          );
          if (exists.rowCount > 0) {
            await pool.query('DELETE FROM sku_desativadas WHERE empresa=$1 AND sku=$2', [company, sku]);
            return res.json({ ok: true, active: true });
          } else {
            await pool.query(
              'INSERT INTO sku_desativadas (empresa, sku) VALUES ($1,$2) ON CONFLICT DO NOTHING',
              [company, sku]
            );
            return res.json({ ok: true, active: false });
          }
        }
        if (action === 'deactivate_many') {
          if (!Array.isArray(skus) || !skus.length) return res.status(400).json({ error: 'skus obrigatorio' });
          for (const s of skus) {
            await pool.query(
              'INSERT INTO sku_desativadas (empresa, sku) VALUES ($1,$2) ON CONFLICT DO NOTHING',
              [company, String(s)]
            );
          }
          return res.json({ ok: true, count: skus.length });
        }
        if (action === 'activate_all') {
          await pool.query('DELETE FROM sku_desativadas WHERE empresa=$1', [company]);
          return res.json({ ok: true });
        }
        if (action === 'activate_many') {
          if (!Array.isArray(skus) || !skus.length) return res.status(400).json({ error: 'skus obrigatorio' });
          for (const s of skus) {
            await pool.query('DELETE FROM sku_desativadas WHERE empresa=$1 AND sku=$2', [company, String(s)]);
          }
          return res.json({ ok: true });
        }
        return res.status(400).json({ error: 'action invalida' });
      }
      return res.status(405).json({ error: 'Method not allowed' });
    } catch(e) {
      console.error('[SKU-DESATIVADAS]', e.message);
      return res.status(500).json({ error: e.message });
    }
  }

  // Módulo Sync Vendas
  if (req.query.module === 'sync-vendas') {
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
  if (req.query.module === 'configuracoes') {
    const company = payload.company || 'lanzi';
    const pool = getPool(company);
    try {
      await pool.query(`CREATE TABLE IF NOT EXISTS configuracoes (empresa VARCHAR(50) NOT NULL, chave VARCHAR(100) NOT NULL, valor TEXT, atualizado_em TIMESTAMP DEFAULT NOW(), PRIMARY KEY (empresa, chave))`);
      if (req.method === 'GET') {
        const r = await pool.query(`SELECT chave, valor FROM configuracoes WHERE empresa=$1`, [company]);
        const cfg = {};
        r.rows.forEach(({ chave, valor }) => { cfg[chave] = valor; });
        return res.json({ gefinance_email: cfg['gefinance_email'] || null, gefinance_password_set: !!cfg['gefinance_password'] });
      }
      if (req.method === 'POST') {
        const body = req.body || {};
        const allowed = ['gefinance_email', 'gefinance_password'];
        const updates = Object.entries(body).filter(([k]) => allowed.includes(k));
        if (!updates.length) return res.status(400).json({ error: 'Nenhum campo válido enviado.' });
        for (const [chave, valor] of updates) {
          await pool.query(`INSERT INTO configuracoes (empresa, chave, valor, atualizado_em) VALUES ($1,$2,$3,NOW()) ON CONFLICT (empresa, chave) DO UPDATE SET valor=EXCLUDED.valor, atualizado_em=NOW()`, [company, chave, String(valor)]);
        }
        return res.json({ ok: true, saved: updates.map(([k]) => k) });
      }
      return res.status(405).json({ error: 'Method not allowed' });
    } catch(e) { console.error('[CONFIGURACOES]', e.message); return res.status(500).json({ error: e.message }); }
  }

  // Módulo Vendas
  if (req.query.module === 'vendas') {
    const company = payload.company || 'lanzi';
    const pool = getPool(company);
    const { action } = req.query;
    try {
      await pool.query(`
        CREATE TABLE IF NOT EXISTS vendas_canais_config (
          empresa        VARCHAR(50) NOT NULL,
          canal          TEXT        NOT NULL,
          lead_time_dias INTEGER     NOT NULL DEFAULT 3,
          PRIMARY KEY (empresa, canal)
        )
      `);
      await pool.query(`
        CREATE TABLE IF NOT EXISTS vendas_previsao (
          empresa       VARCHAR(50)   NOT NULL,
          ano           INTEGER       NOT NULL,
          mes           INTEGER       NOT NULL,
          canal         TEXT          NOT NULL,
          valor         NUMERIC(18,2) NOT NULL DEFAULT 0,
          atualizado_em TIMESTAMP     DEFAULT NOW(),
          PRIMARY KEY (empresa, ano, mes, canal)
        )
      `);
      await pool.query(`
        CREATE TABLE IF NOT EXISTS vendas_grupos_canais (
          empresa VARCHAR(50) NOT NULL,
          grupo   TEXT        NOT NULL,
          canal   TEXT        NOT NULL,
          PRIMARY KEY (empresa, grupo, canal)
        )
      `);

      if (action === 'canais') {
        const r = await pool.query(`
          SELECT c.canal, COALESCE(v.lead_time_dias, 3) AS lead_time_dias
          FROM (
            SELECT DISTINCT COALESCE(NULLIF(TRIM("Canal Apelido"::text), ''), TRIM("Canal de venda"), 'Sem canal') AS canal
            FROM bd_vendas
            WHERE "Canal de venda" IS NOT NULL AND TRIM("Canal de venda") != ''
          ) c
          LEFT JOIN vendas_canais_config v ON v.canal = c.canal AND v.empresa = $1
          ORDER BY c.canal
        `, [company]);
        return res.json({ canais: r.rows });
      }

      if (action === 'lead_time' && req.method === 'POST') {
        const { canal, lead_time_dias } = req.body || {};
        if (!canal || lead_time_dias == null) return res.status(400).json({ error: 'canal e lead_time_dias são obrigatórios' });
        const dias = parseInt(lead_time_dias);
        if (isNaN(dias) || dias < 0) return res.status(400).json({ error: 'lead_time_dias deve ser inteiro >= 0' });
        await pool.query(`
          INSERT INTO vendas_canais_config (empresa, canal, lead_time_dias)
          VALUES ($1, $2, $3)
          ON CONFLICT (empresa, canal) DO UPDATE SET lead_time_dias = EXCLUDED.lead_time_dias
        `, [company, canal, dias]);
        return res.json({ ok: true });
      }

      if (action === 'realizadas') {
        const meses = Math.min(parseInt(req.query.meses) || 3, 36);
        const params = [company, meses];
        const r = await pool.query(`
          SELECT
            (bv."Data"::date + COALESCE(v.lead_time_dias, 3)) AS vencimento_data,
            COALESCE(NULLIF(TRIM(bv."Canal Apelido"::text), ''), TRIM(bv."Canal de venda"), 'Sem canal') AS canal,
            ROUND(SUM(COALESCE(bv."Repasse Financeiro"::numeric, 0)), 2) AS repasse,
            COUNT(DISTINCT bv."Order ID") AS qtd
          FROM bd_vendas bv
          LEFT JOIN vendas_canais_config v
            ON v.canal = COALESCE(NULLIF(TRIM(bv."Canal Apelido"::text), ''), TRIM(bv."Canal de venda"), 'Sem canal')
            AND v.empresa = $1
          WHERE bv."Data"::date >= CURRENT_DATE - ($2 || ' months')::interval
            AND bv."Status" !~* '(cancel|devol|n[aã]o.?pago)'
            AND bv."Data" IS NOT NULL
            AND (bv."Data"::date + COALESCE(v.lead_time_dias, 3)) >= CURRENT_DATE
          GROUP BY 1, COALESCE(NULLIF(TRIM(bv."Canal Apelido"::text), ''), TRIM(bv."Canal de venda"), 'Sem canal')
          ORDER BY 1 ASC, repasse DESC
        `, params);
        return res.json({ rows: r.rows });
      }

      if (action === 'previsao' && req.method === 'GET') {
        const r = await pool.query(`
          SELECT ano, mes, canal, valor FROM vendas_previsao
          WHERE empresa = $1
          ORDER BY ano DESC, mes DESC, canal
        `, [company]);
        return res.json({ rows: r.rows });
      }

      if (action === 'previsao' && req.method === 'POST') {
        const { ano, mes, canal, valor } = req.body || {};
        if (!ano || !mes || !canal || valor == null) return res.status(400).json({ error: 'ano, mes, canal e valor são obrigatórios' });
        await pool.query(`
          INSERT INTO vendas_previsao (empresa, ano, mes, canal, valor, atualizado_em)
          VALUES ($1, $2, $3, $4, $5, NOW())
          ON CONFLICT (empresa, ano, mes, canal) DO UPDATE SET valor = EXCLUDED.valor, atualizado_em = NOW()
        `, [company, parseInt(ano), parseInt(mes), canal, parseFloat(valor)]);
        return res.json({ ok: true });
      }

      if (action === 'grupos') {
        if (req.method === 'GET') {
          const r = await pool.query(`
            SELECT grupo, canal FROM vendas_grupos_canais
            WHERE empresa = $1
            ORDER BY grupo, canal
          `, [company]);
          const grupos = {};
          r.rows.forEach(({ grupo, canal }) => {
            if (!grupos[grupo]) grupos[grupo] = [];
            grupos[grupo].push(canal);
          });
          return res.json({ grupos });
        }
        if (req.method === 'POST') {
          const { grupo, canal, action: subAction, grupo_old, grupo_new } = req.body || {};
          if (subAction === 'rename') {
            if (!grupo_old || !grupo_new) return res.status(400).json({ error: 'grupo_old e grupo_new são obrigatórios' });
            await pool.query(
              `UPDATE vendas_grupos_canais SET grupo = $1 WHERE empresa = $2 AND grupo = $3`,
              [grupo_new.trim(), company, grupo_old]
            );
            return res.json({ ok: true });
          }
          if (!grupo || !canal) return res.status(400).json({ error: 'grupo e canal são obrigatórios' });
          await pool.query(
            `INSERT INTO vendas_grupos_canais (empresa, grupo, canal) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`,
            [company, grupo.trim(), canal.trim()]
          );
          return res.json({ ok: true });
        }
        if (req.method === 'DELETE') {
          const { grupo, canal } = req.body || {};
          if (!grupo) return res.status(400).json({ error: 'grupo é obrigatório' });
          if (canal) {
            await pool.query(
              `DELETE FROM vendas_grupos_canais WHERE empresa = $1 AND grupo = $2 AND canal = $3`,
              [company, grupo, canal]
            );
          } else {
            await pool.query(
              `DELETE FROM vendas_grupos_canais WHERE empresa = $1 AND grupo = $2`,
              [company, grupo]
            );
          }
          return res.json({ ok: true });
        }
      }

      return res.status(400).json({ error: 'action inválida' });
    } catch(e) {
      console.error('[VENDAS]', e.message);
      return res.status(500).json({ error: e.message });
    }
  }

  // ── Módulo Forecast por Canal ──────────────────────────────────────────
  if (req.query.module === 'forecast-canais') {
    const company = payload.empresa || payload.company || 'lanzi';
    const pool = getPool(company);
    try {
      // Detectar colunas de forecast_12m
      const colsR = await pool.query(
        `SELECT column_name FROM information_schema.columns WHERE table_name='forecast_12m' ORDER BY ordinal_position`
      );
      if (!colsR.rows.length) return res.json({ meses: [], por_sku: [], por_canal: [] });

      const colMap = {};
      colsR.rows.forEach(c => { colMap[c.column_name.toLowerCase()] = c.column_name; });
      const skuCol  = colMap['sku']  || colMap[Object.keys(colMap).find(k => k.includes('sku'))];
      const dataCol = colMap['data'] || colMap[Object.keys(colMap).find(k => k.includes('data'))];
      const qtdCol  = colMap['prev_qtd'] || colMap['previsao_quantidade']
                   || colMap[Object.keys(colMap).find(k => k.includes('qtd') || k.includes('quant') || k.includes('prev'))];

      if (!skuCol || !dataCol || !qtdCol) {
        return res.json({ meses: [], por_sku: [], por_canal: [], warn: 'Colunas nao encontradas em forecast_12m' });
      }

      // Forecast agrupado por SKU × mês
      const fcR = await pool.query(`
        SELECT
          TRIM("${skuCol}"::text) AS sku,
          TO_CHAR("${dataCol}"::date, 'YYYY-MM') AS mes,
          SUM(COALESCE("${qtdCol}"::numeric, 0)) AS prev_qtd
        FROM forecast_12m
        WHERE "${skuCol}" IS NOT NULL AND TRIM("${skuCol}"::text) != ''
        GROUP BY 1, 2
        ORDER BY 2, 1
      `);

      // Mix de canais por SKU — últimos 6 meses de bd_vendas
      const mixR = await pool.query(`
        SELECT
          TRIM("Sku"::text) AS sku,
          COALESCE(NULLIF(TRIM("Canal Apelido"::text),''), TRIM("Canal de venda"), 'Sem canal') AS canal,
          SUM(COALESCE("Quantidade Vendida"::numeric, 0)) AS qtd
        FROM bd_vendas
        WHERE "Data" >= CURRENT_DATE - INTERVAL '6 months'
          AND "Status" !~* '(cancel|devol|n[aã]o.?pago)'
          AND "Sku" IS NOT NULL AND TRIM("Sku"::text) != ''
        GROUP BY 1, 2
      `);

      // Monta canal-mix % por SKU
      const skuQtd = {};
      mixR.rows.forEach(({ sku, canal, qtd }) => {
        if (!skuQtd[sku]) skuQtd[sku] = {};
        skuQtd[sku][canal] = (skuQtd[sku][canal] || 0) + parseFloat(qtd || 0);
      });
      const skuMix = {};
      Object.entries(skuQtd).forEach(([sku, canais]) => {
        const total = Object.values(canais).reduce((a, b) => a + b, 0);
        if (!total) return;
        skuMix[sku] = {};
        Object.entries(canais).forEach(([c, q]) => { skuMix[sku][c] = q / total; });
      });

      // Preço médio e repasse médio por unidade (de bd_vendas)
      const precosR = await pool.query(`
        SELECT
          TRIM("Sku"::text) AS sku,
          ROUND(AVG("Total Venda"         / NULLIF("Quantidade Vendida", 0))::numeric, 2) AS preco_medio_und,
          ROUND(AVG("Repasse Financeiro"  / NULLIF("Quantidade Vendida", 0))::numeric, 2) AS repasse_medio_und
        FROM bd_vendas
        WHERE "Status" !~* '(cancel|devol|n[ãa]o.?pago)'
          AND "Quantidade Vendida" > 0
          AND "Sku" IS NOT NULL AND TRIM("Sku"::text) != ''
        GROUP BY 1
      `);
      const skuPrecos = {};
      precosR.rows.forEach(({ sku, preco_medio_und, repasse_medio_und }) => {
        skuPrecos[sku] = { preco: parseFloat(preco_medio_und || 0), repasse: parseFloat(repasse_medio_und || 0) };
      });

      // Monta por_sku
      const skuMesMap = {};
      fcR.rows.forEach(({ sku, mes, prev_qtd }) => {
        if (!skuMesMap[sku]) skuMesMap[sku] = {};
        skuMesMap[sku][mes] = (skuMesMap[sku][mes] || 0) + parseFloat(prev_qtd || 0);
      });
      const meses = [...new Set(fcR.rows.map(r => r.mes))].sort();
      const por_sku = Object.entries(skuMesMap)
        .map(([sku, mesesData]) => ({
          sku,
          meses: mesesData,
          total: Object.values(mesesData).reduce((a, b) => a + b, 0),
          preco_medio_und:   skuPrecos[sku]?.preco   || 0,
          repasse_medio_und: skuPrecos[sku]?.repasse || 0,
        }))
        .sort((a, b) => b.total - a.total);

      // Monta por_canal usando mix histórico
      const canalMesMap = {};
      Object.entries(skuMesMap).forEach(([sku, mesesData]) => {
        const mix = skuMix[sku];
        if (!mix || !Object.keys(mix).length) {
          const c = 'Sem histórico';
          if (!canalMesMap[c]) canalMesMap[c] = {};
          Object.entries(mesesData).forEach(([m, q]) => { canalMesMap[c][m] = (canalMesMap[c][m] || 0) + q; });
          return;
        }
        Object.entries(mix).forEach(([canal, pct]) => {
          if (!canalMesMap[canal]) canalMesMap[canal] = {};
          Object.entries(mesesData).forEach(([m, q]) => { canalMesMap[canal][m] = (canalMesMap[canal][m] || 0) + q * pct; });
        });
      });
      const por_canal = Object.entries(canalMesMap)
        .map(([canal, mesesData]) => ({ canal, meses: mesesData, total: Object.values(mesesData).reduce((a, b) => a + b, 0) }))
        .sort((a, b) => b.total - a.total);

      return res.json({ meses, por_sku, por_canal });
    } catch(e) {
      console.error('[FORECAST-CANAIS]', e.message);
      return res.status(500).json({ error: e.message });
    }
  }

  // ── Módulo Forecast Diário ─────────────────────────────────────────────
  if (req.query.module === 'forecast-diario') {
    const company = payload.empresa || payload.company || 'lanzi';
    const pool = getPool(company);
    const mes = req.query.mes || '';
    try {
      // Lista de meses disponíveis
      const mesesR = await pool.query(`
        SELECT DISTINCT TO_CHAR(data::date, 'YYYY-MM') AS mes
        FROM forecast_diario
        ORDER BY 1
      `);
      const mesesDisp = mesesR.rows.map(r => r.mes);
      if (!mesesDisp.length) return res.json({ meses: [], por_sku: [], dias: [] });

      const mesSel = mesesDisp.includes(mes) ? mes : mesesDisp[0];
      const [ano, mo] = mesSel.split('-').map(Number);
      const nDias = new Date(ano, mo, 0).getDate();
      const dias = Array.from({ length: nDias }, (_, i) => i + 1);

      const r = await pool.query(`
        SELECT
          TRIM(sku::text) AS sku,
          EXTRACT(DAY FROM data::date)::int AS dia,
          SUM(quantidade_prevista) AS qtd
        FROM forecast_diario
        WHERE TO_CHAR(data::date, 'YYYY-MM') = $1
          AND sku IS NOT NULL AND TRIM(sku::text) != ''
        GROUP BY 1, 2
        ORDER BY 1, 2
      `, [mesSel]);

      const skuMap = {};
      r.rows.forEach(({ sku, dia, qtd }) => {
        if (!skuMap[sku]) skuMap[sku] = {};
        skuMap[sku][dia] = (skuMap[sku][dia] || 0) + parseFloat(qtd || 0);
      });
      const por_sku = Object.entries(skuMap)
        .map(([sku, diasData]) => ({
          sku,
          dias: diasData,
          total: Object.values(diasData).reduce((a, b) => a + b, 0)
        }))
        .sort((a, b) => b.total - a.total);

      return res.json({ meses: mesesDisp, mes: mesSel, dias, por_sku });
    } catch(e) {
      console.error('[FORECAST-DIARIO]', e.message);
      return res.status(500).json({ error: e.message });
    }
  }

  // 2. Verificar tabela (whitelist)
  const { tabela } = req.query;
  if (!tabela || !TABELAS_PERMITIDAS.includes(tabela))
    return res.status(400).json({ error: `Tabela '${tabela}' não permitida.` });

  // 3. Query no banco da empresa correta
  const pool = getPool(payload.company);

  try {
    let result;
    if (tabela === 'sopc') {
      const [ppRes, esRes, canalRes, full1Map, full2Map] = await Promise.all([
        pool.query(`SELECT sku, COALESCE(estoque_atual::numeric,0) AS estoque_atual, COALESCE(ponto_pedido::numeric,0) AS ponto_pedido, COALESCE(alerta,'SEM DADOS') AS alerta FROM ponto_pedido`),
        pool.query(`SELECT sku, REPLACE(media_mensal::text,',','.')::numeric AS media_mensal FROM estoque_seguranca`),
        pool.query(`SELECT "Sku" AS sku, TRIM("Canal de venda") AS canal, ROUND(SUM("Quantidade Vendida"::numeric)/3.0,1) AS media FROM bd_vendas WHERE "Status" !~* '(cancel|devol|n[aã]o.?pago)' AND "Data"::date >= (SELECT MAX("Data"::date) FROM bd_vendas) - INTERVAL '3 months' AND "Sku" IS NOT NULL AND TRIM("Canal de venda") IS NOT NULL AND TRIM("Canal de venda") != '' GROUP BY "Sku", TRIM("Canal de venda")`).catch(()=>({rows:[]})),
        lerEstoqueFullMap(pool,'full_1'),
        lerEstoqueFullMap(pool,'full_2'),
      ]);
      const mediaMap={};
      esRes.rows.forEach(r=>{mediaMap[String(r.sku||'').trim()]=parseFloat(r.media_mensal)||0;});
      const canalMap={};
      const canaisSet=new Set();
      canalRes.rows.forEach(r=>{
        const s=String(r.sku||'').trim(); const c=String(r.canal||'').trim();
        if(!s||!c) return;
        if(!canalMap[s]) canalMap[s]={};
        canalMap[s][c]=(canalMap[s][c]||0)+(parseFloat(r.media)||0);
        canaisSet.add(c);
      });
      const fullMap={};
      const allFull=new Set([...Object.keys(full1Map),...Object.keys(full2Map)]);
      allFull.forEach(s=>{fullMap[s]=(full1Map[s]||0)+(full2Map[s]||0);});

      // Origens de estoque via estoque_consolidado
      const origenMap={};
      const origensSet=new Set();
      try {
        const ecCols = await getTableColumns(pool,'estoque_consolidado');
        const skuCol  = ecCols.find(c=>c==='SKU')||ecCols.find(c=>c.toLowerCase()==='sku')||'SKU';
        const oriCol  = ecCols.find(c=>c.toLowerCase()==='origem')||'Origem';
        const qtdCol  = ecCols.find(c=>c.toLowerCase().includes('estoque'))||'Estoque Base';
        const origenRes = await pool.query(
          `SELECT "${skuCol}" AS sku, TRIM("${oriCol}") AS origem, SUM("${qtdCol}"::numeric) AS qtd FROM estoque_consolidado WHERE "${skuCol}" IS NOT NULL AND TRIM("${skuCol}"::text)!='' AND "${oriCol}" IS NOT NULL AND TRIM("${oriCol}"::text)!='' GROUP BY "${skuCol}", TRIM("${oriCol}")`
        );
        origenRes.rows.forEach(r=>{
          const s=String(r.sku||'').trim(); const o=String(r.origem||'').trim();
          if(!s||!o) return;
          if(!origenMap[s]) origenMap[s]={};
          origenMap[s][o]=(origenMap[s][o]||0)+(parseFloat(r.qtd)||0);
          origensSet.add(o);
        });
      } catch(e){ console.error('[SOPC] origens:',e.message); }

      const rows=ppRes.rows.map(r=>{
        const sku=String(r.sku||'').trim();
        return {sku, alerta_pp:r.alerta, estoque_base:parseFloat(r.estoque_atual)||0, estoque_full:fullMap[sku]||0, media_mensal:mediaMap[sku]||0, ponto_pedido:parseFloat(r.ponto_pedido)||0, canais:canalMap[sku]||{}, origens:origenMap[sku]||{}};
      });
      return res.json({rows, canais_disponiveis:[...canaisSet].sort(), origens_disponiveis:[...origensSet].sort()});
    }
    if (tabela === 'sku_atividade') {
      result = await pool.query(`
        WITH max_d AS (SELECT MAX("Data"::date) AS d FROM bd_vendas)
        SELECT
          "Sku" AS sku,
          ROUND(SUM(CASE WHEN "Data"::date >= (SELECT d FROM max_d) - INTERVAL '1 month'  THEN "Quantidade Vendida"::numeric ELSE 0 END),0) AS qtd_1m,
          ROUND(SUM(CASE WHEN "Data"::date >= (SELECT d FROM max_d) - INTERVAL '3 months' THEN "Quantidade Vendida"::numeric ELSE 0 END),0) AS qtd_3m,
          ROUND(SUM(CASE WHEN "Data"::date >= (SELECT d FROM max_d) - INTERVAL '6 months' THEN "Quantidade Vendida"::numeric ELSE 0 END),0) AS qtd_6m,
          ROUND(SUM("Quantidade Vendida"::numeric),0) AS qtd_12m
        FROM bd_vendas
        WHERE "Status" !~* '(cancel|devol|n[aã]o.?pago)'
          AND "Sku" IS NOT NULL AND TRIM("Sku"::text) != ''
          AND "Data"::date >= (SELECT d FROM max_d) - INTERVAL '12 months'
        GROUP BY "Sku"
        ORDER BY qtd_12m ASC
      `);
      return res.json(result.rows);
    }
    if (tabela === 'sku_discontinued') {
      result = await pool.query(`
        SELECT DISTINCT "Sku" AS sku
        FROM cadastros_sku
        WHERE "Sku" NOT IN (
          SELECT DISTINCT "Sku"
          FROM bd_vendas
          WHERE "Data" >= CURRENT_DATE - INTERVAL '6 months'
        )
      `);
      return res.json(result.rows.map(r => r.sku));
    }
    if (tabela === 'dashboard_kpis') {
      result = await pool.query(`
          WITH lm AS (
            SELECT DATE_TRUNC('month', MAX("Data"::date)) AS m
            FROM bd_vendas WHERE "Data" IS NOT NULL
          ),
          rb AS (
            SELECT SUM(tvp) AS receita_bruta
            FROM (
              SELECT "Order ID", MAX("Total Venda Pedido") AS tvp
              FROM bd_vendas
              WHERE DATE_TRUNC('month', "Data"::date) = (SELECT m FROM lm)
              GROUP BY "Order ID"
            ) t
          )
          SELECT
            EXTRACT(YEAR  FROM "Data"::date)  AS ano,
            EXTRACT(MONTH FROM "Data"::date)  AS mes,
            (SELECT receita_bruta FROM rb)    AS receita_bruta,
            SUM(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN "Total Venda"            ELSE 0 END) AS receita_liquida,
            SUM(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN "Quantidade Vendida"     ELSE 0 END) AS qtd_liquida,
            SUM(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN COALESCE("Margem Produto", 0) ELSE 0 END) AS margem_bruta,
            SUM(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN COALESCE("Custo Total", 0) ELSE 0 END)  AS custo_total
          FROM bd_vendas
          WHERE DATE_TRUNC('month', "Data"::date) = (SELECT m FROM lm)
          GROUP BY 1, 2
        `);
      return res.json(result.rows[0] || {});
    }
    if (tabela === 'contas_pagar') {
      result = await pool.query(`
        SELECT id, situacao, token_origem, numero_doc, historico, fornecedor,
               valor, saldo, data_vencimento, data_emissao, atualizado_em, data_calculo
        FROM contas_pagar
        ORDER BY data_vencimento ASC NULLS LAST, id ASC
      `);
      return res.json(result.rows);
    }
    if (tabela === 'monthly_revenue') {
      result = await pool.query(`
        SELECT "Ano" AS ano, "Mes" AS mes,
               SUM("Total Venda") AS receita,
               SUM("Quantidade Vendida") AS qtd,
               CASE WHEN SUM(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN "Total Venda" ELSE 0 END) > 0
                    THEN ROUND((
                      SUM(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN COALESCE("Margem Produto", 0) ELSE 0 END) /
                      SUM(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN "Total Venda" ELSE 0 END) * 100
                    )::numeric, 2)
                    ELSE NULL END AS mc_pct
        FROM bd_vendas
        GROUP BY "Ano", "Mes"
        ORDER BY "Ano" ASC, "Mes" ASC
      `);
      return res.json(result.rows);
    }
    if (tabela === 'pmv_months') {
      result = await pool.query(`
        SELECT DISTINCT "Ano" AS ano, "Mes" AS mes
        FROM bd_vendas
        WHERE "Ano" IS NOT NULL AND "Mes" IS NOT NULL
        ORDER BY "Ano" DESC, "Mes" DESC
      `);
      return res.json(result.rows);
    }
    if (tabela === 'pmv') {
      const { mes_prev, ano_prev, mes_curr, ano_curr, dia_ini_prev, dia_fim_prev, dia_ini_curr, dia_fim_curr } = req.query;
      if (!mes_prev || !ano_prev || !mes_curr || !ano_curr)
        return res.status(400).json({ error: 'Parâmetros mes_prev, ano_prev, mes_curr, ano_curr são obrigatórios.' });
      const pad = n => String(n).padStart(2, '0');
      const lastDay = (y, m) => new Date(+y, +m, 0).getDate();
      const dIniPrev = dia_ini_prev ? +dia_ini_prev : 1;
      const dFimPrev = dia_fim_prev ? +dia_fim_prev : lastDay(ano_prev, mes_prev);
      const dIniCurr = dia_ini_curr ? +dia_ini_curr : 1;
      const dFimCurr = dia_fim_curr ? +dia_fim_curr : lastDay(ano_curr, mes_curr);
      const datePrevIni = `${ano_prev}-${pad(mes_prev)}-${pad(dIniPrev)}`;
      const datePrevFim = `${ano_prev}-${pad(mes_prev)}-${pad(dFimPrev)}`;
      const dateCurrIni = `${ano_curr}-${pad(mes_curr)}-${pad(dIniCurr)}`;
      const dateCurrFim = `${ano_curr}-${pad(mes_curr)}-${pad(dFimCurr)}`;
      result = await pool.query(`
        SELECT
          "Sku" AS sku,
          MAX("Nome Produto") AS nome_produto,
          MAX("Categoria") AS categoria,
          SUM(CASE WHEN "Data"::date BETWEEN $1::date AND $2::date THEN "Quantidade Vendida" ELSE 0 END) AS qtd_prev,
          SUM(CASE WHEN "Data"::date BETWEEN $1::date AND $2::date THEN "Total Venda" ELSE 0 END) AS rev_prev,
          SUM(CASE WHEN "Data"::date BETWEEN $1::date AND $2::date THEN COALESCE("Margem Produto",0) ELSE 0 END) AS mar_prev,
          SUM(CASE WHEN "Data"::date BETWEEN $3::date AND $4::date THEN "Quantidade Vendida" ELSE 0 END) AS qtd_curr,
          SUM(CASE WHEN "Data"::date BETWEEN $3::date AND $4::date THEN "Total Venda" ELSE 0 END) AS rev_curr,
          SUM(CASE WHEN "Data"::date BETWEEN $3::date AND $4::date THEN COALESCE("Margem Produto",0) ELSE 0 END) AS mar_curr
        FROM bd_vendas
        WHERE "Status" !~* '(cancel|devol|n[aã]o.?pago)'
          AND (
            "Data"::date BETWEEN $1::date AND $2::date OR
            "Data"::date BETWEEN $3::date AND $4::date
          )
        GROUP BY "Sku"
        HAVING SUM("Quantidade Vendida") > 0
        ORDER BY SUM(CASE WHEN "Data"::date BETWEEN $3::date AND $4::date THEN "Total Venda" ELSE 0 END) DESC
      `, [datePrevIni, datePrevFim, dateCurrIni, dateCurrFim]);
      return res.json(result.rows);
    }
    if (tabela === 'pmv_canais') {
      const { mes_prev, ano_prev, mes_curr, ano_curr, dia_ini_prev, dia_fim_prev, dia_ini_curr, dia_fim_curr, sku } = req.query;
      if (!mes_prev || !ano_prev || !mes_curr || !ano_curr || !sku)
        return res.status(400).json({ error: 'Parâmetros obrigatórios ausentes.' });
      const pad = n => String(n).padStart(2, '0');
      const lastDay = (y, m) => new Date(+y, +m, 0).getDate();
      const dIniPrev = dia_ini_prev ? +dia_ini_prev : 1;
      const dFimPrev = dia_fim_prev ? +dia_fim_prev : lastDay(ano_prev, mes_prev);
      const dIniCurr = dia_ini_curr ? +dia_ini_curr : 1;
      const dFimCurr = dia_fim_curr ? +dia_fim_curr : lastDay(ano_curr, mes_curr);
      const datePrevIni = `${ano_prev}-${pad(mes_prev)}-${pad(dIniPrev)}`;
      const datePrevFim = `${ano_prev}-${pad(mes_prev)}-${pad(dFimPrev)}`;
      const dateCurrIni = `${ano_curr}-${pad(mes_curr)}-${pad(dIniCurr)}`;
      const dateCurrFim = `${ano_curr}-${pad(mes_curr)}-${pad(dFimCurr)}`;
      result = await pool.query(`
        SELECT
          COALESCE(NULLIF(TRIM("Canal Apelido"::text), ''), TRIM("Canal de venda"), 'Sem canal') AS canal,
          SUM(CASE WHEN "Data"::date BETWEEN $1::date AND $2::date THEN "Quantidade Vendida" ELSE 0 END) AS qtd_prev,
          SUM(CASE WHEN "Data"::date BETWEEN $1::date AND $2::date THEN "Total Venda" ELSE 0 END) AS rev_prev,
          SUM(CASE WHEN "Data"::date BETWEEN $1::date AND $2::date THEN COALESCE("Margem Produto",0) ELSE 0 END) AS mar_prev,
          SUM(CASE WHEN "Data"::date BETWEEN $3::date AND $4::date THEN "Quantidade Vendida" ELSE 0 END) AS qtd_curr,
          SUM(CASE WHEN "Data"::date BETWEEN $3::date AND $4::date THEN "Total Venda" ELSE 0 END) AS rev_curr,
          SUM(CASE WHEN "Data"::date BETWEEN $3::date AND $4::date THEN COALESCE("Margem Produto",0) ELSE 0 END) AS mar_curr
        FROM bd_vendas
        WHERE "Status" !~* '(cancel|devol|n[aã]o.?pago)'
          AND "Sku" = $5
          AND (
            "Data"::date BETWEEN $1::date AND $2::date OR
            "Data"::date BETWEEN $3::date AND $4::date
          )
        GROUP BY COALESCE(NULLIF(TRIM("Canal Apelido"::text), ''), TRIM("Canal de venda"), 'Sem canal')
        HAVING SUM("Quantidade Vendida") > 0
        ORDER BY SUM(CASE WHEN "Data"::date BETWEEN $3::date AND $4::date THEN "Total Venda" ELSE 0 END) DESC
      `, [datePrevIni, datePrevFim, dateCurrIni, dateCurrFim, sku]);
      return res.json(result.rows);
    }
    if (tabela === 'curva_abc' || tabela === 'ponto_pedido') {
      try {
        result = await pool.query(`
          SELECT * FROM ${tabela}
          WHERE "Ano" = (SELECT MAX("Ano") FROM ${tabela})
            AND "Mês" = (
              SELECT MAX("Mês") FROM ${tabela}
              WHERE "Ano" = (SELECT MAX("Ano") FROM ${tabela})
            )
          LIMIT 5000
        `);
        return res.json(result.rows);
      } catch(_) {
        // Tabelas sem coluna Ano/Mês – usa query genérica
      }
    }
    if (tabela === 'categoria_vendas') {
      const { ano, mes } = req.query;
      let whereClause = `WHERE "Categoria" IS NOT NULL AND TRIM("Categoria"::text) != '' AND "Status" !~* '(cancel|devol|n[aã]o.?pago)'`;
      const catParams = [];
      if (ano && mes) {
        whereClause += ` AND "Ano" = $1 AND "Mes" = $2`;
        catParams.push(parseInt(ano), parseInt(mes));
      } else {
        whereClause += ` AND "Ano" = (SELECT MAX("Ano") FROM bd_vendas) AND "Mes" = (SELECT MAX("Mes") FROM bd_vendas WHERE "Ano" = (SELECT MAX("Ano") FROM bd_vendas))`;
      }
      result = await pool.query(`
        SELECT
          "Categoria"                                                              AS categoria,
          ROUND(SUM("Total Venda"), 2)                                             AS receita,
          ROUND(SUM(COALESCE("Margem Produto", 0)), 2)                             AS margem,
          ROUND(SUM(COALESCE("Custo Total", 0)), 2)                                AS custo,
          SUM("Quantidade Vendida")                                                AS qtd,
          COUNT(DISTINCT "Sku")                                                    AS skus
        FROM bd_vendas
        ${whereClause}
        GROUP BY "Categoria"
        ORDER BY receita DESC
      `, catParams);
      return res.json(result.rows);
    }
    result = await pool.query(`SELECT * FROM ${tabela} LIMIT 5000`);
    res.json(result.rows);
  } catch (e) {
    console.error(`[ERRO] ${payload.company} / ${tabela}:`, e.message);
    res.status(500).json({ error: e.message });
  }
};
