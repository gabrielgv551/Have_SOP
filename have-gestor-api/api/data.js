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
  'dashboard_filters',
];

const CANAL_COL = `COALESCE(NULLIF(TRIM("Canal Apelido"::text), ''), TRIM("Canal de venda"::text))`;
const CANAL_GRUPO_SQL = `CASE
  WHEN ${CANAL_COL} ILIKE '%amazon%' THEN 'Amazon'
  WHEN TRIM("Canal de venda"::text) ILIKE 'ml full%' THEN TRIM("Canal de venda"::text)
  WHEN ${CANAL_COL} ILIKE '%mercado livre%' OR ${CANAL_COL} ILIKE 'melibr%' THEN 'Mercado Livre'
  WHEN ${CANAL_COL} ILIKE '%shopee%' THEN 'Shopee'
  WHEN ${CANAL_COL} ILIKE '%magalu%' THEN 'Magalu'
  WHEN ${CANAL_COL} ILIKE '%tiktok%' THEN 'TikTok Shop'
  WHEN ${CANAL_COL} ILIKE '%loja integrada%' THEN 'Loja Integrada'
  ELSE ${CANAL_COL}
END`;

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
// ML TOKEN HELPER — lê access_token do DB e renova se expirado
// ──────────────────────────────────────────────────────────────
async function getMlToken(pool, company, accountId) {
  const chave = accountId + '_token';
  const r = await pool.query(
    `SELECT chave, valor FROM configuracoes WHERE empresa=$1 AND chave LIKE $2`,
    [company, accountId + '%']
  );
  const cfg = {};
  r.rows.forEach(({ chave, valor }) => { cfg[chave] = valor; });

  const accessToken  = cfg[chave];
  const refreshToken = cfg[chave + '_refresh'];
  const expAt        = cfg[chave + '_exp'];

  if (!accessToken || !refreshToken) throw new Error(`Conta ${accountId} não autenticada`);

  const expired = expAt ? new Date(expAt).getTime() - 5 * 60 * 1000 < Date.now() : true;
  if (!expired) return accessToken;

  const ML_CLIENT_ID     = process.env.ML_CLIENT_ID     || '2803787506623043';
  const ML_CLIENT_SECRET = process.env.ML_CLIENT_SECRET || 'y7HAmpTr8wWjWwTL55pJiwq3y1MNxCkE';
  const tokenRes = await fetch('https://api.mercadolibre.com/oauth/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Accept': 'application/json' },
    body: JSON.stringify({ grant_type: 'refresh_token', client_id: ML_CLIENT_ID, client_secret: ML_CLIENT_SECRET, refresh_token: refreshToken }),
  });
  const tokenData = await tokenRes.json();
  if (!tokenRes.ok) throw new Error('Falha ao renovar token ML: ' + (tokenData.message || tokenRes.status));

  const newExpAt = new Date(Date.now() + (tokenData.expires_in || 21600) * 1000).toISOString();
  for (const [k, v] of [
    [chave,             tokenData.access_token],
    [chave + '_refresh', tokenData.refresh_token],
    [chave + '_exp',     newExpAt],
  ]) {
    await pool.query(
      `INSERT INTO configuracoes (empresa,chave,valor,atualizado_em) VALUES ($1,$2,$3,NOW())
       ON CONFLICT (empresa,chave) DO UPDATE SET valor=EXCLUDED.valor, atualizado_em=NOW()`,
      [company, k, v]
    );
  }
  return tokenData.access_token;
}
// ──────────────────────────────────────────────────────────────

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
        const [rMeses, rbMeses, rSkus, rSkusMes] = await Promise.all([
          pool.query(`
            SELECT
              "Mes"::int AS mes,
              SUM("Total Venda") AS receita_bruta,
              SUM(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN "Total Venda" ELSE 0 END) AS receita_liquida,
              SUM(CASE WHEN "Status"  ~* '(cancel|devol|n[aã]o.?pago)' THEN "Total Venda" ELSE 0 END) AS devolucoes,
              SUM(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN COALESCE("Margem Produto",0) ELSE 0 END) AS margem_contribuicao,
              SUM(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN COALESCE("Repasse Financeiro"::numeric,0) ELSE 0 END) AS repasse_financeiro,
              SUM(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN COALESCE("Frete Pago Prod"::numeric,0) ELSE 0 END) AS frete_pago_prod,
              SUM(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN COALESCE("Comissao Produto"::numeric,0) ELSE 0 END) AS comissao_produto,
              SUM(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN COALESCE("Imposto Produto"::numeric,0) ELSE 0 END) AS imposto_produto,
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
              SUM(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN COALESCE("Repasse Financeiro"::numeric,0) ELSE 0 END) AS repasse_financeiro,
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
          `, [parseInt(ano)]),
          pool.query(`
            SELECT
              "Sku"            AS sku,
              "Mes"::int       AS mes,
              SUM("Total Venda")                                                                                AS receita_bruta,
              SUM(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN "Total Venda"    ELSE 0 END)      AS receita_liquida,
              SUM(CASE WHEN "Status"  ~* '(cancel|devol|n[aã]o.?pago)' THEN "Total Venda"    ELSE 0 END)      AS devolucoes,
              SUM(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN COALESCE("Margem Produto",0) ELSE 0 END) AS margem_contribuicao,
              SUM(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN COALESCE("Repasse Financeiro"::numeric,0) ELSE 0 END) AS repasse_financeiro,
              SUM(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN COALESCE("Frete Pago Prod"::numeric,0) ELSE 0 END) AS frete_pago_prod,
              SUM(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN COALESCE("Comissao Produto"::numeric,0) ELSE 0 END) AS comissao_produto,
              SUM(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN COALESCE("Imposto Produto"::numeric,0) ELSE 0 END) AS imposto_produto,
              SUM(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN "Quantidade Vendida" ELSE 0 END)  AS qtd_liquida
            FROM bd_vendas
            WHERE "Ano"::int = $1
              AND "Sku" IS NOT NULL AND TRIM("Sku"::text) != ''
            GROUP BY "Sku", "Mes"
            ORDER BY "Mes"::int
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
          repasse_financeiro:  parseFloat(r.repasse_financeiro)  || 0,
          frete_pago_prod:     parseFloat(r.frete_pago_prod)     || 0,
          comissao_produto:    parseFloat(r.comissao_produto)    || 0,
          imposto_produto:     parseFloat(r.imposto_produto)     || 0,
          qtd_liquida:         parseFloat(r.qtd_liquida)         || 0,
        }));
        return res.json({ meses_dre, skus_ano: rSkus.rows, skus_mes: rSkusMes.rows });
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
                 f.lead_time_dias,
                 COALESCE(f.frequencia_tipo, 'mensal') AS frequencia_tipo,
                 COALESCE(f.dia_semana_preferido, 5) AS dia_semana_preferido,
                 COALESCE(f.intervalo_dias, 30) AS intervalo_dias
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
        const { marca, lead_time_dias, frequencia_tipo, dia_semana_preferido, intervalo_dias } = req.body || {};
        if (!marca || lead_time_dias == null) {
          return res.status(400).json({ error: 'marca e lead_time_dias são obrigatórios' });
        }
        const dias = parseInt(lead_time_dias);
        if (isNaN(dias) || dias < 1) {
          return res.status(400).json({ error: 'lead_time_dias deve ser inteiro >= 1' });
        }
        const freqTipo = ['semanal','quinzenal','mensal','custom'].includes(frequencia_tipo) ? frequencia_tipo : 'mensal';
        const diaSem = Math.max(0, Math.min(6, parseInt(dia_semana_preferido) || 5));
        const intervalo = Math.max(1, parseInt(intervalo_dias) || 30);
        await pool.query(`
          INSERT INTO fornecedores_config (empresa, marca, lead_time_dias, frequencia_tipo, dia_semana_preferido, intervalo_dias)
          VALUES ($1, $2, $3, $4, $5, $6)
          ON CONFLICT (empresa, marca) DO UPDATE SET
            lead_time_dias = EXCLUDED.lead_time_dias,
            frequencia_tipo = EXCLUDED.frequencia_tipo,
            dia_semana_preferido = EXCLUDED.dia_semana_preferido,
            intervalo_dias = EXCLUDED.intervalo_dias
        `, [company, marca, dias, freqTipo, diaSem, intervalo]);
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
  if (req.query.module === 'ml-oauth') {
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
      // ── Disparar ETL Worker (await necessário no Vercel serverless) ──
      const ETL_WORKER_URL = process.env.ETL_WORKER_URL;
      const ETL_SECRET     = process.env.ETL_SECRET;
      let etl_debug = { url: ETL_WORKER_URL || null, account_id: state, triggered: false, error: null };
      if (ETL_WORKER_URL) {
        try {
          const etlRes = await fetch(`${ETL_WORKER_URL}/etl/trigger`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ company, account_id: state, secret: ETL_SECRET || '' }),
            signal: AbortSignal.timeout(8000),
          });
          const etlData = await etlRes.json();
          console.log('[ETL Worker] trigger:', etlData);
          etl_debug.triggered = true;
          etl_debug.job_id = etlData.job_id || null;
        } catch(err) {
          console.error('[ETL Worker] trigger failed:', err.message);
          etl_debug.error = err.message;
        }
      }
      return res.json({ ok: true, nick, etl: etl_debug });
    } catch(e) { return res.status(500).json({ error: e.message }); }
  }

  // Módulo ML Remove — desconectar conta ML (apaga tokens + tabela via worker)
  if (req.query.module === 'ml-remove') {
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

      // 2. Notificar worker para dropar a tabela
      const ETL_WORKER_URL = process.env.ETL_WORKER_URL;
      const ETL_SECRET     = process.env.ETL_SECRET;
      let worker_result = { triggered: false };
      if (ETL_WORKER_URL) {
        try {
          const wRes = await fetch(`${ETL_WORKER_URL}/etl/remove-account`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ company, account_id, secret: ETL_SECRET || '' }),
            signal: AbortSignal.timeout(10000),
          });
          worker_result = await wRes.json();
        } catch(e) {
          worker_result = { triggered: false, error: e.message };
        }
      }

      return res.json({
        ok: true,
        chaves_removidas: del.rowCount,
        worker: worker_result,
      });
    } catch(e) { return res.status(500).json({ error: e.message }); }
  }

  // Módulo Tiny OAuth — conectar conta Tiny ERP v3
  if (req.query.module === 'tiny-oauth') {
    if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });
    const _tBody = (typeof req.body === 'string') ? (() => { try { return JSON.parse(req.body); } catch { return {}; } })() : (req.body || {});
    const { code, state, modules } = _tBody;
    if (!code || !state) return res.status(400).json({ error: 'code e state são obrigatórios' });

    const TINY_REDIRECT_URI  = 'https://have-gestor-frontend.vercel.app/tiny-callback';
    const TINY_TOKEN_URL = 'https://accounts.tiny.com.br/realms/tiny/protocol/openid-connect/token';
    const TINY_API_BASE  = 'https://erp.tiny.com.br/public-api/v3';
    // Credenciais por empresa (DB) com fallback para env vars globais
    const _company = payload?.company || 'lanzi';
    const _pool = getPool(_company);
    const _creds = await _pool.query(`SELECT chave, valor FROM configuracoes WHERE empresa=$1 AND chave IN ('tiny_client_id','tiny_client_secret')`, [_company]);
    const _credMap = {}; _creds.rows.forEach(r => { _credMap[r.chave] = r.valor; });
    const TINY_CLIENT_ID     = (_credMap.tiny_client_id     || process.env.TINY_CLIENT_ID     || '').trim();
    const TINY_CLIENT_SECRET = (_credMap.tiny_client_secret || process.env.TINY_CLIENT_SECRET || '').trim();

    if (!TINY_CLIENT_ID) return res.status(500).json({ error: 'TINY_CLIENT_ID não configurado no servidor' });

    try {
      // 1. Trocar code por tokens
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

      // 2. Obter nome da empresa no Tiny
      let nick = state;
      try {
        const uRes = await fetch(`${TINY_API_BASE}/empresas`, {
          headers: { 'Authorization': 'Bearer ' + apiToken },
        });
        const uData = await uRes.json();
        const emp = (uData.itens || uData.data || [])[0];
        if (emp) nick = emp.nomeFantasia || emp.razaoSocial || emp.nome || state;
      } catch {}

      // 3. Salvar tokens em configuracoes
      const company = payload.company || 'lanzi';
      const pool    = getPool(company);
      const expAt   = new Date(Date.now() + (tokenData.expires_in || 21600) * 1000).toISOString();
      const modsStr = modules || 'vendas,estoque,pedidos';
      await pool.query(`CREATE TABLE IF NOT EXISTS configuracoes (empresa VARCHAR(50) NOT NULL, chave VARCHAR(100) NOT NULL, valor TEXT, atualizado_em TIMESTAMP DEFAULT NOW(), PRIMARY KEY (empresa, chave))`);
      for (const [k, v] of [
        [`${state}_token`,   apiToken],
        [`${state}_refresh`, tokenData.refresh_token],
        [`${state}_nick`,    nick],
        [`${state}_exp`,     expAt],
        [`${state}_modulos`, modsStr],
      ]) {
        await pool.query(
          `INSERT INTO configuracoes (empresa,chave,valor,atualizado_em) VALUES ($1,$2,$3,NOW()) ON CONFLICT (empresa,chave) DO UPDATE SET valor=EXCLUDED.valor,atualizado_em=NOW()`,
          [company, k, v]
        );
      }

      // 4. Criar tabelas no banco conforme módulos selecionados
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

      // 5. Disparar ETL Worker
      const ETL_WORKER_URL = process.env.ETL_WORKER_URL;
      const ETL_SECRET     = process.env.ETL_SECRET;
      let etl_debug = { triggered: false, error: null };
      if (ETL_WORKER_URL) {
        try {
          const etlRes = await fetch(`${ETL_WORKER_URL}/etl/trigger-tiny`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ company, account_id: state, modules: modsStr, secret: ETL_SECRET || '' }),
            signal: AbortSignal.timeout(8000),
          });
          const etlData = await etlRes.json();
          etl_debug = { triggered: true, job_id: etlData.job_id || null };
        } catch(err) {
          etl_debug = { triggered: false, error: err.message };
        }
      }

      return res.json({ ok: true, nick, modulos: mods, etl: etl_debug });
    } catch(e) { return res.status(500).json({ error: e.message }); }
  }

  // Módulo Tiny Debug — inspeciona resposta bruta da Tiny API
  if (req.query.module === 'tiny-debug') {
    const account = req.query.account;
    if (!account) return res.status(400).json({ error: 'account obrigatório' });
    const company = payload.company || 'lanzi';
    const pool = getPool(company);
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

      // Tenta refresh do token
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
            await pool.query(`INSERT INTO configuracoes (empresa,chave,valor,atualizado_em) VALUES ($1,$2,$3,NOW()) ON CONFLICT (empresa,chave) DO UPDATE SET valor=EXCLUDED.valor,atualizado_em=NOW()`, [company, k, v]);
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
      // Decodifica JWT para inspecionar scope/audience (sem validar assinatura)
      let tokenClaims = null;
      try {
        const parts = accessToken.split('.');
        if (parts.length === 3) tokenClaims = JSON.parse(Buffer.from(parts[1], 'base64').toString('utf8'));
      } catch {}

      // Testa client_credentials flow
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

      // Testa conta/info (endpoint mais simples)
      const ccFetch = ccToken ? await tinyFetch(`${TINY_API}/conta/info`, ccToken) : null;

      // Testa múltiplos formatos de auth
      async function tinyFetchFull(url, tok, scheme) {
        const r = await fetch(url, { headers: { 'Authorization': (scheme||'Bearer') + ' ' + (tok||accessToken) } });
        const text = await r.text();
        let body; try { body = JSON.parse(text); } catch { body = text.substring(0, 200) || null; }
        const wwwAuth = r.headers.get('www-authenticate');
        return { status: r.status, body, wwwAuth };
      }

      return res.json({
        token_roles_count: tokenClaims?.roles?.['tiny-api']?.length || 0,
        token_email_verified: tokenClaims?.email_verified,
        refresh: refreshResult,
        endpoints: {
          pedidos:  await tinyFetchFull(`${TINY_API}/pedidos?dataInicial=${dataInicial}&dataFinal=${dataFinal}&pagina=1&limite=1`),
          produtos: await tinyFetchFull(`${TINY_API}/produtos?pagina=1&limite=1`),
          contatos: await tinyFetchFull(`${TINY_API}/contatos?pagina=1&limite=1`),
          estoque:  await tinyFetchFull(`${TINY_API}/estoque/posicao?pagina=1&limite=1`),
          conta:    await tinyFetchFull(`${TINY_API}/conta`),
          info:     await tinyFetchFull(`${TINY_API}/informacoes-conta`),
        },
      });
    } catch(e) { return res.status(500).json({ error: e.message }); }
  }

  // Módulo Tiny Sync — sincroniza pedidos e estoque via API v3 (OAuth Bearer)
  if (req.query.module === 'tiny-sync') {
    const account = req.query.account; // ex: 'tiny_marcon'
    if (!account) return res.status(400).json({ error: 'account é obrigatório. Ex: ?account=tiny_marcon' });

    const company = payload.company || 'lanzi';
    const pool    = getPool(company);
    const TINY_API       = 'https://erp.tiny.com.br/public-api/v3';
    const TINY_TOKEN_URL = 'https://accounts.tiny.com.br/realms/tiny/protocol/openid-connect/token';

    try {
      // 1. Buscar credenciais no banco
      const cfgRes = await pool.query(
        `SELECT chave, valor FROM configuracoes WHERE empresa=$1 AND (chave LIKE $2 OR chave IN ('tiny_client_id','tiny_client_secret'))`,
        [company, account + '%']
      );
      const cfg = {};
      cfgRes.rows.forEach(({ chave, valor }) => { cfg[chave] = valor; });

      let accessToken    = cfg[account + '_token'];
      const refreshToken = cfg[account + '_refresh'];
      const expAt        = cfg[account + '_exp'];
      const modulos      = (cfg[account + '_modulos'] || 'vendas,estoque').split(',').map(m => m.trim());
      const TINY_CLIENT_ID     = (cfg.tiny_client_id     || process.env.TINY_CLIENT_ID     || '').trim();
      const TINY_CLIENT_SECRET = (cfg.tiny_client_secret || process.env.TINY_CLIENT_SECRET || '').trim();

      if (!accessToken) return res.status(400).json({ error: `Conta ${account} não autenticada.` });

      // 2. Renovar token se expirado (ou sempre renovar para garantir token fresco)
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
            await pool.query(`INSERT INTO configuracoes (empresa,chave,valor,atualizado_em) VALUES ($1,$2,$3,NOW()) ON CONFLICT (empresa,chave) DO UPDATE SET valor=EXCLUDED.valor,atualizado_em=NOW()`, [company, k, v]);
          }
        }
      }

      const safeName = account.replace(/[^a-z0-9_]/gi, '_').toLowerCase();
      const results  = {};

      // Helper: fetch paginado da Tiny API v3
      async function tinyPages(endpoint, params = {}) {
        const items = [];
        let pagina  = 1;
        while (true) {
          const qs = new URLSearchParams({ ...params, pagina: String(pagina), limite: '100' }).toString();
          const r  = await fetch(`${TINY_API}${endpoint}?${qs}`, { headers: { 'Authorization': 'Bearer ' + accessToken } });
          if (!r.ok) { console.error('[tiny-sync] ' + endpoint + ' p' + pagina + ' → ' + r.status); break; }
          const d   = await r.json();
          const pg  = d.itens || d.data || [];
          items.push(...pg);
          if (pagina >= (d.totalPaginas || 1) || pg.length === 0) break;
          pagina++;
        }
        return items;
      }

      // 3. Sincronizar pedidos (últimos 90 dias)
      if (modulos.includes('vendas')) {
        await pool.query(`CREATE TABLE IF NOT EXISTS bd_pedidos_tiny_${safeName} (
          id_tiny TEXT PRIMARY KEY,
          numero TEXT, numero_ecommerce TEXT, data_pedido DATE,
          situacao TEXT, nome_cliente TEXT, cpf_cnpj TEXT, uf TEXT,
          valor_produtos NUMERIC DEFAULT 0, valor_frete NUMERIC DEFAULT 0,
          valor_desconto NUMERIC DEFAULT 0, total_pedido NUMERIC DEFAULT 0,
          forma_pagamento TEXT, transportadora TEXT, codigo_rastreamento TEXT,
          canal_venda TEXT, atualizado_em TIMESTAMP DEFAULT NOW()
        )`);
        const dataFinal   = new Date().toISOString().split('T')[0];
        const dataInicial = new Date(Date.now() - 90 * 86400000).toISOString().split('T')[0];
        const pedidos = await tinyPages('/pedidos', { dataInicial, dataFinal });
        let cnt = 0;
        for (const p of pedidos) {
          await pool.query(`
            INSERT INTO bd_pedidos_tiny_${safeName}
              (id_tiny,numero,numero_ecommerce,data_pedido,situacao,nome_cliente,cpf_cnpj,uf,
               valor_produtos,valor_frete,valor_desconto,total_pedido,forma_pagamento,
               transportadora,codigo_rastreamento,canal_venda,atualizado_em)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,NOW())
            ON CONFLICT (id_tiny) DO UPDATE SET
              situacao=EXCLUDED.situacao, total_pedido=EXCLUDED.total_pedido, atualizado_em=NOW()
          `, [
            String(p.id||p.numero||''), p.numero||null, p.numeroEcommerce||p.numero_ecommerce||null,
            p.dataCriacao ? p.dataCriacao.split('T')[0] : null,
            p.situacao?.descricao||p.situacao||null,
            p.contato?.nome||p.cliente?.nome||null, p.contato?.cpfCnpj||p.cliente?.cpf_cnpj||null,
            p.contato?.endereco?.uf||p.cliente?.uf||null,
            parseFloat(p.totalProdutos||p.total_produtos||0)||0,
            parseFloat(p.totalFrete||p.valor_frete||0)||0,
            parseFloat(p.totalDesconto||p.valor_desconto||0)||0,
            parseFloat(p.total||p.valor_total||0)||0,
            p.formaPagamento?.nome||null, p.transportador?.nome||null,
            p.codigoRastreamento||null, p.canalVenda||null,
          ]);
          cnt++;
        }
        results.vendas = cnt;
      }

      // 4. Sincronizar estoque/produtos
      if (modulos.includes('estoque')) {
        await pool.query(`CREATE TABLE IF NOT EXISTS bd_estoque_tiny_${safeName} (
          id_tiny TEXT PRIMARY KEY, sku TEXT, nome TEXT, unidade TEXT,
          estoque_atual NUMERIC DEFAULT 0, estoque_minimo NUMERIC DEFAULT 0,
          preco_custo NUMERIC DEFAULT 0, preco_venda NUMERIC DEFAULT 0,
          marca TEXT, categoria TEXT, atualizado_em TIMESTAMP DEFAULT NOW()
        )`);
        const produtos = await tinyPages('/produtos');
        let cnt = 0;
        for (const p of produtos) {
          await pool.query(`
            INSERT INTO bd_estoque_tiny_${safeName} (id_tiny,sku,nome,unidade,estoque_atual,estoque_minimo,preco_custo,preco_venda,marca,categoria,atualizado_em)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,NOW())
            ON CONFLICT (id_tiny) DO UPDATE SET
              nome=EXCLUDED.nome, estoque_atual=EXCLUDED.estoque_atual,
              estoque_minimo=EXCLUDED.estoque_minimo, preco_venda=EXCLUDED.preco_venda, atualizado_em=NOW()
          `, [
            String(p.id||p.codigo||''), p.codigo||null, p.nome||null, p.unidade||null,
            parseFloat(p.saldo?.total??p.estoqueAtual??0)||0,
            parseFloat(p.saldo?.minimo??p.estoqueMinimo??0)||0,
            parseFloat(p.precoCusto||0)||0, parseFloat(p.preco||0)||0,
            p.marca||null, p.categoria?.nome||p.categoria||null,
          ]);
          cnt++;
        }
        results.estoque = cnt;
      }

      // 5. Atualizar timestamp de sync
      await pool.query(
        `INSERT INTO configuracoes (empresa,chave,valor,atualizado_em) VALUES ($1,$2,$3,NOW()) ON CONFLICT (empresa,chave) DO UPDATE SET valor=EXCLUDED.valor,atualizado_em=NOW()`,
        [company, account + '_token_sync', new Date().toLocaleString('pt-BR')]
      );

      return res.json({ ok: true, account, synced_at: new Date().toISOString(), results });
    } catch(e) { return res.status(500).json({ error: e.message }); }
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

  // ── Módulo Previsão de Recebimentos ───────────────────────────────────
  if (req.query.module === 'forecast-recebimentos') {
    const company = payload.empresa || payload.company || 'lanzi';
    const pool = getPool(company);
    try {
      // Repasse e preço médio por sku×canal (últimos 12 meses)
      const precosR = await pool.query(`
        SELECT
          TRIM("Sku"::text) AS sku,
          COALESCE(NULLIF(TRIM("Canal Apelido"::text),''), TRIM("Canal de venda"), 'Sem canal') AS canal,
          ROUND(AVG(COALESCE("Repasse Financeiro"::numeric, 0) / NULLIF("Quantidade Vendida"::numeric, 0))::numeric, 4) AS rep_und,
          ROUND(AVG(COALESCE("Total Venda"::numeric, 0)        / NULLIF("Quantidade Vendida"::numeric, 0))::numeric, 4) AS fat_und
        FROM bd_vendas
        WHERE "Status" !~* '(cancel|devol|n[aã]o.?pago)'
          AND "Quantidade Vendida"::numeric > 0
          AND "Sku" IS NOT NULL AND TRIM("Sku"::text) != ''
          AND "Data" >= CURRENT_DATE - INTERVAL '12 months'
        GROUP BY 1, 2
      `);
      const precos = {};
      precosR.rows.forEach(({ sku, canal, rep_und, fat_und }) => {
        precos[sku + '§§' + canal] = { rep: parseFloat(rep_und || 0), fat: parseFloat(fat_und || 0) };
      });
      // fallback por sku (média de todos os canais)
      const precosSkuR = await pool.query(`
        SELECT
          TRIM("Sku"::text) AS sku,
          ROUND(AVG(COALESCE("Repasse Financeiro"::numeric, 0) / NULLIF("Quantidade Vendida"::numeric, 0))::numeric, 4) AS rep_und,
          ROUND(AVG(COALESCE("Total Venda"::numeric, 0)        / NULLIF("Quantidade Vendida"::numeric, 0))::numeric, 4) AS fat_und
        FROM bd_vendas
        WHERE "Status" !~* '(cancel|devol|n[aã]o.?pago)'
          AND "Quantidade Vendida"::numeric > 0
          AND "Sku" IS NOT NULL AND TRIM("Sku"::text) != ''
        GROUP BY 1
      `);
      const precosSku = {};
      precosSkuR.rows.forEach(({ sku, rep_und, fat_und }) => {
        precosSku[sku] = { rep: parseFloat(rep_und || 0), fat: parseFloat(fat_und || 0) };
      });

      // Forecast diário com data de recebimento = data_venda + lead_time
      const r = await pool.query(`
        SELECT
          fd.sku,
          fd.canal,
          TO_CHAR((fd.data::date + COALESCE(v.lead_time_dias, 3)), 'YYYY-MM') AS mes_rec,
          SUM(fd.quantidade_prevista) AS qtd
        FROM forecast_diario fd
        LEFT JOIN vendas_canais_config v ON v.canal = fd.canal AND v.empresa = $1
        WHERE (fd.data::date + COALESCE(v.lead_time_dias, 3)) >= CURRENT_DATE
        GROUP BY fd.sku, fd.canal, mes_rec
        ORDER BY mes_rec, fd.canal, fd.sku
      `, [company]);

      const mesesSet = new Set();
      const canalMap = {};
      const skuMap   = {};

      r.rows.forEach(({ sku, canal, mes_rec, qtd }) => {
        const q = parseFloat(qtd || 0);
        if (!q) return;
        mesesSet.add(mes_rec);
        const p = precos[sku + '§§' + canal] || precosSku[sku] || { rep: 0, fat: 0 };
        const rep = q * p.rep;
        const fat = q * p.fat;

        if (!canalMap[canal]) canalMap[canal] = {};
        if (!canalMap[canal][mes_rec]) canalMap[canal][mes_rec] = { rep: 0, fat: 0, qtd: 0 };
        canalMap[canal][mes_rec].rep += rep;
        canalMap[canal][mes_rec].fat += fat;
        canalMap[canal][mes_rec].qtd += q;

        if (!skuMap[sku]) skuMap[sku] = {};
        if (!skuMap[sku][mes_rec]) skuMap[sku][mes_rec] = { rep: 0, fat: 0, qtd: 0 };
        skuMap[sku][mes_rec].rep += rep;
        skuMap[sku][mes_rec].fat += fat;
        skuMap[sku][mes_rec].qtd += q;
      });

      const meses = [...mesesSet].sort();
      const por_canal = Object.entries(canalMap)
        .map(([canal, md]) => ({ canal, meses: md, total_rep: Object.values(md).reduce((s,v)=>s+v.rep,0), total_fat: Object.values(md).reduce((s,v)=>s+v.fat,0) }))
        .sort((a,b) => b.total_rep - a.total_rep);
      const por_sku = Object.entries(skuMap)
        .map(([sku, md]) => ({ sku, meses: md, total_rep: Object.values(md).reduce((s,v)=>s+v.rep,0), total_fat: Object.values(md).reduce((s,v)=>s+v.fat,0) }))
        .sort((a,b) => b.total_rep - a.total_rep);

      return res.json({ meses, por_canal, por_sku });
    } catch(e) {
      console.error('[FORECAST-RECEBIMENTOS]', e.message);
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
    if (tabela === 'dashboard_filters') {
      const [canalRes, marcaRes, mesesRes] = await Promise.all([
        pool.query(`
          SELECT DISTINCT (${CANAL_GRUPO_SQL}) AS canal
          FROM bd_vendas
          WHERE "Canal de venda" IS NOT NULL AND TRIM("Canal de venda") != ''
          ORDER BY 1
        `),
        pool.query(`
          SELECT DISTINCT TRIM("Marca") AS marca FROM cadastros_sku
          WHERE "Marca" IS NOT NULL AND TRIM("Marca") != ''
          ORDER BY 1
        `),
        pool.query(`
          SELECT DISTINCT "Ano" AS ano, "Mes" AS mes FROM bd_vendas
          WHERE "Ano" IS NOT NULL AND "Mes" IS NOT NULL
          ORDER BY "Ano" DESC, "Mes" DESC
        `)
      ]);
      return res.json({
        canais: canalRes.rows.map(r => r.canal).filter(Boolean),
        marcas: marcaRes.rows.map(r => r.marca).filter(Boolean),
        meses:  mesesRes.rows
      });
    }
    if (tabela === 'dashboard_kpis') {
      const { mes: mesFiltro, ano: anoFiltro, marca: marcaFiltro, canal: canalFiltro } = req.query;
      const params = [];
      const filterClauses = [];
      if (canalFiltro) {
        params.push(canalFiltro);
        filterClauses.push(`(${CANAL_GRUPO_SQL}) = $${params.length}`);
      }
      if (marcaFiltro) {
        params.push(marcaFiltro);
        filterClauses.push(`"Sku" IN (SELECT "Sku" FROM cadastros_sku WHERE TRIM("Marca") = $${params.length})`);
      }
      const fWhere = filterClauses.length ? ' AND ' + filterClauses.join(' AND ') : '';
      let lmSQL;
      if (mesFiltro && anoFiltro) {
        params.push(parseInt(anoFiltro));
        params.push(parseInt(mesFiltro));
        lmSQL = `SELECT DATE_TRUNC('month', TO_DATE(
          LPAD($${params.length-1}::text,4,'0') || '-' || LPAD($${params.length}::text,2,'0') || '-01',
          'YYYY-MM-DD'
        )) AS m`;
      } else {
        lmSQL = `SELECT DATE_TRUNC('month', MAX("Data"::date)) AS m FROM bd_vendas WHERE "Data" IS NOT NULL`;
      }
      result = await pool.query(`
        WITH lm AS (${lmSQL}),
        rb AS (
          SELECT SUM(tvp) AS receita_bruta
          FROM (
            SELECT "Order ID", MAX("Total Venda Pedido") AS tvp
            FROM bd_vendas
            WHERE DATE_TRUNC('month', "Data"::date) = (SELECT m FROM lm)${fWhere}
            GROUP BY "Order ID"
          ) t
        )
        SELECT
          EXTRACT(YEAR  FROM "Data"::date) AS ano,
          EXTRACT(MONTH FROM "Data"::date) AS mes,
          (SELECT receita_bruta FROM rb) AS receita_bruta,
          SUM(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN "Total Venda"                   ELSE 0 END) AS receita_liquida,
          SUM(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN "Quantidade Vendida"            ELSE 0 END) AS qtd_liquida,
          SUM(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN COALESCE("Margem Produto", 0)  ELSE 0 END) AS margem_bruta,
          SUM(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN COALESCE("Custo Total", 0)     ELSE 0 END) AS custo_total
        FROM bd_vendas
        WHERE DATE_TRUNC('month', "Data"::date) = (SELECT m FROM lm)${fWhere}
        GROUP BY 1, 2
      `, params);
      return res.json(result.rows[0] || {});
    }
    if (tabela === 'contas_pagar') {
      result = await pool.query(`
        SELECT id, situacao, token_origem, numero_doc, historico, fornecedor,
               valor, saldo, data_vencimento, data_emissao, atualizado_em, data_calculo,
               empresa
        FROM contas_pagar
        ORDER BY data_vencimento ASC NULLS LAST, id ASC
      `);
      return res.json(result.rows);
    }
    if (tabela === 'monthly_revenue') {
      const { marca: marcaFiltro, canal: canalFiltro } = req.query;
      const params = [];
      const filterClauses = [];
      if (canalFiltro) {
        params.push(canalFiltro);
        filterClauses.push(`(${CANAL_GRUPO_SQL}) = $${params.length}`);
      }
      if (marcaFiltro) {
        params.push(marcaFiltro);
        filterClauses.push(`"Sku" IN (SELECT "Sku" FROM cadastros_sku WHERE TRIM("Marca") = $${params.length})`);
      }
      const fWhere = filterClauses.length ? ' AND ' + filterClauses.join(' AND ') : '';
      result = await pool.query(`
        SELECT ano, mes,
               SUM(tvp)  AS receita,
               SUM(qtd)  AS qtd,
               CASE WHEN SUM(receita_liq) > 0
                    THEN ROUND((
                      SUM(margem_liq) / SUM(receita_liq) * 100
                    )::numeric, 2)
                    ELSE NULL END AS mc_pct
        FROM (
          SELECT "Ano" AS ano, "Mes" AS mes, "Order ID",
                 MAX("Total Venda Pedido") AS tvp,
                 SUM("Quantidade Vendida") AS qtd,
                 SUM(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN "Total Venda"                ELSE 0 END) AS receita_liq,
                 SUM(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN COALESCE("Margem Produto", 0) ELSE 0 END) AS margem_liq
          FROM bd_vendas
          WHERE "Ano" IS NOT NULL AND "Mes" IS NOT NULL${fWhere}
          GROUP BY "Ano", "Mes", "Order ID"
        ) t
        GROUP BY ano, mes
        ORDER BY ano ASC, mes ASC
      `, params);
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
      const { mes_prev, ano_prev, mes_curr, ano_curr, dia_ini_prev, dia_fim_prev, dia_ini_curr, dia_fim_curr, canal } = req.query;
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
      const pmvParams = [datePrevIni, datePrevFim, dateCurrIni, dateCurrFim];
      let pmvCanalWhere = '';
      if (canal) {
        pmvParams.push(canal);
        pmvCanalWhere = ` AND (${CANAL_GRUPO_SQL}) = $${pmvParams.length}`;
      }
      result = await pool.query(`
        SELECT
          v."Sku" AS sku,
          MAX(v."Nome Produto") AS nome_produto,
          MAX(v."Categoria") AS categoria,
          COALESCE(NULLIF(TRIM(MAX(cs."Marca")), ''), '–') AS marca,
          COALESCE(MAX(pp.estoque_atual::numeric), 0) AS estoque_atual,
          SUM(CASE WHEN v."Data"::date BETWEEN $1::date AND $2::date THEN v."Quantidade Vendida" ELSE 0 END) AS qtd_prev,
          SUM(CASE WHEN v."Data"::date BETWEEN $1::date AND $2::date THEN v."Total Venda" ELSE 0 END) AS rev_prev,
          SUM(CASE WHEN v."Data"::date BETWEEN $1::date AND $2::date THEN COALESCE(v."Margem Produto",0) ELSE 0 END) AS mar_prev,
          SUM(CASE WHEN v."Data"::date BETWEEN $3::date AND $4::date THEN v."Quantidade Vendida" ELSE 0 END) AS qtd_curr,
          SUM(CASE WHEN v."Data"::date BETWEEN $3::date AND $4::date THEN v."Total Venda" ELSE 0 END) AS rev_curr,
          SUM(CASE WHEN v."Data"::date BETWEEN $3::date AND $4::date THEN COALESCE(v."Margem Produto",0) ELSE 0 END) AS mar_curr
        FROM bd_vendas v
        LEFT JOIN cadastros_sku cs ON TRIM(cs."Sku"::text) = TRIM(v."Sku"::text)
        LEFT JOIN ponto_pedido pp ON TRIM(pp.sku::text) = TRIM(v."Sku"::text)
        WHERE v."Status" !~* '(cancel|devol|n[aã]o.?pago)'
          AND (
            v."Data"::date BETWEEN $1::date AND $2::date OR
            v."Data"::date BETWEEN $3::date AND $4::date
          )${pmvCanalWhere}
        GROUP BY v."Sku"
        HAVING SUM(v."Quantidade Vendida") > 0
        ORDER BY SUM(CASE WHEN v."Data"::date BETWEEN $3::date AND $4::date THEN v."Total Venda" ELSE 0 END) DESC
      `, pmvParams);
      return res.json(result.rows);
    }
    if (tabela === 'pmv_canais') {
      const { mes_prev, ano_prev, mes_curr, ano_curr, dia_ini_prev, dia_fim_prev, dia_ini_curr, dia_fim_curr, sku, canal } = req.query;
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
      const canaisParams = [datePrevIni, datePrevFim, dateCurrIni, dateCurrFim, sku];
      let canaisCanalWhere = '';
      if (canal) {
        canaisParams.push(canal);
        canaisCanalWhere = ` AND (${CANAL_GRUPO_SQL}) = $${canaisParams.length}`;
      }
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
          )${canaisCanalWhere}
        GROUP BY COALESCE(NULLIF(TRIM("Canal Apelido"::text), ''), TRIM("Canal de venda"), 'Sem canal')
        HAVING SUM("Quantidade Vendida") > 0
        ORDER BY SUM(CASE WHEN "Data"::date BETWEEN $3::date AND $4::date THEN "Total Venda" ELSE 0 END) DESC
      `, canaisParams);
      return res.json(result.rows);
    }
    if (tabela === 'ponto_pedido') {
      const [ppRes, esRes, f1Map, f2Map] = await Promise.all([
        pool.query(`SELECT * FROM ponto_pedido LIMIT 5000`),
        pool.query(`SELECT sku, REPLACE(media_mensal::text,',','.')::numeric AS media_mensal FROM estoque_seguranca`).catch(() => ({ rows: [] })),
        lerEstoqueFullMap(pool, 'full_1').catch(() => ({})),
        lerEstoqueFullMap(pool, 'full_2').catch(() => ({})),
      ]);
      const mediaMap = {};
      esRes.rows.forEach(r => { mediaMap[String(r.sku || '').trim()] = r.media_mensal; });
      const fullMap = {};
      Object.keys(f1Map).forEach(s => { fullMap[s] = (fullMap[s] || 0) + f1Map[s]; });
      Object.keys(f2Map).forEach(s => { fullMap[s] = (fullMap[s] || 0) + f2Map[s]; });
      return res.json(ppRes.rows.map(r => {
        const sku = String(r.sku || '').trim();
        return {
          ...r,
          media_mensal: mediaMap[sku] ?? null,
          estoque_full: fullMap[sku] ?? (parseFloat(r.estoque_atual) || 0),
        };
      }));
    }
    if (tabela === 'curva_abc') {
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
