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
    const { ano, mes } = req.query;
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

  // Módulo Fornecedores Config (lead time por SKU)
  if (req.query.module === 'fornecedores-config') {
    const company = payload.company || 'lanzi';
    const pool = getPool(company);
    try {
      await pool.query(`
        CREATE TABLE IF NOT EXISTS fornecedores_config (
          empresa       VARCHAR(50)  NOT NULL,
          sku           TEXT         NOT NULL,
          lead_time_dias INTEGER     NOT NULL DEFAULT 30,
          PRIMARY KEY (empresa, sku)
        )
      `);
      if (req.method === 'GET') {
        const r = await pool.query(`
          SELECT c."Sku" AS sku, c."Marca" AS nome, f.lead_time_dias
          FROM cadastros_sku c
          LEFT JOIN fornecedores_config f
            ON f.sku = c."Sku" AND f.empresa = $1
          ORDER BY c."Sku"
        `, [company]);
        return res.json({ skus: r.rows });
      }
      if (req.method === 'POST') {
        const { sku, lead_time_dias } = req.body || {};
        if (!sku || lead_time_dias == null) {
          return res.status(400).json({ error: 'sku e lead_time_dias são obrigatórios' });
        }
        const dias = parseInt(lead_time_dias);
        if (isNaN(dias) || dias < 1) {
          return res.status(400).json({ error: 'lead_time_dias deve ser inteiro >= 1' });
        }
        await pool.query(`
          INSERT INTO fornecedores_config (empresa, sku, lead_time_dias)
          VALUES ($1, $2, $3)
          ON CONFLICT (empresa, sku) DO UPDATE SET lead_time_dias = EXCLUDED.lead_time_dias
        `, [company, sku, dias]);
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
    if (tabela === 'monthly_revenue') {
      result = await pool.query(`
        SELECT "Ano" AS ano, "Mes" AS mes,
               SUM("Total Venda") AS receita,
               SUM("Quantidade Vendida") AS qtd
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
