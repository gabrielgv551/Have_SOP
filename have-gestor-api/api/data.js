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
        pool.query(`SELECT "Sku" AS sku, TRIM("Canal de venda") AS canal, ROUND(SUM("Quantidade Vendida"::numeric)/3.0,1) AS media FROM bd_vendas WHERE "Status" NOT ILIKE '%cancel%' AND "Data"::date >= (SELECT MAX("Data"::date) FROM bd_vendas) - INTERVAL '3 months' AND "Sku" IS NOT NULL AND TRIM("Canal de venda") IS NOT NULL AND TRIM("Canal de venda") != '' GROUP BY "Sku", TRIM("Canal de venda")`).catch(()=>({rows:[]})),
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
        WHERE "Status" NOT ILIKE '%cancel%'
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
      const whereLatest = `
        WHERE "Ano" = (SELECT MAX("Ano") FROM bd_vendas)
          AND "Mês" = (
            SELECT "Mês" FROM bd_vendas
            WHERE "Ano" = (SELECT MAX("Ano") FROM bd_vendas)
            ORDER BY "Mês" DESC LIMIT 1
          )`;
      try {
        result = await pool.query(`
          SELECT
            "Ano" AS ano, "Mês" AS mes,
            SUM("Total prod. vendidos") AS receita_bruta,
            SUM(CASE WHEN "Status" NOT ILIKE '%cancel%' THEN "Total prod. vendidos" ELSE 0 END) AS receita_liquida,
            SUM(CASE WHEN "Status" NOT ILIKE '%cancel%' THEN "Quantidade Vendida" ELSE 0 END) AS qtd_liquida,
            SUM(CASE WHEN "Status" NOT ILIKE '%cancel%' THEN "Margem Contribuição" ELSE 0 END) AS margem_bruta
          FROM bd_vendas ${whereLatest}
          GROUP BY "Ano", "Mês"
        `);
      } catch(e) {
        result = await pool.query(`
          SELECT
            "Ano" AS ano, "Mês" AS mes,
            SUM("Total prod. vendidos") AS receita_bruta,
            SUM(CASE WHEN "Status" NOT ILIKE '%cancel%' THEN "Total prod. vendidos" ELSE 0 END) AS receita_liquida,
            SUM(CASE WHEN "Status" NOT ILIKE '%cancel%' THEN "Quantidade Vendida" ELSE 0 END) AS qtd_liquida,
            NULL AS margem_bruta
          FROM bd_vendas ${whereLatest}
          GROUP BY "Ano", "Mês"
        `);
      }
      return res.json(result.rows[0] || {});
    }
    if (tabela === 'monthly_revenue') {
      result = await pool.query(`
        SELECT "Ano" AS ano, "Mês" AS mes,
               SUM("Total prod. vendidos") AS receita,
               SUM("Quantidade Vendida") AS qtd
        FROM bd_vendas
        WHERE "Status" NOT ILIKE '%cancel%'
        GROUP BY "Ano", "Mês"
        ORDER BY "Ano" ASC, "Mês" ASC
      `);
      return res.json(result.rows);
    }
    if (tabela === 'pmv_months') {
      result = await pool.query(`
        SELECT DISTINCT "Ano" AS ano, "Mês" AS mes
        FROM bd_vendas
        WHERE "Ano" IS NOT NULL AND "Mês" IS NOT NULL
        ORDER BY "Ano" DESC, "Mês" DESC
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
          SUM(CASE WHEN "Data"::date BETWEEN $1::date AND $2::date THEN "Quantidade Vendida" ELSE 0 END) AS qtd_prev,
          SUM(CASE WHEN "Data"::date BETWEEN $1::date AND $2::date THEN "Total prod. vendidos" ELSE 0 END) AS rev_prev,
          SUM(CASE WHEN "Data"::date BETWEEN $1::date AND $2::date THEN COALESCE("Margem Contribuição",0) ELSE 0 END) AS mar_prev,
          SUM(CASE WHEN "Data"::date BETWEEN $3::date AND $4::date THEN "Quantidade Vendida" ELSE 0 END) AS qtd_curr,
          SUM(CASE WHEN "Data"::date BETWEEN $3::date AND $4::date THEN "Total prod. vendidos" ELSE 0 END) AS rev_curr,
          SUM(CASE WHEN "Data"::date BETWEEN $3::date AND $4::date THEN COALESCE("Margem Contribuição",0) ELSE 0 END) AS mar_curr
        FROM bd_vendas
        WHERE "Status" NOT ILIKE '%cancel%'
          AND (
            "Data"::date BETWEEN $1::date AND $2::date OR
            "Data"::date BETWEEN $3::date AND $4::date
          )
        GROUP BY "Sku"
        HAVING SUM("Quantidade Vendida") > 0
        ORDER BY SUM(CASE WHEN "Data"::date BETWEEN $3::date AND $4::date THEN "Total prod. vendidos" ELSE 0 END) DESC
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
          COALESCE(TRIM("Canal de venda"), 'Sem canal') AS canal,
          SUM(CASE WHEN "Data"::date BETWEEN $1::date AND $2::date THEN "Quantidade Vendida" ELSE 0 END) AS qtd_prev,
          SUM(CASE WHEN "Data"::date BETWEEN $1::date AND $2::date THEN "Total prod. vendidos" ELSE 0 END) AS rev_prev,
          SUM(CASE WHEN "Data"::date BETWEEN $1::date AND $2::date THEN COALESCE("Margem Contribuição",0) ELSE 0 END) AS mar_prev,
          SUM(CASE WHEN "Data"::date BETWEEN $3::date AND $4::date THEN "Quantidade Vendida" ELSE 0 END) AS qtd_curr,
          SUM(CASE WHEN "Data"::date BETWEEN $3::date AND $4::date THEN "Total prod. vendidos" ELSE 0 END) AS rev_curr,
          SUM(CASE WHEN "Data"::date BETWEEN $3::date AND $4::date THEN COALESCE("Margem Contribuição",0) ELSE 0 END) AS mar_curr
        FROM bd_vendas
        WHERE "Status" NOT ILIKE '%cancel%'
          AND "Sku" = $5
          AND (
            "Data"::date BETWEEN $1::date AND $2::date OR
            "Data"::date BETWEEN $3::date AND $4::date
          )
        GROUP BY COALESCE(TRIM("Canal de venda"), 'Sem canal')
        HAVING SUM("Quantidade Vendida") > 0
        ORDER BY SUM(CASE WHEN "Data"::date BETWEEN $3::date AND $4::date THEN "Total prod. vendidos" ELSE 0 END) DESC
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
    result = await pool.query(`SELECT * FROM ${tabela} LIMIT 5000`);
    res.json(result.rows);
  } catch (e) {
    console.error(`[ERRO] ${payload.company} / ${tabela}:`, e.message);
    res.status(500).json({ error: e.message });
  }
};
