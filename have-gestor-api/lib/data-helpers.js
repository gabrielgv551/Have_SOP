function parseBody(raw) {
  if (typeof raw === 'string') { try { return JSON.parse(raw); } catch { return {}; } }
  return raw || {};
}

async function upsertConfig(pool, company, chave, valor) {
  await pool.query(
    `INSERT INTO configuracoes (empresa,chave,valor,atualizado_em) VALUES ($1,$2,$3,NOW())
     ON CONFLICT (empresa,chave) DO UPDATE SET valor=EXCLUDED.valor, atualizado_em=NOW()`,
    [company, chave, String(valor)]
  );
}

function isTinyTable(t) { return /^bd_(pedidos|estoque)_tiny_[a-z0-9_]+$/.test(t); }
function isBlingTable(t) { return /^bd_(pedidos|estoque)_bling_[a-z0-9_]+$/.test(t); }

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
  'historico_vendas',
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
    [chave,              tokenData.access_token],
    [chave + '_refresh', tokenData.refresh_token],
    [chave + '_exp',     newExpAt],
  ]) {
    await upsertConfig(pool, company, k, v);
  }
  return tokenData.access_token;
}

module.exports = {
  parseBody,
  upsertConfig,
  isTinyTable,
  isBlingTable,
  TABELAS_PERMITIDAS,
  CANAL_COL,
  CANAL_GRUPO_SQL,
  getTableColumns,
  findSkuCol,
  findStockCol,
  lerEstoqueFullMap,
  getMlToken,
};



