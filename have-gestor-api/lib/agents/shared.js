// ── Utilitários compartilhados pelos módulos de agentes ──────────────────────

async function safeQuery(pool, sql, params = []) {
  try {
    const r = await pool.query(sql, params);
    return r.rows;
  } catch (e) {
    console.error('[safeQuery]', e.message);
    return [];
  }
}

// ─── S&OP PARAMS (lê sopc_config, cache 5 min por empresa) ───────────────────

const sopcParamsCache = {};
async function getSopcParams(pool, company) {
  const now = Date.now();
  if (sopcParamsCache[company] && now - sopcParamsCache[company].ts < 5 * 60 * 1000) {
    return sopcParamsCache[company].data;
  }
  const rows = await safeQuery(pool,
    `SELECT chave, valor FROM sopc_config WHERE empresa = $1 AND modulo = 'reposicao'`,
    [company]
  );
  const cfg = Object.fromEntries(rows.map(r => [r.chave, Number(r.valor)]));
  const data = {
    meta_a:              cfg.meta_dias_a             ?? 20,
    meta_b:              cfg.meta_dias_b             ?? 15,
    meta_c:              cfg.meta_dias_c             ?? 10,
    alerta_ruptura_dias: cfg.alerta_ruptura_dias     ?? 7,
    alerta_abaixo_meta:  cfg.alerta_abaixo_meta_dias ?? 15,
    encalhe_dias:        cfg.encalhe_dias            ?? 90,
    lead_time:           cfg.lead_time_dias          ?? 15,
  };
  sopcParamsCache[company] = { data, ts: now };
  return data;
}

// ─── Limites de truncamento de resultados de tools ───────────────────────────

const TOOL_RESULT_MAX_CHARS        = 1500;
const TOOL_RESULT_MAX_CHARS_LARGE  = 5000;
const TOOL_RESULT_MAX_CHARS_SOPC   = 25000;
const TOOL_RESULT_MAX_CHARS_SOPC_ITEM = 10000;

module.exports = {
  safeQuery,
  getSopcParams,
  TOOL_RESULT_MAX_CHARS,
  TOOL_RESULT_MAX_CHARS_LARGE,
  TOOL_RESULT_MAX_CHARS_SOPC,
  TOOL_RESULT_MAX_CHARS_SOPC_ITEM,
};
