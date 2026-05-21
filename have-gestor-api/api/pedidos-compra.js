const jwt = require('jsonwebtoken');
const crypto = require('crypto');
const { getPool } = require('../lib/db');

function verifyToken(req, res) {
  const auth = (req.headers.authorization || '').split(' ')[1];
  if (!auth) { res.status(401).json({ error: 'Token nao fornecido' }); return null; }
  try { return jwt.verify(auth, process.env.JWT_SECRET); }
  catch { res.status(401).json({ error: 'Token invalido' }); return null; }
}

function addDays(dateStr, days) {
  const d = new Date(dateStr + 'T00:00:00Z');
  d.setUTCDate(d.getUTCDate() + days);
  return d.toISOString().slice(0, 10);
}

// ── Helpers for date calculation ─────────────────────────────────────────────
function nextPreferredDay(fromDate, dayOfWeek) {
  // dayOfWeek: 0=Sun,1=Mon,...,5=Fri,6=Sat
  const d = new Date(fromDate + 'T00:00:00Z');
  const diff = (dayOfWeek - d.getUTCDay() + 7) % 7;
  d.setUTCDate(d.getUTCDate() + (diff === 0 ? 7 : diff));
  return d.toISOString().slice(0, 10);
}

function calcNextOrderDate(lastEmissao, freqTipo, diaSemPref, intervaloDias) {
  const today = new Date().toISOString().slice(0, 10);
  if (!lastEmissao) {
    // No previous order → suggest next preferred day from today
    return nextPreferredDay(today, diaSemPref);
  }
  let rawNext;
  if (freqTipo === 'semanal') rawNext = addDays(lastEmissao, 7);
  else if (freqTipo === 'quinzenal') rawNext = addDays(lastEmissao, 14);
  else if (freqTipo === 'mensal') rawNext = addDays(lastEmissao, 30);
  else rawNext = addDays(lastEmissao, intervaloDias || 30);

  // If rawNext is in the past, jump forward to today
  if (rawNext < today) rawNext = today;
  // Snap to preferred day of week
  const d = new Date(rawNext + 'T00:00:00Z');
  if (d.getUTCDay() === diaSemPref) return rawNext;
  return nextPreferredDay(rawNext, diaSemPref);
}

// ── Sugestoes sub-handler ───────────────────────────────────────────────────
async function handleSugestoes(req, res, pool, company) {
  if (req.method !== 'GET') return res.status(405).json({ error: 'Method not allowed' });

  // 1. SKUs that need reorder
  const ppRes = await pool.query(`
    SELECT pp.sku, pp.alerta, pp.qty_sugerida::numeric AS qty_sugerida,
           pp.estoque_atual::numeric AS estoque_atual, pp.ponto_pedido::numeric AS ponto_pedido,
           c."Marca" AS marca, COALESCE(c."Custo Un"::numeric, 0) AS custo_un
    FROM ponto_pedido pp
    LEFT JOIN cadastros_sku c ON TRIM(c."Sku") = TRIM(pp.sku)
    WHERE pp.alerta IN ('RUPTURA IMINENTE', 'PEDIR AGORA')
      AND pp.qty_sugerida::numeric > 0
    ORDER BY pp.alerta ASC, pp.qty_sugerida::numeric DESC
  `);

  // 2. Frequency config per marca
  const fcRes = await pool.query(`
    SELECT marca,
           COALESCE(frequencia_tipo, 'mensal') AS frequencia_tipo,
           COALESCE(dia_semana_preferido, 5) AS dia_semana_preferido,
           COALESCE(intervalo_dias, 30) AS intervalo_dias,
           lead_time_dias
    FROM fornecedores_config
    WHERE empresa = $1
  `, [company]);
  const configMap = {};
  fcRes.rows.forEach(r => { configMap[r.marca] = r; });

  // 3. Last order per fornecedor (marca)
  const lastRes = await pool.query(`
    SELECT fornecedor, MAX(data_emissao) AS last_emissao
    FROM pedidos_compra
    WHERE empresa = $1
    GROUP BY fornecedor
  `, [company]);
  const lastMap = {};
  lastRes.rows.forEach(r => {
    lastMap[r.fornecedor] = r.last_emissao
      ? new Date(r.last_emissao).toISOString().slice(0, 10)
      : null;
  });

  // Default config
  const defaultCfg = { frequencia_tipo: 'mensal', dia_semana_preferido: 5, intervalo_dias: 30, lead_time_dias: 30 };

  // 4. Group by marca
  const groups = {};
  ppRes.rows.forEach(row => {
    const marca = (row.marca || '').trim() || 'SEM MARCA';
    if (!groups[marca]) groups[marca] = { marca, skus: [], custo_total: 0 };
    const qty = parseFloat(row.qty_sugerida) || 0;
    const custo = parseFloat(row.custo_un) || 0;
    groups[marca].skus.push({
      sku: row.sku,
      alerta: row.alerta,
      qty_sugerida: qty,
      estoque_atual: parseFloat(row.estoque_atual) || 0,
      ponto_pedido: parseFloat(row.ponto_pedido) || 0,
      custo_un: custo,
      custo_total: Math.round(qty * custo * 100) / 100,
    });
    groups[marca].custo_total += qty * custo;
  });

  // 5. Enrich each group with dates & config
  const sugestoes = Object.values(groups).map(g => {
    const cfg = configMap[g.marca] || defaultCfg;
    const lastEmissao = lastMap[g.marca] || null;
    const proximaData = calcNextOrderDate(
      lastEmissao,
      cfg.frequencia_tipo,
      parseInt(cfg.dia_semana_preferido),
      parseInt(cfg.intervalo_dias)
    );
    return {
      marca: g.marca,
      total_skus: g.skus.length,
      custo_total: Math.round(g.custo_total * 100) / 100,
      frequencia: cfg.frequencia_tipo,
      lead_time: parseInt(cfg.lead_time_dias) || 30,
      ultimo_pedido: lastEmissao,
      proxima_data: proximaData,
      skus: g.skus,
    };
  });

  // Sort: groups with RUPTURA IMINENTE first, then by custo_total desc
  sugestoes.sort((a, b) => {
    const aRuptura = a.skus.some(s => s.alerta === 'RUPTURA IMINENTE') ? 0 : 1;
    const bRuptura = b.skus.some(s => s.alerta === 'RUPTURA IMINENTE') ? 0 : 1;
    if (aRuptura !== bRuptura) return aRuptura - bRuptura;
    return b.custo_total - a.custo_total;
  });

  return res.json({ sugestoes });
}

// ── Categorias sub-handler (for dropdown) ──────────────────────────────────
async function handleCategorias(req, res, pool, company) {
  if (req.method !== 'GET') return res.status(405).json({ error: 'Method not allowed' });
  const r = await pool.query(
    "SELECT nome FROM caixa_modelo WHERE empresa=$1 AND tipo='item' ORDER BY ordem ASC, nome ASC",
    [company]
  );
  return res.json({ categorias: r.rows.map(r => r.nome) });
}

// ── Fornecedores sub-handler ─────────────────────────────────────────────────
async function handleFornecedores(req, res, pool, company) {
  if (req.method === 'GET') {
    const r = await pool.query(
      'SELECT id, nome, prazo_padrao_dias, criado_em FROM pc_fornecedores WHERE empresa=$1 ORDER BY nome ASC',
      [company]
    );
    return res.json({ fornecedores: r.rows });
  }
  if (req.method === 'POST') {
    const { nome, prazo_padrao_dias } = req.body;
    if (!nome) return res.status(400).json({ error: 'Informe nome' });
    const prazo = parseInt(prazo_padrao_dias) || 30;
    const r = await pool.query(
      `INSERT INTO pc_fornecedores (empresa, nome, prazo_padrao_dias)
       VALUES ($1,$2,$3)
       ON CONFLICT (empresa, nome) DO UPDATE SET prazo_padrao_dias=EXCLUDED.prazo_padrao_dias
       RETURNING id, nome, prazo_padrao_dias, criado_em`,
      [company, nome.substring(0, 200), prazo]
    );
    return res.json({ ok: true, fornecedor: r.rows[0] });
  }
  if (req.method === 'DELETE') {
    const { id } = req.query;
    if (!id) return res.status(400).json({ error: 'Informe id' });
    await pool.query('DELETE FROM pc_fornecedores WHERE id=$1 AND empresa=$2', [parseInt(id), company]);
    return res.json({ ok: true });
  }
  return res.status(405).json({ error: 'Method not allowed' });
}

// ── Itens (SKU breakdown) sub-handler ────────────────────────────────────────
async function handleItens(req, res, pool, company) {
  if (req.method !== 'GET') return res.status(405).json({ error: 'Method not allowed' });
  const { grupo_id } = req.query;
  if (!grupo_id) return res.status(400).json({ error: 'Informe grupo_id' });
  const r = await pool.query(
    'SELECT id, sku, quantidade, custo_un, custo_total FROM pedidos_compra_itens WHERE grupo_id=$1 AND empresa=$2 ORDER BY id',
    [grupo_id, company]
  );
  return res.json({ itens: r.rows });
}

// ── SKU search for autocomplete ────────────────────────────────────────────
async function handleSkuSearch(req, res, pool, company) {
  if (req.method !== 'GET') return res.status(405).json({ error: 'Method not allowed' });
  const q = (req.query.q || '').trim();
  let r;
  if (q) {
    r = await pool.query(
      `SELECT TRIM("Sku"::text) AS sku, MAX("Nome Produto") AS nome, COALESCE(MAX("Custo Un"::numeric), 0) AS custo_un
       FROM cadastros_sku WHERE TRIM("Sku"::text) ILIKE $1 AND "Sku" IS NOT NULL AND TRIM("Sku"::text) != ''
       GROUP BY TRIM("Sku"::text) ORDER BY TRIM("Sku"::text) LIMIT 30`,
      [`%${q}%`]
    );
  } else {
    r = await pool.query(
      `SELECT TRIM("Sku"::text) AS sku, MAX("Nome Produto") AS nome, COALESCE(MAX("Custo Un"::numeric), 0) AS custo_un
       FROM cadastros_sku WHERE "Sku" IS NOT NULL AND TRIM("Sku"::text) != ''
       GROUP BY TRIM("Sku"::text) ORDER BY TRIM("Sku"::text) LIMIT 200`
    );
  }
  return res.json({ skus: r.rows });
}

// ── Main handler ─────────────────────────────────────────────────────────────
module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  if (req.method === 'OPTIONS') return res.status(200).end();

  const payload = verifyToken(req, res);
  if (!payload) return;
  const company = payload.company || 'lanzi';
  const pool = getPool(company);

  try {
    if (req.query.module === 'fornecedores') return await handleFornecedores(req, res, pool, company);
    if (req.query.module === 'categorias')   return await handleCategorias(req, res, pool, company);
    if (req.query.module === 'sugestoes')    return await handleSugestoes(req, res, pool, company);
    if (req.query.module === 'itens')        return await handleItens(req, res, pool, company);
    if (req.query.module === 'skus')         return await handleSkuSearch(req, res, pool, company);

    if (req.method === 'GET') {
      const { ano, mes } = req.query;
      let q = `SELECT id, fornecedor, data_emissao, nf, valor, vencimento, vencimento_ajustado,
                      parcela, total_parcelas, linha_fluxo, grupo_id, created_at
               FROM pedidos_compra WHERE empresa=$1`;
      const params = [company];
      if (ano && mes) {
        q += ` AND (
          EXTRACT(YEAR FROM COALESCE(vencimento_ajustado, vencimento))=$2
          AND EXTRACT(MONTH FROM COALESCE(vencimento_ajustado, vencimento))=$3
        )`;
        params.push(parseInt(ano), parseInt(mes));
      }
      q += ' ORDER BY COALESCE(vencimento_ajustado, vencimento) ASC, id ASC';
      const r = await pool.query(q, params);
      return res.json({ pedidos: r.rows });
    }

    if (req.method === 'POST') {
      const { fornecedor, data_emissao, nf, valor, vencimento, vencimento_ajustado, prazos, linha_fluxo, itens } = req.body;
      if (!fornecedor || !data_emissao || valor == null)
        return res.status(400).json({ error: 'Informe fornecedor, data_emissao e valor' });

      const valorNum = parseFloat(valor);
      if (isNaN(valorNum) || valorNum < 0)
        return res.status(400).json({ error: 'Valor invalido' });

      const grupo_id = crypto.randomUUID();
      const client = await pool.connect();
      try {
        await client.query('BEGIN');
        const inserted = [];

        if (Array.isArray(prazos) && prazos.length > 0) {
          const validPrazos = prazos.map(p => parseInt(p)).filter(p => p > 0);
          if (!validPrazos.length) return res.status(400).json({ error: 'Informe ao menos um prazo valido' });
          const n = validPrazos.length;
          const valorParcela = Math.round((valorNum / n) * 100) / 100;
          for (let i = 0; i < n; i++) {
            const venc = addDays(data_emissao, validPrazos[i]);
            const r = await client.query(
              `INSERT INTO pedidos_compra
                 (empresa, fornecedor, data_emissao, nf, valor, vencimento, vencimento_ajustado, parcela, total_parcelas, linha_fluxo, grupo_id)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
               RETURNING id, fornecedor, data_emissao, nf, valor, vencimento, vencimento_ajustado, parcela, total_parcelas, linha_fluxo, grupo_id, created_at`,
              [company, fornecedor.substring(0, 200), data_emissao,
               nf ? nf.substring(0, 100) : null, valorParcela,
               venc, vencimento_ajustado || null, i + 1, n, linha_fluxo || null, grupo_id]
            );
            inserted.push(r.rows[0]);
          }
        } else {
          if (!vencimento) return res.status(400).json({ error: 'Informe vencimento' });
          const r = await client.query(
            `INSERT INTO pedidos_compra
               (empresa, fornecedor, data_emissao, nf, valor, vencimento, vencimento_ajustado, parcela, total_parcelas, linha_fluxo, grupo_id)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
             RETURNING id, fornecedor, data_emissao, nf, valor, vencimento, vencimento_ajustado, parcela, total_parcelas, linha_fluxo, grupo_id, created_at`,
            [company, fornecedor.substring(0, 200), data_emissao,
             nf ? nf.substring(0, 100) : null, valorNum,
             vencimento, vencimento_ajustado || null, null, null, linha_fluxo || null, grupo_id]
          );
          inserted.push(r.rows[0]);
        }

        // Save SKU items if provided
        if (Array.isArray(itens) && itens.length > 0) {
          await client.query('DELETE FROM pedidos_compra_itens WHERE grupo_id=$1 AND empresa=$2', [grupo_id, company]);
          for (const item of itens) {
            const qty = parseFloat(item.quantidade) || 0;
            const cu = parseFloat(item.custo_un) || 0;
            await client.query(
              `INSERT INTO pedidos_compra_itens (grupo_id, empresa, sku, quantidade, custo_un, custo_total)
               VALUES ($1,$2,$3,$4,$5,$6)`,
              [grupo_id, company, String(item.sku).substring(0, 100), qty, cu, Math.round(qty * cu * 100) / 100]
            );
          }
        }

        await client.query('COMMIT');
        return res.json({ ok: true, pedidos: inserted });
      } catch (e) {
        await client.query('ROLLBACK'); throw e;
      } finally { client.release(); }
    }

    if (req.method === 'PUT') {
      // Edit: find original grupo_id, delete all parcelas + items, recreate
      const { id } = req.query;
      if (!id) return res.status(400).json({ error: 'Informe id' });
      const { fornecedor, data_emissao, nf, valor, vencimento, vencimento_ajustado, prazos, linha_fluxo, itens } = req.body;
      if (!fornecedor || !data_emissao || valor == null)
        return res.status(400).json({ error: 'Informe fornecedor, data_emissao e valor' });
      const valorNum = parseFloat(valor);
      if (isNaN(valorNum) || valorNum < 0)
        return res.status(400).json({ error: 'Valor invalido' });

      // Find original record to get grupo_id
      const origR = await pool.query('SELECT grupo_id FROM pedidos_compra WHERE id=$1 AND empresa=$2', [parseInt(id), company]);
      if (!origR.rowCount) return res.status(404).json({ error: 'Pedido nao encontrado' });
      const oldGrupoId = origR.rows[0].grupo_id;

      const novo_grupo_id = crypto.randomUUID();
      const client = await pool.connect();
      try {
        await client.query('BEGIN');
        // Delete old records
        if (oldGrupoId) {
          await client.query('DELETE FROM pedidos_compra WHERE grupo_id=$1 AND empresa=$2', [oldGrupoId, company]);
          await client.query('DELETE FROM pedidos_compra_itens WHERE grupo_id=$1 AND empresa=$2', [oldGrupoId, company]);
        } else {
          await client.query('DELETE FROM pedidos_compra WHERE id=$1 AND empresa=$2', [parseInt(id), company]);
        }

        const inserted = [];
        if (Array.isArray(prazos) && prazos.length > 0) {
          const validPrazos = prazos.map(p => parseInt(p)).filter(p => p > 0);
          const n = validPrazos.length;
          const valorParcela = Math.round((valorNum / n) * 100) / 100;
          for (let i = 0; i < n; i++) {
            const venc = addDays(data_emissao, validPrazos[i]);
            const r = await client.query(
              `INSERT INTO pedidos_compra
                 (empresa, fornecedor, data_emissao, nf, valor, vencimento, vencimento_ajustado, parcela, total_parcelas, linha_fluxo, grupo_id)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
               RETURNING id, fornecedor, data_emissao, nf, valor, vencimento, vencimento_ajustado, parcela, total_parcelas, linha_fluxo, grupo_id, created_at`,
              [company, fornecedor.substring(0, 200), data_emissao,
               nf ? nf.substring(0, 100) : null, valorParcela,
               venc, vencimento_ajustado || null, i + 1, n, linha_fluxo || null, novo_grupo_id]
            );
            inserted.push(r.rows[0]);
          }
        } else {
          if (!vencimento) return res.status(400).json({ error: 'Informe vencimento' });
          const r = await client.query(
            `INSERT INTO pedidos_compra
               (empresa, fornecedor, data_emissao, nf, valor, vencimento, vencimento_ajustado, parcela, total_parcelas, linha_fluxo, grupo_id)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
             RETURNING id, fornecedor, data_emissao, nf, valor, vencimento, vencimento_ajustado, parcela, total_parcelas, linha_fluxo, grupo_id, created_at`,
            [company, fornecedor.substring(0, 200), data_emissao,
             nf ? nf.substring(0, 100) : null, valorNum,
             vencimento, vencimento_ajustado || null, null, null, linha_fluxo || null, novo_grupo_id]
          );
          inserted.push(r.rows[0]);
        }

        if (Array.isArray(itens) && itens.length > 0) {
          for (const item of itens) {
            const qty = parseFloat(item.quantidade) || 0;
            const cu = parseFloat(item.custo_un) || 0;
            await client.query(
              `INSERT INTO pedidos_compra_itens (grupo_id, empresa, sku, quantidade, custo_un, custo_total)
               VALUES ($1,$2,$3,$4,$5,$6)`,
              [novo_grupo_id, company, String(item.sku).substring(0, 100), qty, cu, Math.round(qty * cu * 100) / 100]
            );
          }
        }

        await client.query('COMMIT');
        return res.json({ ok: true, pedidos: inserted });
      } catch (e) {
        await client.query('ROLLBACK'); throw e;
      } finally { client.release(); }
    }

    if (req.method === 'DELETE') {
      const { id } = req.query;
      if (!id) return res.status(400).json({ error: 'Informe id' });
      // Find grupo_id to delete all parcelas
      const origR = await pool.query('SELECT grupo_id FROM pedidos_compra WHERE id=$1 AND empresa=$2', [parseInt(id), company]);
      const gid = origR.rows[0]?.grupo_id;
      if (gid) {
        await pool.query('DELETE FROM pedidos_compra WHERE grupo_id=$1 AND empresa=$2', [gid, company]);
        await pool.query('DELETE FROM pedidos_compra_itens WHERE grupo_id=$1 AND empresa=$2', [gid, company]);
      } else {
        await pool.query('DELETE FROM pedidos_compra WHERE id=$1 AND empresa=$2', [parseInt(id), company]);
      }
      return res.json({ ok: true });
    }

    res.status(405).json({ error: 'Method not allowed' });
  } catch (e) {
    console.error('[PEDIDOS-COMPRA]', e.message);
    res.status(500).json({ error: e.message });
  }
};
