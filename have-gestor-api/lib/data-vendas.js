const { getPool, getCompanyPool } = require('./db');

module.exports = async function handleVendas(req, res, payload) {

  // Módulo Margens · DRE Gerencial
  if (req.query.module === 'margens') {
    const { company, pool } = getCompanyPool(payload);
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

  // Módulo Vendas
  if (req.query.module === 'vendas') {
    const { company, pool } = getCompanyPool(payload);
    const { action } = req.query;
    try {
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
    const { company, pool } = getCompanyPool(payload);
    try {
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

  // ── Módulo Previsão de Recebimentos ──────────────────────────────────
  if (req.query.module === 'forecast-recebimentos') {
    const { company, pool } = getCompanyPool(payload);
    try {
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
    const { company, pool } = getCompanyPool(payload);
    const mes = req.query.mes || '';
    try {
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
};
