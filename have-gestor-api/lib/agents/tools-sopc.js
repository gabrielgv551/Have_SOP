// ── Agente S&OP — definição de tools e executor ───────────────────────────────

const { safeQuery, getSopcParams } = require('./shared');

// ─── SOPC TOOLS DEFINITIONS ──────────────────────────────────────────────────

const SOPC_TOOLS = [
  {
    name: 'sopc_portfolio_saude',
    description: 'Saúde do portfólio completo (base bd_vendas + ponto_pedido). Por SKU: dias_cobertura, tendencia_pct, receita_media_mensal real (média 3m), status. Limite 200 SKUs por receita.',
    input_schema: {
      type: 'object',
      properties: {
        limite: { type: 'number', description: 'Máximo de SKUs (padrão 200)' },
        status_filtro: { type: 'string', description: 'Filtrar por status: RUPTURA, RUPTURA_IMINENTE, ABAIXO_META_30D, RISCO_ENCALHE, OK, SEM_ESTOQUE_CADASTRADO' },
      },
    },
  },
  {
    name: 'sopc_rupturas_impacto',
    description: 'Foca SKUs com dias_cobertura < 15 ou estoque = 0 que venderam nos últimos 3m. Retorna receita_em_risco_30d, qtd_repor e custo_reposicao por SKU. Ordenado por curva ABC + receita.',
    input_schema: { type: 'object', properties: {} },
  },
  {
    name: 'sopc_reposicao_priorizada',
    description: 'Plano de compras S&OP com metas configuradas por empresa (lidas do sopc_config). Retorna qtd_comprar, custo_R$, data_limite_pedido e urgencia (CRITICO/URGENTE/PROGRAMADO) por SKU, subtotais e total de investimento. Os parâmetros dias_meta_a/b/c são opcionais — se omitidos, usa a config da empresa.',
    input_schema: {
      type: 'object',
      properties: {
        dias_meta_a: { type: 'number', description: 'Sobrescreve meta curva A da config da empresa' },
        dias_meta_b: { type: 'number', description: 'Sobrescreve meta curva B da config da empresa' },
        dias_meta_c: { type: 'number', description: 'Sobrescreve meta curva C da config da empresa' },
      },
    },
  },
  {
    name: 'sopc_diagnostico_base',
    description: 'Diagnóstico de qualidade dos dados: SKUs em cadastros_sku, bd_vendas, ponto_pedido. Mostra quantos SKUs vendem sem ponto_pedido cadastrado e vice-versa. Resolve "94% sem dados".',
    input_schema: { type: 'object', properties: {} },
  },
  {
    name: 'sopc_encalhe_risco',
    description: 'SKUs com dias_cobertura > 120 e tendência < -15%. Capital imobilizado em R$ por SKU. Classificação: ENCALHE_CRITICO | ENCALHE_ATENCAO | MONITORAR.',
    input_schema: { type: 'object', properties: {} },
  },
  {
    name: 'sopc_analise_completa',
    description: 'Executa sopc_portfolio_saude + sopc_rupturas_impacto + sopc_reposicao_priorizada + sopc_diagnostico_base + sopc_encalhe_risco em paralelo. Use para análise geral S&OP.',
    input_schema: {
      type: 'object',
      properties: {
        dias_meta_a: { type: 'number' },
        dias_meta_b: { type: 'number' },
        dias_meta_c: { type: 'number' },
      },
    },
  },
];

// ─── TOOL EXECUTOR — SOP ─────────────────────────────────────────────────────

async function executeSopcTool(toolName, input, pool, company) {
  const STATUS_FILTER_SQL = `"Status" !~* '(cancel|devol|n[aã]o.?pago)'`;
  const p = await getSopcParams(pool, company);

  switch (toolName) {

    case 'sopc_portfolio_saude': {
      const lim = Math.min(input.limite || 200, 500);
      const rows = await safeQuery(pool, `
        WITH max_d AS (SELECT MAX("Data"::date) AS d FROM bd_vendas),
        base AS (
          SELECT v."Sku" AS sku,
            MAX(v."Nome Produto") AS nome,
            MAX(v."Categoria") AS categoria,
            ROUND(SUM(CASE WHEN v."Data"::date >= (SELECT d FROM max_d) - INTERVAL '1 month'
              AND ${STATUS_FILTER_SQL} THEN v."Quantidade Vendida"::numeric ELSE 0 END), 0) AS qtd_1m,
            ROUND(SUM(CASE WHEN v."Data"::date >= (SELECT d FROM max_d) - INTERVAL '3 months'
              AND ${STATUS_FILTER_SQL} THEN v."Quantidade Vendida"::numeric ELSE 0 END) / 3.0, 1) AS media_3m,
            ROUND(SUM(CASE WHEN v."Data"::date >= (SELECT d FROM max_d) - INTERVAL '6 months'
              AND ${STATUS_FILTER_SQL} THEN v."Quantidade Vendida"::numeric ELSE 0 END) / 6.0, 1) AS media_6m,
            ROUND(SUM(CASE WHEN v."Data"::date >= (SELECT d FROM max_d) - INTERVAL '3 months'
              AND ${STATUS_FILTER_SQL} THEN v."Total Venda"::numeric ELSE 0 END) / 3.0, 2) AS receita_media_mensal,
            ROUND(
              SUM(CASE WHEN ${STATUS_FILTER_SQL} THEN COALESCE(v."Margem Produto"::numeric, 0) ELSE 0 END) /
              NULLIF(SUM(CASE WHEN ${STATUS_FILTER_SQL} THEN v."Total Venda"::numeric ELSE 0 END), 0) * 100, 1
            ) AS margem_pct
          FROM bd_vendas v
          WHERE v."Sku" IS NOT NULL AND TRIM(v."Sku"::text) != ''
            AND v."Data"::date >= (SELECT d FROM max_d) - INTERVAL '12 months'
          GROUP BY v."Sku"
        ),
        pp AS (
          SELECT UPPER(TRIM("SKU"::text)) AS sku_k,
            SUM("Estoque Base"::numeric) AS estoque
          FROM estoque_consolidado
          WHERE "SKU" IS NOT NULL AND TRIM("SKU"::text) != ''
          GROUP BY UPPER(TRIM("SKU"::text))
        ),
        pp_abc AS (
          SELECT UPPER(TRIM(sku::text)) AS sku_k, abc_cruzada
          FROM ponto_pedido WHERE sku IS NOT NULL
        )
        SELECT b.sku, b.nome, b.categoria,
          b.qtd_1m, b.media_3m, b.media_6m,
          b.receita_media_mensal, b.margem_pct,
          CASE WHEN p.sku_k IS NOT NULL THEN COALESCE(p.estoque, 0) ELSE NULL END AS estoque_atual,
          COALESCE(pa.abc_cruzada, '?') AS curva_abc,
          CASE WHEN b.media_3m > 0 AND p.sku_k IS NOT NULL
            THEN ROUND(COALESCE(p.estoque, 0) / (b.media_3m / 30.0), 0)
            ELSE NULL END AS dias_cobertura,
          CASE WHEN b.media_3m > 0
            THEN ROUND((b.qtd_1m - b.media_3m) / b.media_3m * 100, 1)
            ELSE NULL END AS tendencia_pct,
          CASE
            WHEN p.sku_k IS NULL THEN 'SEM_ESTOQUE_CADASTRADO'
            WHEN COALESCE(p.estoque, 0) <= 0 THEN 'RUPTURA'
            WHEN b.media_3m > 0 AND COALESCE(p.estoque, 0) / (b.media_3m / 30.0) < ${p.alerta_ruptura_dias} THEN 'RUPTURA_IMINENTE'
            WHEN b.media_3m > 0 AND COALESCE(p.estoque, 0) / (b.media_3m / 30.0) < ${p.alerta_abaixo_meta} THEN 'ABAIXO_META'
            WHEN b.media_3m > 0 AND COALESCE(p.estoque, 0) / (b.media_3m / 30.0) > ${p.encalhe_dias}
              AND b.qtd_1m < b.media_3m * 0.85 THEN 'RISCO_ENCALHE'
            ELSE 'OK'
          END AS status
        FROM base b
        LEFT JOIN pp p ON UPPER(TRIM(b.sku)) = p.sku_k
        LEFT JOIN pp_abc pa ON UPPER(TRIM(b.sku)) = pa.sku_k
        ORDER BY b.receita_media_mensal DESC
        LIMIT ${lim}
      `);

      const statusFiltro = input.status_filtro;
      const filtered = statusFiltro ? rows.filter(r => r.status === statusFiltro) : rows;
      const resumo = {};
      rows.forEach(r => { resumo[r.status] = (resumo[r.status] || 0) + 1; });
      return { total_skus: rows.length, resumo_por_status: resumo, skus: filtered };
    }

    case 'sopc_rupturas_impacto': {
      const rows = await safeQuery(pool, `
        WITH max_d AS (SELECT MAX("Data"::date) AS d FROM bd_vendas),
        base AS (
          SELECT v."Sku" AS sku,
            MAX(v."Nome Produto") AS nome,
            ROUND(SUM(CASE WHEN v."Data"::date >= (SELECT d FROM max_d) - INTERVAL '3 months'
              AND ${STATUS_FILTER_SQL} THEN v."Quantidade Vendida"::numeric ELSE 0 END) / 3.0, 1) AS media_3m,
            ROUND(SUM(CASE WHEN v."Data"::date >= (SELECT d FROM max_d) - INTERVAL '3 months'
              AND ${STATUS_FILTER_SQL} THEN v."Total Venda"::numeric ELSE 0 END) / 3.0, 2) AS receita_media_mensal,
            AVG(CASE WHEN ${STATUS_FILTER_SQL} AND v."Quantidade Vendida"::numeric > 0
              THEN v."Custo Total"::numeric / NULLIF(v."Quantidade Vendida"::numeric, 0) ELSE NULL END) AS custo_unit
          FROM bd_vendas v
          WHERE v."Sku" IS NOT NULL AND TRIM(v."Sku"::text) != ''
            AND v."Data"::date >= (SELECT d FROM max_d) - INTERVAL '3 months'
          GROUP BY v."Sku"
          HAVING SUM(CASE WHEN ${STATUS_FILTER_SQL} THEN v."Quantidade Vendida"::numeric ELSE 0 END) > 0
        ),
        pp AS (
          SELECT UPPER(TRIM("SKU"::text)) AS sku_k,
            SUM("Estoque Base"::numeric) AS estoque
          FROM estoque_consolidado
          WHERE "SKU" IS NOT NULL AND TRIM("SKU"::text) != ''
          GROUP BY UPPER(TRIM("SKU"::text))
        ),
        pp_abc AS (
          SELECT UPPER(TRIM(sku::text)) AS sku_k, abc_cruzada
          FROM ponto_pedido WHERE sku IS NOT NULL
        )
        SELECT b.sku, b.nome,
          COALESCE(pa.abc_cruzada, '?') AS curva_abc,
          COALESCE(p.estoque, 0) AS estoque_atual,
          CASE WHEN b.media_3m > 0
            THEN ROUND(COALESCE(p.estoque, 0) / (b.media_3m / 30.0), 0)
            ELSE 0 END AS dias_cobertura,
          b.receita_media_mensal,
          CASE WHEN b.media_3m > 0
            THEN ROUND(b.receita_media_mensal * (COALESCE(p.estoque, 0) / (b.media_3m / 30.0)) / 30.0, 2)
            ELSE b.receita_media_mensal END AS receita_em_risco_30d,
          GREATEST(0, ROUND(b.media_3m / 30.0 * ${p.meta_a} - COALESCE(p.estoque, 0), 0)) AS qtd_repor,
          ROUND(GREATEST(0, b.media_3m / 30.0 * ${p.meta_a} - COALESCE(p.estoque, 0)) * COALESCE(b.custo_unit, 0), 2) AS custo_reposicao,
          CASE WHEN b.media_3m > 0
            THEN ROUND((SELECT SUM(CASE WHEN vv."Data"::date >= (SELECT d FROM max_d) - INTERVAL '1 month'
              AND ${STATUS_FILTER_SQL} THEN vv."Quantidade Vendida"::numeric ELSE 0 END)
              FROM bd_vendas vv WHERE vv."Sku" = v2."Sku") - b.media_3m) / b.media_3m * 100, 1)
            ELSE NULL END AS tendencia_pct
        FROM base b
        LEFT JOIN pp p ON UPPER(TRIM(b.sku)) = p.sku_k
        LEFT JOIN pp_abc pa ON UPPER(TRIM(b.sku)) = pa.sku_k
        CROSS JOIN LATERAL (SELECT b.sku AS dummy_sku) v2(sku)
        WHERE (COALESCE(p.estoque, 0) = 0 OR (b.media_3m > 0 AND COALESCE(p.estoque, 0) / (b.media_3m / 30.0) < ${p.alerta_ruptura_dias}))
        ORDER BY pa.abc_cruzada ASC NULLS LAST, b.receita_media_mensal DESC
        LIMIT 50
      `);
      const total_risco      = rows.reduce((s, r) => s + (parseFloat(r.receita_em_risco_30d) || 0), 0);
      const total_reposicao  = rows.reduce((s, r) => s + (parseFloat(r.custo_reposicao) || 0), 0);
      return {
        total_skus_em_risco: rows.length,
        receita_total_em_risco_R$: total_risco.toFixed(2),
        custo_total_reposicao_R$: total_reposicao.toFixed(2),
        skus: rows,
      };
    }

    case 'sopc_reposicao_priorizada': {
      const diasA = input.dias_meta_a || p.meta_a;
      const diasB = input.dias_meta_b || p.meta_b;
      const diasC = input.dias_meta_c || p.meta_c;
      const rows = await safeQuery(pool, `
        WITH max_d AS (SELECT MAX("Data"::date) AS d FROM bd_vendas),
        base AS (
          SELECT v."Sku" AS sku,
            MAX(v."Nome Produto") AS nome,
            ROUND(SUM(CASE WHEN v."Data"::date >= (SELECT d FROM max_d) - INTERVAL '3 months'
              AND ${STATUS_FILTER_SQL} THEN v."Quantidade Vendida"::numeric ELSE 0 END) / 3.0, 1) AS media_3m,
            AVG(CASE WHEN ${STATUS_FILTER_SQL} AND v."Quantidade Vendida"::numeric > 0
              THEN v."Custo Total"::numeric / NULLIF(v."Quantidade Vendida"::numeric, 0) ELSE NULL END) AS custo_unit
          FROM bd_vendas v
          WHERE v."Sku" IS NOT NULL AND TRIM(v."Sku"::text) != ''
            AND v."Data"::date >= (SELECT d FROM max_d) - INTERVAL '12 months'
          GROUP BY v."Sku"
          HAVING SUM(CASE WHEN ${STATUS_FILTER_SQL} THEN v."Quantidade Vendida"::numeric ELSE 0 END) > 0
        ),
        pp AS (
          SELECT UPPER(TRIM("SKU"::text)) AS sku_k,
            SUM("Estoque Base"::numeric) AS estoque
          FROM estoque_consolidado
          WHERE "SKU" IS NOT NULL AND TRIM("SKU"::text) != ''
          GROUP BY UPPER(TRIM("SKU"::text))
        ),
        pp_abc AS (
          SELECT UPPER(TRIM(sku::text)) AS sku_k, abc_cruzada
          FROM ponto_pedido WHERE sku IS NOT NULL
        )
        SELECT b.sku, b.nome,
          COALESCE(pa.abc_cruzada, 'C') AS curva_abc,
          ROUND(b.media_3m, 1) AS media_mensal,
          COALESCE(p.estoque, 0) AS estoque_atual,
          CASE WHEN b.media_3m > 0
            THEN ROUND(COALESCE(p.estoque, 0) / (b.media_3m / 30.0), 0)
            ELSE NULL END AS dias_cobertura_atual,
          CASE COALESCE(pa.abc_cruzada, 'C')
            WHEN 'A' THEN ${diasA}
            WHEN 'B' THEN ${diasB}
            ELSE ${diasC}
          END AS dias_meta,
          GREATEST(0, ROUND(b.media_3m / 30.0 *
            CASE COALESCE(pa.abc_cruzada, 'C') WHEN 'A' THEN ${diasA} WHEN 'B' THEN ${diasB} ELSE ${diasC} END
            - COALESCE(p.estoque, 0), 0)) AS qtd_comprar,
          ROUND(
            GREATEST(0, b.media_3m / 30.0 *
              CASE COALESCE(pa.abc_cruzada, 'C') WHEN 'A' THEN ${diasA} WHEN 'B' THEN ${diasB} ELSE ${diasC} END
              - COALESCE(p.estoque, 0)) * COALESCE(b.custo_unit, 0), 2
          ) AS custo_R$,
          (CURRENT_DATE + INTERVAL '15 days')::date AS data_limite_pedido,
          CASE
            WHEN b.media_3m > 0 AND COALESCE(p.estoque, 0) / (b.media_3m / 30.0) < ${p.alerta_ruptura_dias} THEN 'CRITICO'
            WHEN b.media_3m > 0 AND COALESCE(p.estoque, 0) / (b.media_3m / 30.0) < ${p.alerta_abaixo_meta} THEN 'URGENTE'
            ELSE 'PROGRAMADO'
          END AS urgencia
        FROM base b
        LEFT JOIN pp p ON UPPER(TRIM(b.sku)) = p.sku_k
        LEFT JOIN pp_abc pa ON UPPER(TRIM(b.sku)) = pa.sku_k
        WHERE b.media_3m > 0
          AND GREATEST(0, b.media_3m / 30.0 *
            CASE COALESCE(pa.abc_cruzada, 'C') WHEN 'A' THEN ${diasA} WHEN 'B' THEN ${diasB} ELSE ${diasC} END
            - COALESCE(p.estoque, 0)) > 0
        ORDER BY
          CASE WHEN b.media_3m > 0 AND COALESCE(p.estoque, 0) / (b.media_3m / 30.0) < ${p.alerta_ruptura_dias} THEN 0
               WHEN b.media_3m > 0 AND COALESCE(p.estoque, 0) / (b.media_3m / 30.0) < ${p.alerta_abaixo_meta} THEN 1
               ELSE 2 END,
          COALESCE(pa.abc_cruzada, 'C') ASC,
          custo_R$ DESC NULLS LAST
        LIMIT 100
      `);
      const subtotais = {};
      let totalGeral = 0;
      rows.forEach(r => {
        const u = r.urgencia;
        if (!subtotais[u]) subtotais[u] = { quantidade_skus: 0, custo_total_R$: 0 };
        subtotais[u].quantidade_skus++;
        subtotais[u].custo_total_R$ += parseFloat(r['custo_R$']) || 0;
        totalGeral += parseFloat(r['custo_R$']) || 0;
      });
      Object.keys(subtotais).forEach(k => { subtotais[k].custo_total_R$ = subtotais[k].custo_total_R$.toFixed(2); });
      return {
        metas_dias: { A: diasA, B: diasB, C: diasC },
        lead_time_dias: p.lead_time,
        total_investimento_R$: totalGeral.toFixed(2),
        subtotais_por_urgencia: subtotais,
        recomendacoes: rows,
      };
    }

    case 'sopc_diagnostico_base': {
      const [totalSku, comVenda3m, totalPP, ppSemVenda, vendaSemPP, desativados] = await Promise.all([
        safeQuery(pool, `SELECT COUNT(*) AS total FROM cadastros_sku`),
        safeQuery(pool, `
          WITH max_d AS (SELECT MAX("Data"::date) AS d FROM bd_vendas)
          SELECT COUNT(DISTINCT "Sku") AS total
          FROM bd_vendas
          WHERE "Sku" IS NOT NULL AND ${STATUS_FILTER_SQL}
            AND "Data"::date >= (SELECT d FROM max_d) - INTERVAL '3 months'
        `),
        safeQuery(pool, `SELECT COUNT(*) AS total FROM ponto_pedido WHERE sku IS NOT NULL`),
        safeQuery(pool, `
          WITH max_d AS (SELECT MAX("Data"::date) AS d FROM bd_vendas),
          vendas3m AS (
            SELECT DISTINCT UPPER(TRIM("Sku"::text)) AS sku
            FROM bd_vendas
            WHERE "Sku" IS NOT NULL AND ${STATUS_FILTER_SQL}
              AND "Data"::date >= (SELECT d FROM max_d) - INTERVAL '3 months'
          )
          SELECT COUNT(*) AS total
          FROM ponto_pedido pp
          LEFT JOIN vendas3m v ON UPPER(TRIM(pp.sku::text)) = v.sku
          WHERE v.sku IS NULL AND pp.sku IS NOT NULL
        `),
        safeQuery(pool, `
          WITH max_d AS (SELECT MAX("Data"::date) AS d FROM bd_vendas),
          vendas3m AS (
            SELECT DISTINCT UPPER(TRIM("Sku"::text)) AS sku
            FROM bd_vendas
            WHERE "Sku" IS NOT NULL AND ${STATUS_FILTER_SQL}
              AND "Data"::date >= (SELECT d FROM max_d) - INTERVAL '3 months'
          )
          SELECT COUNT(*) AS total
          FROM vendas3m v
          LEFT JOIN ponto_pedido pp ON v.sku = UPPER(TRIM(pp.sku::text))
          WHERE pp.sku IS NULL
        `),
        safeQuery(pool, `SELECT COUNT(*) AS total FROM sku_desativadas WHERE empresa = $1`, [company]),
      ]);
      const tot    = parseInt(totalSku[0]?.total)    || 0;
      const vend   = parseInt(comVenda3m[0]?.total)  || 0;
      const pp     = parseInt(totalPP[0]?.total)     || 0;
      const ppSV   = parseInt(ppSemVenda[0]?.total)  || 0;
      const vSP    = parseInt(vendaSemPP[0]?.total)  || 0;
      const desativ = parseInt(desativados[0]?.total) || 0;
      return {
        total_cadastros_sku: tot,
        total_com_venda_3m: vend,
        total_em_ponto_pedido: pp,
        skus_ponto_pedido_sem_venda: ppSV,
        skus_com_venda_sem_ponto_pedido: vSP,
        skus_desativados: desativ,
        cobertura_pct: vend > 0 ? +((( vend - vSP) / vend) * 100).toFixed(1) : null,
        diagnostico: {
          portafolio_base_bd_vendas: vend,
          gap_sem_ponto_pedido: `${vSP} SKUs vendendo mas sem cadastro em ponto_pedido`,
          gap_estoque_parado: `${ppSV} SKUs em ponto_pedido sem venda nos últimos 3m`,
        },
      };
    }

    case 'sopc_encalhe_risco': {
      const rows = await safeQuery(pool, `
        WITH max_d AS (SELECT MAX("Data"::date) AS d FROM bd_vendas),
        base AS (
          SELECT v."Sku" AS sku,
            MAX(v."Nome Produto") AS nome,
            ROUND(SUM(CASE WHEN v."Data"::date >= (SELECT d FROM max_d) - INTERVAL '3 months'
              AND ${STATUS_FILTER_SQL} THEN v."Quantidade Vendida"::numeric ELSE 0 END) / 3.0, 1) AS media_3m,
            ROUND(SUM(CASE WHEN v."Data"::date >= (SELECT d FROM max_d) - INTERVAL '1 month'
              AND ${STATUS_FILTER_SQL} THEN v."Quantidade Vendida"::numeric ELSE 0 END), 0) AS qtd_1m,
            ROUND(SUM(CASE WHEN v."Data"::date >= (SELECT d FROM max_d) - INTERVAL '3 months'
              AND ${STATUS_FILTER_SQL} THEN v."Total Venda"::numeric ELSE 0 END) / 3.0, 2) AS receita_media_mensal,
            AVG(CASE WHEN ${STATUS_FILTER_SQL} AND v."Quantidade Vendida"::numeric > 0
              THEN v."Custo Total"::numeric / NULLIF(v."Quantidade Vendida"::numeric, 0) ELSE NULL END) AS custo_unit
          FROM bd_vendas v
          WHERE v."Sku" IS NOT NULL AND TRIM(v."Sku"::text) != ''
            AND v."Data"::date >= (SELECT d FROM max_d) - INTERVAL '6 months'
          GROUP BY v."Sku"
        ),
        pp AS (
          SELECT UPPER(TRIM("SKU"::text)) AS sku_k,
            SUM("Estoque Base"::numeric) AS estoque
          FROM estoque_consolidado
          WHERE "SKU" IS NOT NULL AND TRIM("SKU"::text) != ''
          GROUP BY UPPER(TRIM("SKU"::text))
          HAVING SUM("Estoque Base"::numeric) > 0
        ),
        pp_abc AS (
          SELECT UPPER(TRIM(sku::text)) AS sku_k, abc_cruzada
          FROM ponto_pedido WHERE sku IS NOT NULL
        )
        SELECT b.sku, b.nome,
          COALESCE(pa.abc_cruzada, '?') AS curva_abc,
          p.estoque AS estoque_atual,
          ROUND(p.estoque / (b.media_3m / 30.0), 0) AS dias_cobertura,
          b.media_3m,
          CASE WHEN b.media_3m > 0 THEN ROUND((b.qtd_1m - b.media_3m) / b.media_3m * 100, 1) ELSE NULL END AS tendencia_pct,
          ROUND(COALESCE(b.custo_unit, 0), 2) AS custo_unitario_medio,
          ROUND(p.estoque * COALESCE(b.custo_unit, 0), 2) AS capital_imobilizado_R$,
          b.receita_media_mensal,
          CASE
            WHEN COALESCE(pa.abc_cruzada, 'C') = 'C' AND p.estoque / (b.media_3m / 30.0) > 180 THEN 'ENCALHE_CRITICO'
            WHEN p.estoque / (b.media_3m / 30.0) > 180 THEN 'ENCALHE_ATENCAO'
            ELSE 'MONITORAR'
          END AS classificacao
        FROM base b
        JOIN pp p ON UPPER(TRIM(b.sku)) = p.sku_k
        LEFT JOIN pp_abc pa ON UPPER(TRIM(b.sku)) = pa.sku_k
        WHERE b.media_3m > 0
          AND p.estoque / (b.media_3m / 30.0) > 120
          AND b.qtd_1m < b.media_3m * 0.85
        ORDER BY capital_imobilizado_R$ DESC NULLS LAST
        LIMIT 50
      `);
      const total_capital = rows.reduce((s, r) => s + (parseFloat(r['capital_imobilizado_R$']) || 0), 0);
      return {
        total_skus_encalhe: rows.length,
        total_capital_imobilizado_R$: total_capital.toFixed(2),
        skus: rows,
      };
    }

    case 'sopc_analise_completa': {
      const [portfolio, rupturas, reposicao, diagnostico, encalhe] = await Promise.all([
        executeSopcTool('sopc_portfolio_saude', { limite: 200 }, pool, company),
        executeSopcTool('sopc_rupturas_impacto', {}, pool, company),
        executeSopcTool('sopc_reposicao_priorizada', {
          dias_meta_a: input.dias_meta_a || p.meta_a,
          dias_meta_b: input.dias_meta_b || p.meta_b,
          dias_meta_c: input.dias_meta_c || p.meta_c,
        }, pool, company),
        executeSopcTool('sopc_diagnostico_base', {}, pool, company),
        executeSopcTool('sopc_encalhe_risco', {}, pool, company),
      ]);
      const portfolioCompacto = {
        total_skus: portfolio.total_skus,
        resumo_por_status: portfolio.resumo_por_status,
        top_criticos: (portfolio.skus || []).filter(s => ['RUPTURA','RUPTURA_IMINENTE','ABAIXO_META'].includes(s.status)).slice(0, 30),
        nota: 'Para lista completa de SKUs chame sopc_portfolio_saude individualmente',
      };
      return {
        parametros_sop: { meta_dias: { A: p.meta_a, B: p.meta_b, C: p.meta_c }, alerta_ruptura_dias: p.alerta_ruptura_dias, alerta_abaixo_meta_dias: p.alerta_abaixo_meta, encalhe_dias: p.encalhe_dias, lead_time_dias: p.lead_time },
        sopc_portfolio_saude: portfolioCompacto,
        sopc_rupturas_impacto: { ...rupturas, skus: (rupturas.skus || []).slice(0, 30) },
        sopc_reposicao_priorizada: { ...reposicao, recomendacoes: (reposicao.recomendacoes || []).slice(0, 40) },
        sopc_diagnostico_base: diagnostico,
        sopc_encalhe_risco: { ...encalhe, skus: (encalhe.skus || []).slice(0, 20) },
      };
    }

    default:
      return { error: `Tool SOP '${toolName}' não reconhecida` };
  }
}

module.exports = { SOPC_TOOLS, executeSopcTool };
