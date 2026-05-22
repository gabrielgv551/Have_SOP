// ── Agente de Estoque — definição de tools e executor ────────────────────────

const { safeQuery } = require('./shared');

// ─── ESTOQUE TOOLS DEFINITIONS ───────────────────────────────────────────────

const ESTOQUE_TOOLS = [
  {
    name: 'portfolio_summary',
    description: 'Totais do portfólio: SKUs cadastrados, com/sem venda em 1m e 6m.',
    input_schema: { type: 'object', properties: {} },
  },
  {
    name: 'sku_tendencia',
    description: 'Tendência de volume por SKU (1m vs 3m, 3m vs 6m) + receita.',
    input_schema: {
      type: 'object',
      properties: {
        limite: { type: 'number' },
        apenas_acelerando: { type: 'boolean' },
        apenas_desacelerando: { type: 'boolean' },
      },
    },
  },
  {
    name: 'sku_saude_multidimensional',
    description: 'Por SKU: tendência, dias cobertura, margem%, curva ABC, estoque.',
    input_schema: {
      type: 'object',
      properties: {
        skus: { type: 'array', items: { type: 'string' } },
        limite: { type: 'number' },
      },
    },
  },
  {
    name: 'pareto',
    description: 'Quantos SKUs = 80% da receita e 80% da margem.',
    input_schema: { type: 'object', properties: {} },
  },
  {
    name: 'skus_orfaos',
    description: 'SKUs com estoque > 0 sem venda em N dias. Capital imobilizado em R$.',
    input_schema: {
      type: 'object',
      properties: { dias_sem_venda: { type: 'number' } },
    },
  },
  {
    name: 'recomendacao_compra',
    description: 'Qtd e custo R$ para atingir N dias de cobertura. Lead time 15d.',
    input_schema: {
      type: 'object',
      properties: {
        dias_meta_cobertura: { type: 'number' },
        apenas_acelerando: { type: 'boolean' },
      },
    },
  },
  {
    name: 'sazonalidade_yoy',
    description: 'Mês atual vs mesmo mês ano anterior por SKU.',
    input_schema: {
      type: 'object',
      properties: { limite: { type: 'number' } },
    },
  },
  {
    name: 'caixa_disponivel',
    description: 'Saldo de caixa vs custo de reposição dos SKUs críticos.',
    input_schema: { type: 'object', properties: {} },
  },
  {
    name: 'concentracao_canal',
    description: 'Receita por canal de venda por SKU. Risco se >70% num canal.',
    input_schema: {
      type: 'object',
      properties: { skus: { type: 'array', items: { type: 'string' } } },
    },
  },
  {
    name: 'executar_sql',
    description: 'SELECT com LIMIT. Use $1 para filtrar por empresa quando a tabela tiver coluna company/empresa.',
    input_schema: {
      type: 'object',
      properties: { sql: { type: 'string' } },
      required: ['sql'],
    },
  },
  {
    name: 'analise_completa',
    description: 'Executa portfolio_summary + pareto + skus_orfaos + recomendacao_compra + caixa_disponivel em paralelo. Use para análise geral ou quando o gestor pedir visão completa do estoque.',
    input_schema: {
      type: 'object',
      properties: {
        dias_orfaos: { type: 'number' },
        dias_meta_cobertura: { type: 'number' },
      },
    },
  },
];

// ─── TOOL EXECUTOR — ESTOQUE ─────────────────────────────────────────────────

async function executeEstoqueTool(toolName, input, pool, company) {
  const BLOCKED = ['usuarios', 'belvo_links'];

  switch (toolName) {

    case 'portfolio_summary': {
      const [totals, cv1m, cv6m] = await Promise.all([
        safeQuery(pool, `SELECT COUNT(*) AS total FROM cadastros_sku`),
        safeQuery(pool, `
          WITH max_d AS (SELECT MAX("Data"::date) AS d FROM bd_vendas)
          SELECT COUNT(DISTINCT UPPER(TRIM(c."Sku"::text))) AS com_venda
          FROM cadastros_sku c
          INNER JOIN bd_vendas v
            ON UPPER(TRIM(v."Sku"::text)) = UPPER(TRIM(c."Sku"::text))
          WHERE v."Data"::date >= (SELECT d FROM max_d) - INTERVAL '1 month'
            AND v."Status" !~* '(cancel|devol|n[aã]o.?pago)'`),
        safeQuery(pool, `
          WITH max_d AS (SELECT MAX("Data"::date) AS d FROM bd_vendas)
          SELECT COUNT(DISTINCT UPPER(TRIM(c."Sku"::text))) AS com_venda
          FROM cadastros_sku c
          INNER JOIN bd_vendas v
            ON UPPER(TRIM(v."Sku"::text)) = UPPER(TRIM(c."Sku"::text))
          WHERE v."Data"::date >= (SELECT d FROM max_d) - INTERVAL '6 months'
            AND v."Status" !~* '(cancel|devol|n[aã]o.?pago)'`),
      ]);
      const total = parseInt(totals[0]?.total) || 0;
      const c1m   = parseInt(cv1m[0]?.com_venda) || 0;
      const c6m   = parseInt(cv6m[0]?.com_venda) || 0;
      return {
        total_cadastrados: total,
        com_venda_1m: c1m,
        sem_venda_1m: total - c1m,
        com_venda_6m: c6m,
        sem_venda_6m: total - c6m,
        pct_ativo_1m: total > 0 ? ((c1m / total) * 100).toFixed(1) + '%' : 'N/A',
        pct_ativo_6m: total > 0 ? ((c6m / total) * 100).toFixed(1) + '%' : 'N/A',
      };
    }

    case 'sku_tendencia': {
      const lim = Math.min(input.limite || 50, 100);
      const filtroAcel = input.apenas_acelerando  ? 'AND q_1m > media_3m * 1.20' : '';
      const filtroDesc = input.apenas_desacelerando ? 'AND q_1m < media_3m * 0.80' : '';
      return await safeQuery(pool, `
        WITH max_d AS (SELECT MAX("Data"::date) AS d FROM bd_vendas),
        base AS (
          SELECT "Sku" AS sku,
            MAX("Nome Produto") AS nome,
            ROUND(SUM(CASE WHEN "Data"::date >= (SELECT d FROM max_d) - INTERVAL '1 month'
              AND "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN "Quantidade Vendida"::numeric ELSE 0 END), 0) AS q_1m,
            ROUND(SUM(CASE WHEN "Data"::date >= (SELECT d FROM max_d) - INTERVAL '3 months'
              AND "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN "Quantidade Vendida"::numeric ELSE 0 END) / 3.0, 1) AS media_3m,
            ROUND(SUM(CASE WHEN "Data"::date >= (SELECT d FROM max_d) - INTERVAL '6 months'
              AND "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN "Quantidade Vendida"::numeric ELSE 0 END) / 6.0, 1) AS media_6m,
            ROUND(SUM(CASE WHEN "Data"::date >= (SELECT d FROM max_d) - INTERVAL '1 month'
              AND "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN "Total Venda"::numeric ELSE 0 END), 2) AS receita_1m
          FROM bd_vendas
          WHERE "Sku" IS NOT NULL AND TRIM("Sku"::text) != ''
            AND "Data"::date >= (SELECT d FROM max_d) - INTERVAL '12 months'
          GROUP BY "Sku"
        )
        SELECT sku, nome, q_1m AS qtd_1m, media_3m, media_6m, receita_1m,
          CASE WHEN media_3m > 0 THEN ROUND((q_1m - media_3m) / media_3m * 100, 1) ELSE NULL END AS tend_1m_vs_3m_pct,
          CASE WHEN media_6m > 0 THEN ROUND((media_3m - media_6m) / media_6m * 100, 1) ELSE NULL END AS tend_3m_vs_6m_pct
        FROM base
        WHERE media_3m > 0 ${filtroAcel} ${filtroDesc}
        ORDER BY tend_1m_vs_3m_pct DESC NULLS LAST
        LIMIT ${lim}
      `);
    }

    case 'sku_saude_multidimensional': {
      const lim = Math.min(input.limite || 30, 80);
      const skuArr = (input.skus || []).map(s => s.replace(/'/g, "''"));
      const skuFilter = skuArr.length
        ? `AND v."Sku" = ANY(ARRAY[${skuArr.map(s => `'${s}'`).join(',')}])`
        : '';
      return await safeQuery(pool, `
        WITH max_d AS (SELECT MAX("Data"::date) AS d FROM bd_vendas),
        vendas AS (
          SELECT v."Sku" AS sku,
            MAX(v."Nome Produto") AS nome,
            ROUND(SUM(CASE WHEN v."Data"::date >= (SELECT d FROM max_d) - INTERVAL '1 month'
              AND v."Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN v."Quantidade Vendida"::numeric ELSE 0 END), 0) AS q_1m,
            ROUND(SUM(CASE WHEN v."Data"::date >= (SELECT d FROM max_d) - INTERVAL '3 months'
              AND v."Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN v."Quantidade Vendida"::numeric ELSE 0 END) / 3.0, 1) AS media_3m,
            ROUND(SUM(CASE WHEN v."Data"::date >= (SELECT d FROM max_d) - INTERVAL '6 months'
              AND v."Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN v."Quantidade Vendida"::numeric ELSE 0 END) / 6.0, 1) AS media_6m,
            ROUND(SUM(CASE WHEN v."Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN v."Total Venda"::numeric ELSE 0 END) / 12.0, 2) AS receita_media_mensal,
            ROUND(
              SUM(CASE WHEN v."Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN COALESCE(v."Margem Produto"::numeric, 0) ELSE 0 END) /
              NULLIF(SUM(CASE WHEN v."Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN v."Total Venda"::numeric ELSE 0 END), 0) * 100, 1
            ) AS margem_pct
          FROM bd_vendas v
          WHERE v."Sku" IS NOT NULL AND TRIM(v."Sku"::text) != ''
            AND v."Data"::date >= (SELECT d FROM max_d) - INTERVAL '12 months'
            ${skuFilter}
          GROUP BY v."Sku"
        ),
        pp AS (
          SELECT UPPER(TRIM("SKU"::text)) AS sku_k,
            SUM("Estoque Base"::numeric) AS estoque
          FROM estoque_consolidado
          WHERE "SKU" IS NOT NULL AND TRIM("SKU"::text) != ''
          GROUP BY UPPER(TRIM("SKU"::text))
        ),
        abc AS (
          SELECT UPPER(TRIM(COALESCE("Sku"::text, ''))) AS sku_k,
            MAX(COALESCE("Curva"::text, '?')) AS curva
          FROM curva_abc
          WHERE "Ano" = (SELECT MAX("Ano") FROM curva_abc)
          GROUP BY 1
        )
        SELECT v.sku, v.nome,
          v.q_1m AS qtd_1m, v.media_3m, v.media_6m,
          v.receita_media_mensal, v.margem_pct,
          COALESCE(p.estoque, 0) AS estoque_atual,
          CASE WHEN v.media_3m > 0
            THEN ROUND(COALESCE(p.estoque, 0) / (v.media_3m / 30), 0)
            ELSE NULL END AS dias_cobertura,
          CASE WHEN v.media_3m > 0
            THEN ROUND((v.q_1m - v.media_3m) / v.media_3m * 100, 1)
            ELSE NULL END AS tend_1m_vs_3m_pct,
          CASE WHEN v.media_6m > 0
            THEN ROUND((v.media_3m - v.media_6m) / v.media_6m * 100, 1)
            ELSE NULL END AS tend_3m_vs_6m_pct,
          COALESCE(a.curva, '?') AS curva_abc,
          ROUND(v.media_3m * 12, 0) AS giro_anual_estimado
        FROM vendas v
        LEFT JOIN pp p ON UPPER(TRIM(v.sku)) = p.sku_k
        LEFT JOIN abc a ON UPPER(TRIM(v.sku)) = a.sku_k
        WHERE v.media_3m > 0
        ORDER BY v.receita_media_mensal DESC
        LIMIT ${lim}
      `);
    }

    case 'pareto': {
      const rows = await safeQuery(pool, `
        WITH max_d AS (SELECT MAX("Data"::date) AS d FROM bd_vendas),
        sku_agg AS (
          SELECT "Sku" AS sku,
            SUM(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN "Total Venda"::numeric ELSE 0 END) AS receita,
            SUM(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN COALESCE("Margem Produto"::numeric, 0) ELSE 0 END) AS margem
          FROM bd_vendas
          WHERE "Data"::date >= (SELECT d FROM max_d) - INTERVAL '3 months' AND "Sku" IS NOT NULL
          GROUP BY "Sku"
        ),
        totais AS (SELECT SUM(receita) AS tr, SUM(margem) AS tm, COUNT(*) AS n FROM sku_agg WHERE receita > 0),
        ranked AS (
          SELECT sku, receita, margem,
            SUM(receita) OVER (ORDER BY receita DESC) / NULLIF((SELECT tr FROM totais), 0) * 100 AS acum_rec_pct,
            SUM(margem) OVER (ORDER BY margem DESC) / NULLIF((SELECT tm FROM totais), 0) * 100 AS acum_mg_pct
          FROM sku_agg WHERE receita > 0
        )
        SELECT
          (SELECT COUNT(*) FROM ranked WHERE acum_rec_pct <= 80) AS skus_80pct_receita,
          (SELECT COUNT(*) FROM ranked WHERE acum_mg_pct  <= 80) AS skus_80pct_margem,
          (SELECT n FROM totais) AS total_skus_com_venda,
          ROUND((SELECT tr FROM totais), 2) AS receita_total_3m,
          ROUND((SELECT tm FROM totais), 2) AS margem_total_3m
        FROM totais
      `);
      return rows[0] || {};
    }

    case 'skus_orfaos': {
      const dias = Math.min(input.dias_sem_venda || 90, 365);
      const rows = await safeQuery(pool, `
        WITH ativos AS (
          SELECT DISTINCT UPPER(TRIM("Sku"::text)) AS sku
          FROM bd_vendas
          WHERE "Data"::date >= CURRENT_DATE - INTERVAL '${dias} days'
            AND "Status" !~* '(cancel|devol|n[aã]o.?pago)'
        ),
        estoques AS (
          SELECT UPPER(TRIM("SKU"::text)) AS sku,
            SUM("Estoque Base"::numeric) AS estoque
          FROM estoque_consolidado
          WHERE "SKU" IS NOT NULL AND TRIM("SKU"::text) != ''
          GROUP BY UPPER(TRIM("SKU"::text))
          HAVING SUM("Estoque Base"::numeric) > 0
        ),
        custo AS (
          SELECT UPPER(TRIM("Sku"::text)) AS sku,
            AVG("Custo Total"::numeric / NULLIF("Quantidade Vendida"::numeric, 0)) AS custo_unit
          FROM bd_vendas
          WHERE "Custo Total" IS NOT NULL AND "Quantidade Vendida"::numeric > 0
          GROUP BY 1
        )
        SELECT e.sku, e.estoque AS estoque_atual,
          ROUND(COALESCE(c.custo_unit, 0), 2) AS custo_unitario,
          ROUND(e.estoque * COALESCE(c.custo_unit, 0), 2) AS capital_imobilizado_R$
        FROM estoques e
        LEFT JOIN ativos a ON e.sku = a.sku
        LEFT JOIN custo c ON e.sku = c.sku
        WHERE a.sku IS NULL
        ORDER BY capital_imobilizado_R$ DESC NULLS LAST
        LIMIT 30
      `);
      const total_capital = rows.reduce((s, r) => s + (parseFloat(r['capital_imobilizado_R$']) || 0), 0);
      return { dias_sem_venda: dias, total_skus_orfaos: rows.length, capital_total_imobilizado_R$: total_capital.toFixed(2), skus: rows };
    }

    case 'recomendacao_compra': {
      const diasMeta = input.dias_meta_cobertura || 60;
      const rows = await safeQuery(pool, `
        WITH max_d AS (SELECT MAX("Data"::date) AS d FROM bd_vendas),
        vendas AS (
          SELECT "Sku" AS sku, MAX("Nome Produto") AS nome,
            SUM(CASE WHEN "Data"::date >= (SELECT d FROM max_d) - INTERVAL '3 months'
              AND "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN "Quantidade Vendida"::numeric ELSE 0 END) / 3.0 AS media_3m,
            AVG(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' AND "Quantidade Vendida"::numeric > 0
              THEN "Custo Total"::numeric / NULLIF("Quantidade Vendida"::numeric, 0) ELSE NULL END) AS custo_unit
          FROM bd_vendas
          WHERE "Sku" IS NOT NULL AND "Data"::date >= (SELECT d FROM max_d) - INTERVAL '12 months'
          GROUP BY "Sku"
          HAVING SUM(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN "Quantidade Vendida"::numeric ELSE 0 END) > 0
        ),
        pp AS (
          SELECT UPPER(TRIM("SKU"::text)) AS sku_k,
            SUM("Estoque Base"::numeric) AS estoque
          FROM estoque_consolidado
          WHERE "SKU" IS NOT NULL AND TRIM("SKU"::text) != ''
          GROUP BY UPPER(TRIM("SKU"::text))
        )
        SELECT v.sku, v.nome,
          ROUND(v.media_3m, 1) AS media_mensal,
          COALESCE(p.estoque, 0) AS estoque_atual,
          CASE WHEN v.media_3m > 0 THEN ROUND(COALESCE(p.estoque, 0) / (v.media_3m / 30), 0) ELSE NULL END AS dias_cobertura_atual,
          GREATEST(0, ROUND(v.media_3m / 30.0 * ${diasMeta} - COALESCE(p.estoque, 0), 0)) AS qtd_comprar,
          ROUND(GREATEST(0, v.media_3m / 30.0 * ${diasMeta} - COALESCE(p.estoque, 0)) * COALESCE(v.custo_unit, 0), 2) AS custo_R$,
          (CURRENT_DATE + INTERVAL '15 days')::date AS data_limite_pedido
        FROM vendas v
        LEFT JOIN pp p ON UPPER(TRIM(v.sku)) = p.sku_k
        WHERE v.media_3m > 0
          AND (COALESCE(p.estoque, 0) / NULLIF(v.media_3m / 30.0, 0)) < ${diasMeta}
        ORDER BY custo_R$ DESC NULLS LAST
        LIMIT 25
      `);
      const custo_total = rows.reduce((s, r) => s + (parseFloat(r['custo_R$']) || 0), 0);
      return { dias_meta_cobertura: diasMeta, lead_time_dias: 15, custo_total_R$: custo_total.toFixed(2), recomendacoes: rows };
    }

    case 'sazonalidade_yoy': {
      const lim = Math.min(input.limite || 30, 80);
      return await safeQuery(pool, `
        WITH max_d AS (SELECT MAX("Data"::date) AS d FROM bd_vendas),
        ref AS (
          SELECT EXTRACT(MONTH FROM (SELECT d FROM max_d))::int AS m,
                 EXTRACT(YEAR  FROM (SELECT d FROM max_d))::int AS a
        ),
        atual AS (
          SELECT "Sku" AS sku, MAX("Nome Produto") AS nome,
            SUM("Quantidade Vendida"::numeric) AS qtd,
            SUM("Total Venda"::numeric) AS rec
          FROM bd_vendas
          WHERE "Status" !~* '(cancel|devol|n[aã]o.?pago)'
            AND "Mes" = (SELECT m FROM ref) AND "Ano" = (SELECT a FROM ref)
          GROUP BY "Sku"
        ),
        anterior AS (
          SELECT "Sku" AS sku,
            SUM("Quantidade Vendida"::numeric) AS qtd,
            SUM("Total Venda"::numeric) AS rec
          FROM bd_vendas
          WHERE "Status" !~* '(cancel|devol|n[aã]o.?pago)'
            AND "Mes" = (SELECT m FROM ref) AND "Ano" = (SELECT a FROM ref) - 1
          GROUP BY "Sku"
        )
        SELECT a.sku, a.nome,
          ROUND(a.qtd, 0) AS qtd_atual,
          ROUND(ant.qtd, 0) AS qtd_ano_anterior,
          CASE WHEN ant.qtd > 0
            THEN ROUND((a.qtd - ant.qtd) / ant.qtd * 100, 1)
            ELSE NULL END AS variacao_yoy_pct,
          ROUND(a.rec, 2) AS receita_atual_R$
        FROM atual a
        LEFT JOIN anterior ant ON a.sku = ant.sku
        ORDER BY ABS(COALESCE((a.qtd - ant.qtd) / NULLIF(ant.qtd, 0), 0)) DESC
        LIMIT ${lim}
      `);
    }

    case 'caixa_disponivel': {
      const [caixa, criticos] = await Promise.all([
        safeQuery(pool, `
          SELECT ROUND(SUM(valor::numeric) / 100.0, 2) AS saldo_R$
          FROM caixa_extrato
          WHERE ano = (SELECT MAX(ano) FROM caixa_extrato)
            AND mes = (SELECT MAX(mes) FROM caixa_extrato WHERE ano=(SELECT MAX(ano) FROM caixa_extrato))`),
        safeQuery(pool, `
          WITH max_d AS (SELECT MAX("Data"::date) AS d FROM bd_vendas),
          media AS (
            SELECT "Sku" AS sku,
              SUM(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN "Quantidade Vendida"::numeric ELSE 0 END) / 3.0 AS media_3m,
              AVG(CASE WHEN "Quantidade Vendida"::numeric > 0
                THEN "Custo Total"::numeric / NULLIF("Quantidade Vendida"::numeric, 0) ELSE NULL END) AS custo_unit
            FROM bd_vendas
            WHERE "Data"::date >= (SELECT d FROM max_d) - INTERVAL '3 months'
            GROUP BY "Sku"
          )
          SELECT ROUND(SUM(GREATEST(0, m.media_3m * 2 - COALESCE(ec.estoque, 0)) * COALESCE(m.custo_unit, 0)), 2) AS custo_reposicao_criticos_R$
          FROM ponto_pedido pp
          LEFT JOIN (
            SELECT UPPER(TRIM("SKU"::text)) AS sku_k, SUM("Estoque Base"::numeric) AS estoque
            FROM estoque_consolidado WHERE "SKU" IS NOT NULL AND TRIM("SKU"::text) != ''
            GROUP BY UPPER(TRIM("SKU"::text))
          ) ec ON UPPER(TRIM(pp.sku::text)) = ec.sku_k
          LEFT JOIN media m ON UPPER(TRIM(pp.sku::text)) = UPPER(TRIM(m.sku))
          WHERE pp.alerta IN ('CRÍTICO', 'ATENÇÃO')`),
      ]);
      const saldo = parseFloat(caixa[0]?.['saldo_R$']) || 0;
      const custo = parseFloat(criticos[0]?.['custo_reposicao_criticos_R$']) || 0;
      return {
        saldo_caixa_R$: saldo.toFixed(2),
        custo_reposicao_criticos_R$: custo.toFixed(2),
        folga_R$: (saldo - custo).toFixed(2),
        status: saldo >= custo ? 'CAIXA_SUFICIENTE' : 'CAIXA_INSUFICIENTE',
      };
    }

    case 'concentracao_canal': {
      const skuArr = (input.skus || []).map(s => s.replace(/'/g, "''"));
      const skuFilter = skuArr.length
        ? `AND "Sku" = ANY(ARRAY[${skuArr.map(s => `'${s}'`).join(',')}])`
        : '';
      return await safeQuery(pool, `
        WITH max_d AS (SELECT MAX("Data"::date) AS d FROM bd_vendas),
        base AS (
          SELECT "Sku" AS sku,
            COALESCE(NULLIF(TRIM("Canal Apelido"::text), ''), TRIM("Canal de venda"::text), 'Sem canal') AS canal,
            SUM(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN "Total Venda"::numeric ELSE 0 END) AS receita
          FROM bd_vendas
          WHERE "Data"::date >= (SELECT d FROM max_d) - INTERVAL '3 months'
            AND "Sku" IS NOT NULL ${skuFilter}
          GROUP BY 1, 2
        ),
        totais AS (SELECT sku, SUM(receita) AS total FROM base GROUP BY sku)
        SELECT b.sku, b.canal,
          ROUND(b.receita, 2) AS receita_R$,
          ROUND(b.receita / NULLIF(t.total, 0) * 100, 1) AS pct_canal,
          CASE WHEN b.receita / NULLIF(t.total, 0) > 0.7 THEN 'RISCO_CONCENTRACAO' ELSE 'OK' END AS risco
        FROM base b JOIN totais t ON b.sku = t.sku
        WHERE b.receita > 0
        ORDER BY b.sku, b.receita DESC
        LIMIT 100
      `);
    }

    case 'executar_sql': {
      const sql = (input.sql || '').trim();
      if (!/^\s*SELECT\s/i.test(sql)) return { error: 'Apenas queries SELECT são permitidas' };
      if (!/LIMIT\s+\d+/i.test(sql)) return { error: 'Query deve conter LIMIT para evitar dumps grandes. Máximo recomendado: 100' };
      if (BLOCKED.some(t => sql.toLowerCase().includes(t))) return { error: 'Tabela bloqueada para agentes' };
      return await safeQuery(pool, sql, company ? [company] : []);
    }

    case 'analise_completa': {
      const [portfolio, pareto, orfaos, compras, caixa] = await Promise.all([
        executeEstoqueTool('portfolio_summary', {}, pool, company),
        executeEstoqueTool('pareto', {}, pool, company),
        executeEstoqueTool('skus_orfaos', { dias_sem_venda: input.dias_orfaos || 90 }, pool, company),
        executeEstoqueTool('recomendacao_compra', { dias_meta_cobertura: input.dias_meta_cobertura || 60 }, pool, company),
        executeEstoqueTool('caixa_disponivel', {}, pool, company),
      ]);
      return { portfolio_summary: portfolio, pareto, skus_orfaos: orfaos, recomendacao_compra: compras, caixa_disponivel: caixa };
    }

    default:
      return { error: `Tool '${toolName}' não reconhecida` };
  }
}

module.exports = { ESTOQUE_TOOLS, executeEstoqueTool };
