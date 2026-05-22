// ── Agente Vendas — definição de tools e executor ────────────────────────────

const { safeQuery } = require('./shared');

// ─── VENDAS TOOLS DEFINITIONS ────────────────────────────────────────────────

const VENDAS_TOOLS = [
  {
    name: 'vendas_kpi_periodo',
    description: 'KPIs do mês: receita, qtd, margem, pedidos, SKUs ativos, ticket médio, variação MoM e YoY.',
    input_schema: {
      type: 'object',
      properties: {
        ano: { type: 'number' },
        mes: { type: 'number' },
      },
    },
  },
  {
    name: 'vendas_diagnostico_queda',
    description: 'Decomposição da variação MoM em efeito volume, efeito ticket e efeito mix. Identifica canais/categorias que puxaram para baixo, SKUs que sumiram e novos SKUs.',
    input_schema: {
      type: 'object',
      properties: {
        ano: { type: 'number' },
        mes: { type: 'number' },
      },
    },
  },
  {
    name: 'vendas_margem_real_por_canal',
    description: 'Margem bruta e margem pós-comissão por canal de venda. Classifica canal como LUCRATIVO / MARGINAL / PREJUIZO. Top 40 SKUs por canal.',
    input_schema: {
      type: 'object',
      properties: { meses: { type: 'number' } },
    },
  },
  {
    name: 'vendas_canibalismo_portfolio',
    description: 'Pares de SKUs da mesma categoria com preço similar que apresentam inversão de vendas no mesmo mês. Índice de canibalismo 0-100%.',
    input_schema: {
      type: 'object',
      properties: { meses: { type: 'number' } },
    },
  },
  {
    name: 'vendas_sazonalidade_historica',
    description: 'Índice de sazonalidade por mês (1.0 = média histórica). Compara mês atual vs histórico e classifica como sazonalidade_normal / queda_dentro_da_sazonalidade / queda_estrutural.',
    input_schema: { type: 'object', properties: {} },
  },
  {
    name: 'vendas_cohort_skus',
    description: 'Cohort de SKUs: quais SKUs existiam no período base e sumiram, quais apareceram. Impacto em R$ dos SKUs perdidos vs ganhos.',
    input_schema: {
      type: 'object',
      properties: {
        periodo_atual_ano: { type: 'number' },
        periodo_atual_mes: { type: 'number' },
        periodo_base_ano:  { type: 'number' },
        periodo_base_mes:  { type: 'number' },
      },
    },
  },
  {
    name: 'vendas_concentracao_risco',
    description: 'Top 15 SKUs por receita + participação %. Índice Herfindahl, canal mais dependente. Classifica risco como ALTO/MEDIO/BAIXO.',
    input_schema: {
      type: 'object',
      properties: { meses: { type: 'number' } },
    },
  },
  {
    name: 'vendas_analise_completa',
    description: 'Executa vendas_kpi_periodo + vendas_sazonalidade_historica + vendas_cohort_skus + vendas_concentracao_risco em paralelo. Use para análise geral de vendas.',
    input_schema: {
      type: 'object',
      properties: {
        ano: { type: 'number' },
        mes: { type: 'number' },
      },
    },
  },
];

// ─── Constante auxiliar ──────────────────────────────────────────────────────

const CANAL_COMISSAO_SQL = `CASE
  WHEN LOWER(COALESCE(NULLIF(TRIM("Canal Apelido"::text),''), TRIM("Canal de venda"::text), '')) LIKE '%mercado livre%' THEN 0.16
  WHEN LOWER(COALESCE(NULLIF(TRIM("Canal Apelido"::text),''), TRIM("Canal de venda"::text), '')) LIKE '%shopee%' THEN 0.12
  WHEN LOWER(COALESCE(NULLIF(TRIM("Canal Apelido"::text),''), TRIM("Canal de venda"::text), '')) LIKE '%amazon%' THEN 0.15
  ELSE 0.12
END`;

// ─── TOOL EXECUTOR — VENDAS ──────────────────────────────────────────────────

async function executeVendasTool(toolName, input, pool, company) {
  const SF    = `"Status" !~* '(cancel|devol|n[aã]o.?pago)'`;
  const CANAL = `COALESCE(NULLIF(TRIM("Canal Apelido"::text),''), TRIM("Canal de venda"::text), 'Sem canal')`;

  switch (toolName) {

    case 'vendas_kpi_periodo': {
      const rows = await safeQuery(pool, `
        WITH ref AS (
          SELECT COALESCE($1::int, MAX("Ano")) AS ano,
                 COALESCE($2::int, MAX("Mes")) AS mes
          FROM bd_vendas
        ),
        cur AS (
          SELECT ROUND(SUM(CASE WHEN ${SF} THEN "Total Venda"::numeric ELSE 0 END),2) AS receita,
                 ROUND(SUM(CASE WHEN ${SF} THEN "Quantidade Vendida"::numeric ELSE 0 END),0) AS qtd,
                 ROUND(SUM(CASE WHEN ${SF} THEN COALESCE("Margem Produto"::numeric,0) ELSE 0 END),2) AS margem,
                 COUNT(DISTINCT CASE WHEN ${SF} THEN "Order ID" END) AS pedidos,
                 COUNT(DISTINCT CASE WHEN ${SF} THEN "Sku" END) AS skus_ativos
          FROM bd_vendas, ref
          WHERE "Ano"=ref.ano AND "Mes"=ref.mes
        ),
        prev AS (
          SELECT ROUND(SUM(CASE WHEN ${SF} THEN "Total Venda"::numeric ELSE 0 END),2) AS receita,
                 ROUND(SUM(CASE WHEN ${SF} THEN "Quantidade Vendida"::numeric ELSE 0 END),0) AS qtd
          FROM bd_vendas, ref
          WHERE ("Ano"=ref.ano AND "Mes"=ref.mes-1)
             OR ("Ano"=ref.ano-1 AND "Mes"=12 AND ref.mes=1)
        ),
        yoy AS (
          SELECT ROUND(SUM(CASE WHEN ${SF} THEN "Total Venda"::numeric ELSE 0 END),2) AS receita
          FROM bd_vendas, ref
          WHERE "Ano"=ref.ano-1 AND "Mes"=ref.mes
        )
        SELECT ref.ano, ref.mes,
          cur.receita, cur.qtd, cur.margem, cur.pedidos, cur.skus_ativos,
          ROUND(cur.margem / NULLIF(cur.receita,0) * 100, 1) AS margem_bruta_pct,
          ROUND(cur.receita / NULLIF(cur.qtd,0), 2) AS ticket_medio,
          ROUND(cur.receita / NULLIF(cur.skus_ativos,0), 2) AS receita_por_sku,
          ROUND((cur.receita - prev.receita) / NULLIF(prev.receita,0) * 100, 1) AS variacao_mom_pct,
          ROUND((cur.receita - yoy.receita) / NULLIF(yoy.receita,0) * 100, 1) AS variacao_yoy_pct
        FROM ref, cur, prev, yoy
      `, [input.ano || null, input.mes || null]);
      return rows[0] || { error: 'Sem dados para o período' };
    }

    case 'vendas_diagnostico_queda': {
      const rows = await safeQuery(pool, `
        WITH ref AS (
          SELECT COALESCE($1::int, MAX("Ano")) AS ano,
                 COALESCE($2::int, MAX("Mes")) AS mes
          FROM bd_vendas
        ),
        cur AS (
          SELECT "Sku" AS sku,
            MAX("Nome Produto") AS nome,
            MAX(${CANAL}) AS canal,
            MAX("Categoria") AS categoria,
            SUM(CASE WHEN ${SF} THEN "Total Venda"::numeric ELSE 0 END) AS receita,
            SUM(CASE WHEN ${SF} THEN "Quantidade Vendida"::numeric ELSE 0 END) AS qtd
          FROM bd_vendas, ref WHERE "Ano"=ref.ano AND "Mes"=ref.mes
          GROUP BY "Sku"
        ),
        prv AS (
          SELECT "Sku" AS sku,
            SUM(CASE WHEN ${SF} THEN "Total Venda"::numeric ELSE 0 END) AS receita,
            SUM(CASE WHEN ${SF} THEN "Quantidade Vendida"::numeric ELSE 0 END) AS qtd
          FROM bd_vendas, ref
          WHERE ("Ano"=ref.ano AND "Mes"=ref.mes-1)
             OR ("Ano"=ref.ano-1 AND "Mes"=12 AND ref.mes=1)
          GROUP BY "Sku"
        ),
        totais AS (
          SELECT
            COALESCE(SUM(c.receita),0) AS rec_cur, COALESCE(SUM(p.receita),0) AS rec_prv,
            COALESCE(SUM(c.qtd),0) AS qtd_cur,   COALESCE(SUM(p.qtd),0) AS qtd_prv,
            COALESCE(SUM(p.receita),0) - COALESCE(SUM(c.receita),0) AS queda_total
          FROM cur c FULL JOIN prv p ON c.sku=p.sku
        )
        SELECT
          ROUND((t.rec_cur - t.rec_prv) / NULLIF(t.rec_prv,0) * 100, 1) AS variacao_mom_pct,
          ROUND(t.rec_cur,2) AS receita_atual, ROUND(t.rec_prv,2) AS receita_anterior,
          ROUND(t.queda_total,2) AS queda_total_R$,
          ROUND((t.qtd_cur - t.qtd_prv) * (t.rec_prv / NULLIF(t.qtd_prv,0)), 2) AS efeito_volume_R$,
          ROUND((t.rec_cur/NULLIF(t.qtd_cur,0) - t.rec_prv/NULLIF(t.qtd_prv,0)) * t.qtd_cur, 2) AS efeito_ticket_R$
        FROM totais t
      `, [input.ano || null, input.mes || null]);

      const [canalRows, catRows, sumiramRows, aparecRow] = await Promise.all([
        safeQuery(pool, `
          WITH ref AS (SELECT COALESCE($1::int, MAX("Ano")) AS ano, COALESCE($2::int, MAX("Mes")) AS mes FROM bd_vendas),
          cur AS (SELECT ${CANAL} AS canal, SUM(CASE WHEN ${SF} THEN "Total Venda"::numeric ELSE 0 END) AS r FROM bd_vendas,ref WHERE "Ano"=ref.ano AND "Mes"=ref.mes GROUP BY 1),
          prv AS (SELECT ${CANAL} AS canal, SUM(CASE WHEN ${SF} THEN "Total Venda"::numeric ELSE 0 END) AS r FROM bd_vendas,ref WHERE ("Ano"=ref.ano AND "Mes"=ref.mes-1) OR ("Ano"=ref.ano-1 AND "Mes"=12 AND ref.mes=1) GROUP BY 1)
          SELECT c.canal, ROUND(c.r,2) AS receita_cur, ROUND(p.r,2) AS receita_prv,
            ROUND((c.r-p.r)/NULLIF(p.r,0)*100,1) AS variacao_pct
          FROM cur c LEFT JOIN prv p ON c.canal=p.canal ORDER BY variacao_pct ASC NULLS LAST LIMIT 5
        `, [input.ano || null, input.mes || null]),
        safeQuery(pool, `
          WITH ref AS (SELECT COALESCE($1::int, MAX("Ano")) AS ano, COALESCE($2::int, MAX("Mes")) AS mes FROM bd_vendas),
          cur AS (SELECT "Categoria" AS cat, SUM(CASE WHEN ${SF} THEN "Total Venda"::numeric ELSE 0 END) AS r FROM bd_vendas,ref WHERE "Ano"=ref.ano AND "Mes"=ref.mes GROUP BY 1),
          prv AS (SELECT "Categoria" AS cat, SUM(CASE WHEN ${SF} THEN "Total Venda"::numeric ELSE 0 END) AS r FROM bd_vendas,ref WHERE ("Ano"=ref.ano AND "Mes"=ref.mes-1) OR ("Ano"=ref.ano-1 AND "Mes"=12 AND ref.mes=1) GROUP BY 1)
          SELECT c.cat AS categoria, ROUND(c.r,2) AS receita_cur, ROUND(p.r,2) AS receita_prv,
            ROUND((c.r-p.r)/NULLIF(p.r,0)*100,1) AS variacao_pct
          FROM cur c LEFT JOIN prv p ON c.cat=p.cat ORDER BY variacao_pct ASC NULLS LAST LIMIT 5
        `, [input.ano || null, input.mes || null]),
        safeQuery(pool, `
          WITH ref AS (SELECT COALESCE($1::int, MAX("Ano")) AS ano, COALESCE($2::int, MAX("Mes")) AS mes FROM bd_vendas),
          prv_skus AS (SELECT DISTINCT "Sku" AS sku, MAX("Nome Produto") AS nome, SUM(CASE WHEN ${SF} THEN "Total Venda"::numeric ELSE 0 END) AS receita FROM bd_vendas,ref WHERE ("Ano"=ref.ano AND "Mes"=ref.mes-1) OR ("Ano"=ref.ano-1 AND "Mes"=12 AND ref.mes=1) GROUP BY "Sku"),
          cur_skus AS (SELECT DISTINCT "Sku" AS sku FROM bd_vendas,ref WHERE "Ano"=ref.ano AND "Mes"=ref.mes AND ${SF})
          SELECT p.sku, p.nome, ROUND(p.receita,2) AS receita_anterior
          FROM prv_skus p WHERE p.sku NOT IN (SELECT sku FROM cur_skus) AND p.receita > 0
          ORDER BY p.receita DESC LIMIT 10
        `, [input.ano || null, input.mes || null]),
        safeQuery(pool, `
          WITH ref AS (SELECT COALESCE($1::int, MAX("Ano")) AS ano, COALESCE($2::int, MAX("Mes")) AS mes FROM bd_vendas),
          cur_skus AS (SELECT "Sku" AS sku, MAX("Nome Produto") AS nome, SUM(CASE WHEN ${SF} THEN "Total Venda"::numeric ELSE 0 END) AS receita FROM bd_vendas,ref WHERE "Ano"=ref.ano AND "Mes"=ref.mes AND ${SF} GROUP BY "Sku"),
          prv_skus AS (SELECT DISTINCT "Sku" AS sku FROM bd_vendas,ref WHERE ("Ano"=ref.ano AND "Mes"=ref.mes-1) OR ("Ano"=ref.ano-1 AND "Mes"=12 AND ref.mes=1))
          SELECT c.sku, c.nome, ROUND(c.receita,2) AS receita_atual
          FROM cur_skus c WHERE c.sku NOT IN (SELECT sku FROM prv_skus) AND c.receita > 0
          ORDER BY c.receita DESC LIMIT 10
        `, [input.ano || null, input.mes || null]),
      ]);

      const base   = rows[0] || {};
      const efVol  = parseFloat(base.efeito_volume_R$) || 0;
      const efTick = parseFloat(base.efeito_ticket_R$) || 0;
      const queda  = parseFloat(base['queda_total_R$']) || 0;
      return {
        ...base,
        efeito_mix_R$: +(queda - efVol - efTick).toFixed(2),
        canal_por_variacao: canalRows,
        categoria_por_variacao: catRows,
        skus_que_sumiram: sumiramRows,
        skus_que_apareceram: aparecRow,
      };
    }

    case 'vendas_margem_real_por_canal': {
      const meses = input.meses || 3;
      const [canalRows, skuRows] = await Promise.all([
        safeQuery(pool, `
          WITH max_d AS (SELECT MAX("Data"::date) AS d FROM bd_vendas),
          base AS (
            SELECT ${CANAL} AS canal,
              SUM(CASE WHEN ${SF} THEN "Total Venda"::numeric ELSE 0 END) AS receita,
              SUM(CASE WHEN ${SF} THEN COALESCE("Margem Produto"::numeric,0) ELSE 0 END) AS margem_bruta,
              AVG(${CANAL_COMISSAO_SQL}) AS comissao_pct
            FROM bd_vendas
            WHERE "Data"::date >= (SELECT d FROM max_d) - ($1 || ' months')::interval
            GROUP BY ${CANAL}
          )
          SELECT canal,
            ROUND(receita,2) AS receita_R$,
            ROUND(margem_bruta / NULLIF(receita,0) * 100, 1) AS margem_bruta_pct,
            ROUND(comissao_pct * 100, 1) AS comissao_pct,
            ROUND((margem_bruta - receita * comissao_pct) / NULLIF(receita,0) * 100, 1) AS margem_pos_comissao_pct,
            ROUND(margem_bruta - receita * comissao_pct, 2) AS margem_pos_comissao_R$,
            CASE
              WHEN (margem_bruta - receita * comissao_pct) / NULLIF(receita,0) > 0.10 THEN 'LUCRATIVO'
              WHEN (margem_bruta - receita * comissao_pct) / NULLIF(receita,0) > 0 THEN 'MARGINAL'
              ELSE 'PREJUIZO'
            END AS status
          FROM base ORDER BY receita DESC
        `, [meses]),
        safeQuery(pool, `
          WITH max_d AS (SELECT MAX("Data"::date) AS d FROM bd_vendas),
          base AS (
            SELECT "Sku" AS sku, MAX("Nome Produto") AS nome,
              ${CANAL} AS canal,
              SUM(CASE WHEN ${SF} THEN "Total Venda"::numeric ELSE 0 END) AS receita,
              SUM(CASE WHEN ${SF} THEN COALESCE("Margem Produto"::numeric,0) ELSE 0 END) AS margem_bruta,
              AVG(${CANAL_COMISSAO_SQL}) AS comissao_pct
            FROM bd_vendas
            WHERE "Data"::date >= (SELECT d FROM max_d) - ($1 || ' months')::interval
              AND "Sku" IS NOT NULL
            GROUP BY "Sku", ${CANAL}
          )
          SELECT sku, nome, canal,
            ROUND(receita,2) AS receita_R$,
            ROUND(margem_bruta / NULLIF(receita,0) * 100, 1) AS margem_bruta_pct,
            ROUND(comissao_pct * 100, 1) AS comissao_pct,
            ROUND((margem_bruta - receita * comissao_pct) / NULLIF(receita,0) * 100, 1) AS margem_pos_comissao_pct,
            CASE
              WHEN (margem_bruta - receita * comissao_pct) / NULLIF(receita,0) > 0.10 THEN 'LUCRATIVO'
              WHEN (margem_bruta - receita * comissao_pct) / NULLIF(receita,0) > 0 THEN 'MARGINAL'
              ELSE 'PREJUIZO'
            END AS status
          FROM base WHERE receita > 0
          ORDER BY receita DESC LIMIT 40
        `, [meses]),
      ]);
      return { janela_meses: meses, por_canal: canalRows, por_sku_canal: skuRows };
    }

    case 'vendas_canibalismo_portfolio': {
      const meses = input.meses || 6;
      const rows = await safeQuery(pool, `
        WITH max_d AS (SELECT MAX("Data"::date) AS d FROM bd_vendas),
        mensal AS (
          SELECT "Sku" AS sku, MAX("Nome Produto") AS nome,
            MAX("Categoria") AS categoria,
            DATE_TRUNC('month',"Data"::date) AS mes,
            SUM(CASE WHEN ${SF} THEN "Total Venda"::numeric ELSE 0 END) AS receita,
            AVG(CASE WHEN ${SF} AND "Quantidade Vendida"::numeric>0
              THEN "Total Venda"::numeric/"Quantidade Vendida"::numeric END) AS preco_medio
          FROM bd_vendas
          WHERE "Data"::date >= (SELECT d FROM max_d) - ($1 || ' months')::interval
            AND "Sku" IS NOT NULL
          GROUP BY "Sku", DATE_TRUNC('month',"Data"::date)
        ),
        skus_base AS (
          SELECT sku, nome, categoria,
            AVG(preco_medio) AS preco_avg,
            COUNT(DISTINCT mes) AS meses_com_venda
          FROM mensal GROUP BY sku, nome, categoria
          HAVING COUNT(DISTINCT mes) >= 3
        ),
        pares AS (
          SELECT a.sku AS sku_a, a.nome AS nome_a, b.sku AS sku_b, b.nome AS nome_b,
            a.categoria,
            ROUND(a.preco_avg::numeric,2) AS preco_a,
            ROUND(b.preco_avg::numeric,2) AS preco_b
          FROM skus_base a JOIN skus_base b ON a.categoria=b.categoria AND a.sku < b.sku
            AND ABS(a.preco_avg - b.preco_avg) / NULLIF(GREATEST(a.preco_avg, b.preco_avg),0) <= 0.20
        ),
        inversoes AS (
          SELECT p.sku_a, p.sku_b, p.categoria, p.nome_a, p.nome_b, p.preco_a, p.preco_b,
            COUNT(*) AS meses_analisados,
            SUM(CASE WHEN
              (ma.receita > LAG(ma.receita) OVER (PARTITION BY ma.sku ORDER BY ma.mes)
               AND mb.receita < LAG(mb.receita) OVER (PARTITION BY mb.sku ORDER BY mb.mes))
              OR
              (ma.receita < LAG(ma.receita) OVER (PARTITION BY ma.sku ORDER BY ma.mes)
               AND mb.receita > LAG(mb.receita) OVER (PARTITION BY mb.sku ORDER BY mb.mes))
              THEN 1 ELSE 0 END) AS meses_inversao
          FROM pares p
          JOIN mensal ma ON ma.sku=p.sku_a
          JOIN mensal mb ON mb.sku=p.sku_b AND mb.mes=ma.mes
          GROUP BY p.sku_a, p.sku_b, p.categoria, p.nome_a, p.nome_b, p.preco_a, p.preco_b
        )
        SELECT sku_a, nome_a, sku_b, nome_b, categoria, preco_a, preco_b,
          meses_inversao, meses_analisados,
          ROUND(meses_inversao::numeric / NULLIF(meses_analisados-1,0) * 100, 0) AS indice_canibalismo_pct
        FROM inversoes WHERE meses_analisados > 2
        ORDER BY indice_canibalismo_pct DESC NULLS LAST LIMIT 20
      `, [meses]);
      return { janela_meses: meses, total_pares: rows.length, pares_canibais: rows };
    }

    case 'vendas_sazonalidade_historica': {
      const rows = await safeQuery(pool, `
        WITH max_d AS (SELECT MAX("Data"::date) AS d FROM bd_vendas),
        mensal AS (
          SELECT "Ano" AS ano, "Mes" AS mes,
            SUM(CASE WHEN ${SF} THEN "Total Venda"::numeric ELSE 0 END) AS receita
          FROM bd_vendas GROUP BY "Ano","Mes"
        ),
        media_global AS (
          SELECT AVG(receita) AS avg_global FROM mensal
        ),
        media_mes AS (
          SELECT mes, AVG(receita) AS avg_mes, COUNT(*) AS anos_amostra
          FROM mensal GROUP BY mes
        ),
        atual AS (
          SELECT "Ano" AS ano, "Mes" AS mes,
            SUM(CASE WHEN ${SF} THEN "Total Venda"::numeric ELSE 0 END) AS receita
          FROM bd_vendas, max_d
          WHERE "Ano"=(SELECT MAX("Ano") FROM bd_vendas)
            AND "Mes"=(SELECT MAX("Mes") FROM bd_vendas WHERE "Ano"=(SELECT MAX("Ano") FROM bd_vendas))
          GROUP BY "Ano","Mes"
        )
        SELECT mm.mes,
          ROUND(mm.avg_mes,2) AS media_historica_R$,
          ROUND(mm.avg_mes / NULLIF((SELECT avg_global FROM media_global),0), 3) AS indice_sazonalidade,
          mm.anos_amostra,
          CASE WHEN mm.mes = (SELECT mes FROM atual)
            THEN ROUND((SELECT receita FROM atual),2) END AS receita_mes_atual,
          CASE WHEN mm.mes = (SELECT mes FROM atual)
            THEN ROUND(((SELECT receita FROM atual) - mm.avg_mes) / NULLIF(mm.avg_mes,0) * 100, 1) END AS performance_vs_historico_pct
        FROM media_mes mm ORDER BY mm.mes
      `);
      const atual = rows.find(r => r.receita_mes_atual != null);
      let interpretacao = null;
      if (atual) {
        const perf = parseFloat(atual.performance_vs_historico_pct) || 0;
        const idx  = parseFloat(atual.indice_sazonalidade) || 1;
        interpretacao = perf > -5 ? 'sazonalidade_normal'
          : idx < 0.85 ? 'queda_dentro_da_sazonalidade'
          : 'queda_estrutural_acima_da_sazonalidade';
      }
      return { sazonalidade_por_mes: rows, interpretacao };
    }

    case 'vendas_cohort_skus': {
      const [anoAtual, mesAtual] = await safeQuery(pool,
        `SELECT MAX("Ano") AS ano, MAX("Mes") AS mes FROM bd_vendas WHERE "Ano"=(SELECT MAX("Ano") FROM bd_vendas)`
      ).then(r => [r[0]?.ano, r[0]?.mes]);

      const pAtualAno = input.periodo_atual_ano || anoAtual;
      const pAtualMes = input.periodo_atual_mes || mesAtual;
      const pBaseAno  = input.periodo_base_ano  || (pAtualAno - 1);
      const pBaseMes  = input.periodo_base_mes  || pAtualMes;

      const [baseSkus, atualSkus] = await Promise.all([
        safeQuery(pool, `
          SELECT "Sku" AS sku, MAX("Nome Produto") AS nome,
            ROUND(SUM(CASE WHEN ${SF} THEN "Total Venda"::numeric ELSE 0 END),2) AS receita
          FROM bd_vendas WHERE "Ano"=$1 AND "Mes"=$2 AND "Sku" IS NOT NULL AND ${SF}
          GROUP BY "Sku" HAVING SUM(CASE WHEN ${SF} THEN "Total Venda"::numeric ELSE 0 END) > 0
        `, [pBaseAno, pBaseMes]),
        safeQuery(pool, `
          SELECT "Sku" AS sku, MAX("Nome Produto") AS nome,
            ROUND(SUM(CASE WHEN ${SF} THEN "Total Venda"::numeric ELSE 0 END),2) AS receita
          FROM bd_vendas WHERE "Ano"=$1 AND "Mes"=$2 AND "Sku" IS NOT NULL AND ${SF}
          GROUP BY "Sku" HAVING SUM(CASE WHEN ${SF} THEN "Total Venda"::numeric ELSE 0 END) > 0
        `, [pAtualAno, pAtualMes]),
      ]);

      const baseMap  = new Map(baseSkus.map(r => [r.sku, r]));
      const atualMap = new Map(atualSkus.map(r => [r.sku, r]));
      const perdidos = baseSkus.filter(r => !atualMap.has(r.sku)).sort((a, b) => b.receita - a.receita).slice(0, 10);
      const ganhos   = atualSkus.filter(r => !baseMap.has(r.sku)).sort((a, b) => b.receita - a.receita).slice(0, 10);
      const mantidos = atualSkus.filter(r => baseMap.has(r.sku)).length;
      const recPerdida = perdidos.reduce((s, r) => s + (parseFloat(r.receita) || 0), 0);
      const recGanha   = ganhos.reduce((s, r) => s + (parseFloat(r.receita) || 0), 0);
      return {
        periodo_base: `${pBaseAno}-${String(pBaseMes).padStart(2, '0')}`,
        periodo_atual: `${pAtualAno}-${String(pAtualMes).padStart(2, '0')}`,
        skus_mantidos: mantidos,
        skus_perdidos_count: perdidos.length,
        skus_ganhos_count: ganhos.length,
        receita_perdida_R$: recPerdida.toFixed(2),
        receita_ganha_R$: recGanha.toFixed(2),
        saldo_R$: (recGanha - recPerdida).toFixed(2),
        top_skus_perdidos: perdidos,
        top_skus_ganhos: ganhos,
      };
    }

    case 'vendas_concentracao_risco': {
      const meses = input.meses || 3;
      const [skuRows, canalRows] = await Promise.all([
        safeQuery(pool, `
          WITH max_d AS (SELECT MAX("Data"::date) AS d FROM bd_vendas),
          total AS (SELECT SUM(CASE WHEN ${SF} THEN "Total Venda"::numeric ELSE 0 END) AS t
            FROM bd_vendas WHERE "Data"::date >= (SELECT d FROM max_d) - ($1 || ' months')::interval),
          ranked AS (
            SELECT "Sku" AS sku, MAX("Nome Produto") AS nome,
              SUM(CASE WHEN ${SF} THEN "Total Venda"::numeric ELSE 0 END) AS receita,
              ROW_NUMBER() OVER (ORDER BY SUM(CASE WHEN ${SF} THEN "Total Venda"::numeric ELSE 0 END) DESC) AS rk
            FROM bd_vendas
            WHERE "Data"::date >= (SELECT d FROM max_d) - ($1 || ' months')::interval
              AND "Sku" IS NOT NULL
            GROUP BY "Sku"
          )
          SELECT sku, nome, ROUND(receita,2) AS receita_R$,
            ROUND(receita/(SELECT t FROM total)*100,2) AS pct_receita,
            rk
          FROM ranked ORDER BY rk LIMIT 15
        `, [meses]),
        safeQuery(pool, `
          WITH max_d AS (SELECT MAX("Data"::date) AS d FROM bd_vendas),
          total AS (SELECT SUM(CASE WHEN ${SF} THEN "Total Venda"::numeric ELSE 0 END) AS t
            FROM bd_vendas WHERE "Data"::date >= (SELECT d FROM max_d) - ($1 || ' months')::interval)
          SELECT ${CANAL} AS canal,
            ROUND(SUM(CASE WHEN ${SF} THEN "Total Venda"::numeric ELSE 0 END),2) AS receita_R$,
            ROUND(SUM(CASE WHEN ${SF} THEN "Total Venda"::numeric ELSE 0 END)/(SELECT t FROM total)*100,1) AS pct_receita
          FROM bd_vendas
          WHERE "Data"::date >= (SELECT d FROM max_d) - ($1 || ' months')::interval
          GROUP BY ${CANAL} ORDER BY receita_R$ DESC
        `, [meses]),
      ]);

      const top5pct  = skuRows.filter(r => r.rk <= 5).reduce((s, r) => s + (parseFloat(r.pct_receita) || 0), 0);
      const top10pct = skuRows.filter(r => r.rk <= 10).reduce((s, r) => s + (parseFloat(r.pct_receita) || 0), 0);
      const hhi      = skuRows.reduce((s, r) => s + Math.pow(parseFloat(r.pct_receita) || 0, 2), 0);
      const top3rec  = skuRows.filter(r => r.rk <= 3).reduce((s, r) => s + (parseFloat(r['receita_R$']) || 0), 0);
      const canalDep = canalRows[0] || {};
      return {
        janela_meses: meses,
        top_5_pct_receita: +top5pct.toFixed(1),
        top_10_pct_receita: +top10pct.toFixed(1),
        herfindahl_index: +hhi.toFixed(0),
        classificacao_concentracao: hhi < 1500 ? 'DIVERSIFICADO' : hhi < 2500 ? 'MODERADO' : 'CONCENTRADO',
        canal_mais_dependente: { canal: canalDep.canal, pct_receita: canalDep.pct_receita },
        sku_mais_dependente: { sku: skuRows[0]?.sku, nome: skuRows[0]?.nome, pct_receita: skuRows[0]?.pct_receita },
        cenario_ruptura_top3_R$: +top3rec.toFixed(2),
        classificacao_risco: top5pct > 60 ? 'ALTO' : top5pct > 40 ? 'MEDIO' : 'BAIXO',
        top_skus: skuRows,
        canais: canalRows,
      };
    }

    case 'vendas_analise_completa': {
      const [kpi, sazonalidade, cohort, concentracao] = await Promise.all([
        executeVendasTool('vendas_kpi_periodo', { ano: input.ano, mes: input.mes }, pool, company),
        executeVendasTool('vendas_sazonalidade_historica', {}, pool, company),
        executeVendasTool('vendas_cohort_skus', {}, pool, company),
        executeVendasTool('vendas_concentracao_risco', { meses: 3 }, pool, company),
      ]);
      return { vendas_kpi_periodo: kpi, vendas_sazonalidade_historica: sazonalidade, vendas_cohort_skus: cohort, vendas_concentracao_risco: concentracao };
    }

    default:
      return { error: `Tool Vendas '${toolName}' não reconhecida` };
  }
}

module.exports = { VENDAS_TOOLS, executeVendasTool };
