const jwt = require('jsonwebtoken');
const { Pool } = require('pg');
const companies = require('../lib/companies');

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

async function safeQuery(pool, sql, params = []) {
  try {
    const r = await pool.query(sql, params);
    return r.rows;
  } catch (e) {
    console.error('[safeQuery]', e.message);
    return [];
  }
}

// ─── AGENTS DEFINITION ───────────────────────────────────────────────────────

const AGENTS = {

  sop: {
    label: 'S&OP',
    icon: '📦',
    description: 'Cobertura de dias, rupturas iminentes e reposição urgente',
    systemPrompt: `Você é analista S&OP sênior da {companyName}, e-commerce B2C.
Você recebe dados JÁ PRÉ-CALCULADOS: dias_cobertura, status de cada SKU e tendência de venda.
Regras do negócio:
- SKUs curva A são críticos — ruptura zero tolerância
- Lead time médio de fornecedores: 15 dias
- Meta de cobertura mínima: 30 dias
- Todo alerta deve vir acompanhado de impacto financeiro estimado (R$)
Seja CIRÚRGICO: cite SKUs, números e valores reais dos dados. Proibido generalizar.
Responda em português brasileiro.`,
    autoPrompt: `Faça este raciocínio antes de escrever:
1. Quais SKUs têm dias_cobertura < 15? Esses vão romper antes do próximo pedido chegar.
2. Quais SKUs têm dias_cobertura entre 15-30? Reposição urgente mas não crítica.
3. Existe algum SKU com estoque alto E tendência negativa (risco de encalhe)?
4. Qual o impacto financeiro estimado das rupturas iminentes? (use receita_media_mensal/30 * dias_sem_estoque)

Escreva a análise neste formato exato:

## 🚨 Rupturas Iminentes (dias_cobertura < 15)
Tabela: SKU | Estoque | Dias Restantes | Receita em Risco/mês | Ação
Se nenhum: confirme explicitamente que não há rupturas iminentes.

## ⚠️ Reposição Urgente (15–30 dias de cobertura)
Liste SKUs com quantidade sugerida de compra para atingir 45 dias de cobertura.

## 📦 Curva ABC — Saúde por Segmento
Mostre quantos SKUs de cada curva estão OK vs em risco. Destaque anomalias.

## ⚠️ Risco de Encalhe
SKUs com dias_cobertura > 120 e tendência negativa. Calcule custo de capital imobilizado.

## 📊 Decisões para Hoje
3 ações concretas priorizadas por impacto financeiro, com valores estimados em R$.`,
    async fetchData(pool) {
      const [sopc, vendas12m, abcDist, esRisco] = await Promise.all([
        safeQuery(pool, `SELECT sku, alerta, estoque_atual::numeric AS estoque, ponto_pedido::numeric AS pp FROM ponto_pedido ORDER BY alerta DESC NULLS LAST LIMIT 150`),
        safeQuery(pool, `
          WITH max_d AS (SELECT MAX("Data"::date) AS d FROM bd_vendas)
          SELECT "Sku" AS sku,
            MAX("Categoria") AS categoria,
            ROUND(SUM(CASE WHEN "Data"::date >= (SELECT d FROM max_d) - INTERVAL '1 month'  THEN "Quantidade Vendida"::numeric ELSE 0 END), 0) AS qtd_1m,
            ROUND(SUM(CASE WHEN "Data"::date >= (SELECT d FROM max_d) - INTERVAL '3 months' THEN "Quantidade Vendida"::numeric ELSE 0 END) / 3.0, 1) AS media_mensal_3m,
            ROUND(SUM(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN "Total Venda"::numeric ELSE 0 END) / 12.0, 2) AS receita_media_mensal,
            ROUND(SUM(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN COALESCE("Margem Produto"::numeric, 0) ELSE 0 END) / 12.0, 2) AS margem_media_mensal,
            ROUND(SUM(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN COALESCE("Custo Total"::numeric, 0) ELSE 0 END) / 12.0, 2) AS custo_medio_mensal
          FROM bd_vendas
          WHERE "Sku" IS NOT NULL AND TRIM("Sku"::text) != ''
            AND "Data"::date >= (SELECT d FROM max_d) - INTERVAL '12 months'
          GROUP BY "Sku"
        `),
        safeQuery(pool, `SELECT "Curva" AS curva, COUNT(*) AS qtd FROM curva_abc WHERE "Ano"=(SELECT MAX("Ano") FROM curva_abc) GROUP BY "Curva" ORDER BY "Curva"`),
        safeQuery(pool, `SELECT sku, media_mensal::numeric AS media_mensal FROM estoque_seguranca WHERE sku IS NOT NULL LIMIT 50`),
      ]);

      const vendasMap = {};
      vendas12m.forEach(v => { vendasMap[String(v.sku || '').trim().toUpperCase()] = v; });
      const esMap = {};
      esRisco.forEach(e => { esMap[String(e.sku || '').trim().toUpperCase()] = e; });

      const enriquecidos = sopc.map(s => {
        const key = String(s.sku || '').trim().toUpperCase();
        const v = vendasMap[key] || {};
        const es = esMap[key] || {};
        const mediaDaily = (parseFloat(v.media_mensal_3m) || 0) / 30;
        const estoque = parseFloat(s.estoque) || 0;
        const diasCobertura = mediaDaily > 0 ? Math.round(estoque / mediaDaily) : null;
        const qtd1m = parseFloat(v.qtd_1m) || 0;
        const media3m = parseFloat(v.media_mensal_3m) || 0;
        const tendenciaPct = media3m > 0 ? Math.round((qtd1m - media3m) / media3m * 100) : null;
        let status;
        if (diasCobertura === null) status = 'SEM_DADOS_VENDA';
        else if (diasCobertura <= 0)  status = 'RUPTURA';
        else if (diasCobertura < 15)  status = 'RUPTURA_IMINENTE';
        else if (diasCobertura < 30)  status = 'ABAIXO_META_30D';
        else if (diasCobertura > 120 && tendenciaPct !== null && tendenciaPct < -15) status = 'RISCO_ENCALHE';
        else status = 'OK';
        return {
          sku: s.sku,
          estoque,
          ponto_pedido: parseFloat(s.pp) || 0,
          dias_cobertura: diasCobertura,
          status,
          tendencia_pct: tendenciaPct !== null ? `${tendenciaPct > 0 ? '+' : ''}${tendenciaPct}%` : 'sem dados',
          receita_media_mensal: parseFloat(v.receita_media_mensal) || 0,
          margem_media_mensal: parseFloat(v.margem_media_mensal) || 0,
          estoque_seguranca: parseFloat(es.media_mensal) || null,
        };
      });

      const porStatus = {};
      enriquecidos.forEach(s => { porStatus[s.status] = (porStatus[s.status] || 0) + 1; });
      const criticos = enriquecidos
        .filter(s => ['RUPTURA', 'RUPTURA_IMINENTE', 'ABAIXO_META_30D'].includes(s.status))
        .sort((a, b) => (a.dias_cobertura ?? 9999) - (b.dias_cobertura ?? 9999));
      const encalhes = enriquecidos.filter(s => s.status === 'RISCO_ENCALHE');

      return {
        total_skus: enriquecidos.length,
        resumo_por_status: porStatus,
        distribuicao_abc: abcDist,
        skus_criticos: criticos.slice(0, 35),
        risco_encalhe: encalhes.slice(0, 10),
      };
    },
  },

  estoque: {
    label: 'Estoque',
    icon: '🏭',
    description: 'Portfólio, giro, tendências e oportunidades de enxugamento',
    systemPrompt: `Você é analista de portfólio e inventário da {companyName}, e-commerce B2C.
Os dados já incluem velocidade de venda (1m vs média 3m) e classificação de tendência calculadas no backend.
Foco: identificar produtos para descontinuar, produtos subexpostos com potencial e riscos de ruptura silenciosa.
Seja específico: cite SKUs, números e percentuais reais.
Responda em português brasileiro.`,
    autoPrompt: `Raciocínio antes de escrever:
1. Quais SKUs têm giro alto no 1m mas eram medianos nos 3m? (aceleração — oportunidade de aumentar estoque)
2. Quais SKUs têm giro caindo consistentemente? (candidatos a descontinuação)
3. Há SKUs no cadastro com zero venda nos últimos 6 meses? (portfólio morto)
4. Qual % do portfólio gera 80% das vendas? (concentração de Pareto)

Escreva neste formato:

## 📊 Visão Geral do Portfólio
Totais: cadastrados, com venda 1m, sem venda 1m, sem venda 6m. Calcule % de portfólio ativo.

## 🚀 SKUs em Aceleração (oportunidade)
Produtos com tendencia_pct > +20% no último mês vs média 3m. Liste top 10 com números.

## 📉 SKUs em Desaceleração (risco)
tendencia_pct < -20%. Diferencie queda sazonal de queda estrutural pela magnitude.

## 🗑️ Candidatos à Descontinuação
Sem venda há 6+ meses. Calcule o custo de manter esses SKUs no portfólio.

## 💡 Recomendações
Top 3 ações com impacto estimado em receita ou redução de custo operacional.`,
    async fetchData(pool) {
      const [atividade, totalCad, semVenda6m] = await Promise.all([
        safeQuery(pool, `
          WITH max_d AS (SELECT MAX("Data"::date) AS d FROM bd_vendas)
          SELECT "Sku" AS sku,
            MAX("Nome Produto") AS nome_produto,
            MAX("Categoria") AS categoria,
            ROUND(SUM(CASE WHEN "Data"::date >= (SELECT d FROM max_d) - INTERVAL '1 month'  THEN "Quantidade Vendida"::numeric ELSE 0 END), 0) AS qtd_1m,
            ROUND(SUM(CASE WHEN "Data"::date >= (SELECT d FROM max_d) - INTERVAL '3 months' THEN "Quantidade Vendida"::numeric ELSE 0 END) / 3.0, 1) AS media_mensal_3m,
            ROUND(SUM(CASE WHEN "Data"::date >= (SELECT d FROM max_d) - INTERVAL '6 months' THEN "Quantidade Vendida"::numeric ELSE 0 END), 0) AS qtd_6m,
            ROUND(SUM(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN "Total Venda"::numeric ELSE 0 END) / 12.0, 2) AS receita_media_mensal
          FROM bd_vendas
          WHERE "Status" !~* '(cancel|devol|n[aã]o.?pago)' AND "Sku" IS NOT NULL AND TRIM("Sku"::text) != ''
            AND "Data"::date >= (SELECT d FROM max_d) - INTERVAL '12 months'
          GROUP BY "Sku" ORDER BY receita_media_mensal DESC LIMIT 120
        `),
        safeQuery(pool, `SELECT COUNT(*) AS total FROM cadastros_sku`),
        safeQuery(pool, `
          SELECT DISTINCT c."Sku" AS sku FROM cadastros_sku c
          WHERE c."Sku" NOT IN (
            SELECT DISTINCT "Sku" FROM bd_vendas
            WHERE "Data" >= CURRENT_DATE - INTERVAL '6 months' AND "Status" !~* '(cancel|devol|n[aã]o.?pago)'
          ) LIMIT 30
        `),
      ]);

      const enriquecido = atividade.map(r => {
        const qtd1m = parseFloat(r.qtd_1m) || 0;
        const media3m = parseFloat(r.media_mensal_3m) || 0;
        const tendenciaPct = media3m > 0 ? Math.round((qtd1m - media3m) / media3m * 100) : null;
        let tendencia;
        if (tendenciaPct === null) tendencia = 'sem_dados';
        else if (tendenciaPct >= 20) tendencia = 'ACELERANDO';
        else if (tendenciaPct <= -20) tendencia = 'DESACELERANDO';
        else tendencia = 'ESTAVEL';
        return { ...r, tendencia_pct: tendenciaPct !== null ? `${tendenciaPct > 0 ? '+' : ''}${tendenciaPct}%` : 'N/A', tendencia };
      });

      const acelerando = enriquecido.filter(r => r.tendencia === 'ACELERANDO');
      const desacelerando = enriquecido.filter(r => r.tendencia === 'DESACELERANDO');
      const semVenda1m = enriquecido.filter(r => (parseFloat(r.qtd_1m) || 0) === 0).length;

      return {
        total_cadastrados: parseInt(totalCad[0]?.total) || 0,
        skus_com_venda_1m: enriquecido.length - semVenda1m,
        skus_sem_venda_1m: semVenda1m,
        skus_sem_venda_6m: semVenda6m.length,
        candidatos_descontinuacao: semVenda6m.slice(0, 20),
        top_acelerando: acelerando.slice(0, 15),
        top_desacelerando: desacelerando.sort((a,b) => (parseFloat(a.tendencia_pct)||0) - (parseFloat(b.tendencia_pct)||0)).slice(0, 15),
        top_receita: enriquecido.slice(0, 20),
      };
    },
  },

  financeiro: {
    label: 'Financeiro',
    icon: '💰',
    description: 'DRE, Balanço, índices financeiros e tendência de caixa',
    systemPrompt: `Você é CFO analítico da {companyName}, e-commerce B2C.
Os dados incluem totais de ativo/passivo pré-calculados, variação mensal do fluxo e principais contas contábeis.
Calcule e interprete: liquidez corrente, endividamento, giro do ativo, margem líquida.
Seja preciso com números. Se um indicador está fora do padrão saudável, diga explicitamente o que está errado.
Responda em português brasileiro.`,
    autoPrompt: `Raciocínio antes de escrever:
1. Calcule liquidez_corrente = ativo_circulante / passivo_circulante (contas 1.x vs 2.x)
2. Calcule endividamento = passivo_total / ativo_total
3. No fluxo dos últimos 6 meses: o saldo livre (entradas - saídas) está crescendo ou caindo?
4. Existe alguma conta com variação anormal (debito ou credito muito acima da média)?

Escreva neste formato:

## 💼 Posição Patrimonial (mês mais recente)
Ativo Total | Passivo Total | Patrimônio Líquido | Índice de Endividamento (%)

## 📈 Indicadores Financeiros Calculados
Liquidez Corrente | Giro do Ativo | Margem (se disponível nos dados)
Para cada um: valor calculado + interpretação (saudável / atenção / crítico)

## 💧 Fluxo de Caixa — Tendência 6 Meses
Tabela mês a mês: Entradas | Saídas | Saldo Livre | Variação %
Identifique tendência (melhorando / piorando / estável)

## 🚨 Alertas Contábeis
Contas com variação > 30% ou valores que merecem investigação. Cite conta + valor + motivo suspeito.

## 📋 Recomendações
3 ações financeiras priorizadas por urgência, com impacto quantificado.`,
    async fetchData(pool) {
      const [balancoAtual, balancoAnterior, fluxoMensal] = await Promise.all([
        safeQuery(pool, `
          SELECT conta, nome, saldo_anterior::numeric AS saldo_anterior, debito::numeric AS debito,
                 credito::numeric AS credito, saldo_atual::numeric AS saldo_atual
          FROM dfs_balanco
          WHERE ano = (SELECT MAX(ano) FROM dfs_balanco)
            AND mes = (SELECT MAX(mes) FROM dfs_balanco WHERE ano = (SELECT MAX(ano) FROM dfs_balanco))
          ORDER BY conta LIMIT 60
        `),
        safeQuery(pool, `
          SELECT conta, saldo_atual::numeric AS saldo_atual
          FROM dfs_balanco
          WHERE ano = (SELECT MAX(ano) FROM dfs_balanco)
            AND mes = (SELECT MAX(mes) FROM dfs_balanco WHERE ano = (SELECT MAX(ano) FROM dfs_balanco)) - 1
          ORDER BY conta LIMIT 60
        `),
        safeQuery(pool, `
          SELECT ano, mes,
            ROUND(SUM(CASE WHEN valor::numeric > 0 THEN valor::numeric ELSE 0 END), 2) AS entradas,
            ROUND(ABS(SUM(CASE WHEN valor::numeric < 0 THEN valor::numeric ELSE 0 END)), 2) AS saidas,
            ROUND(SUM(valor::numeric), 2) AS saldo_livre
          FROM dfs_fluxo_caixa_diario
          GROUP BY ano, mes ORDER BY ano DESC, mes DESC LIMIT 8
        `),
      ]);

      const saldoAnteriorMap = {};
      balancoAnterior.forEach(r => { saldoAnteriorMap[r.conta] = parseFloat(r.saldo_atual) || 0; });

      const totalAtivo     = balancoAtual.filter(r => String(r.conta||'').startsWith('1')).reduce((s,r) => s+(parseFloat(r.saldo_atual)||0), 0);
      const totalPassivo   = balancoAtual.filter(r => String(r.conta||'').startsWith('2')).reduce((s,r) => s+(parseFloat(r.saldo_atual)||0), 0);
      const ativoCirc      = balancoAtual.filter(r => /^1\.[12]/.test(String(r.conta||''))).reduce((s,r) => s+(parseFloat(r.saldo_atual)||0), 0);
      const passivoCirc    = balancoAtual.filter(r => /^2\.[12]/.test(String(r.conta||''))).reduce((s,r) => s+(parseFloat(r.saldo_atual)||0), 0);
      const pl             = totalAtivo - totalPassivo;
      const liquidezCorr   = passivoCirc > 0 ? (ativoCirc / passivoCirc).toFixed(2) : 'N/A';
      const endividamento  = totalAtivo > 0 ? ((totalPassivo / totalAtivo) * 100).toFixed(1) + '%' : 'N/A';

      const contasComVariacao = balancoAtual.map(r => {
        const ant = saldoAnteriorMap[r.conta] || 0;
        const atual = parseFloat(r.saldo_atual) || 0;
        const variacao = ant !== 0 ? Math.round((atual - ant) / Math.abs(ant) * 100) : null;
        return { conta: r.conta, nome: r.nome, saldo_atual: atual.toFixed(2), variacao_pct: variacao !== null ? `${variacao > 0 ? '+' : ''}${variacao}%` : 'N/A' };
      }).sort((a, b) => Math.abs(parseInt(b.variacao_pct)||0) - Math.abs(parseInt(a.variacao_pct)||0));

      return {
        periodo: { ano: balancoAtual[0]?.ano, mes: balancoAtual[0]?.mes },
        indicadores: {
          total_ativo: totalAtivo.toFixed(2),
          total_passivo: totalPassivo.toFixed(2),
          patrimonio_liquido: pl.toFixed(2),
          liquidez_corrente: liquidezCorr,
          endividamento,
        },
        contas_maior_variacao: contasComVariacao.slice(0, 15),
        fluxo_mensal_8m: fluxoMensal,
      };
    },
  },

  vendas: {
    label: 'Vendas',
    icon: '📈',
    description: 'Performance, crescimento, concentração e margem por SKU',
    systemPrompt: `Você é analista comercial sênior da {companyName}, e-commerce B2C.
Os dados incluem crescimento mês a mês calculado, margem por SKU e concentração de Pareto.
Foco: identificar o que está crescendo, o que está caindo, concentração de risco e oportunidades de mix.
Cite crescimentos e quedas com % reais. Calcule concentração (top 10 SKUs = X% da receita).
Responda em português brasileiro.`,
    autoPrompt: `Raciocínio antes de escrever:
1. Qual a tendência dos últimos 3 meses? Calcule CAGR simplificado: (mês_atual / mês_3_atrás - 1) * 100
2. Os top 10 SKUs concentram quanto % da receita total? Isso é saudável?
3. Quais SKUs têm margem acima da média? Estão sendo priorizados?
4. Há meses com queda abrupta? O que pode explicar (sazonalidade, cancelamentos)?

Escreva neste formato:

## 📊 KPIs do Mês Atual
Receita líquida | Qtd vendida | Margem total | Ticket médio
Variação vs mês anterior e vs mesmo mês ano anterior (se disponível)

## 📈 Tendência — Últimos 12 Meses
Tabela: Mês | Receita | Variação MoM% | Qtd
Destaque os 3 melhores e 3 piores meses. Identifique padrão sazonal se houver.

## 🏆 Top 20 SKUs (últimos 3 meses)
Tabela: SKU | Receita | Qtd | % do Total | Margem média
Calcule: os top 5 SKUs respondem por X% da receita — isso é concentração normal ou risco?

## 🔻 SKUs em Queda
SKUs que estavam no top do mês anterior mas caíram mais de 20%. Identificar causa.

## 💡 Oportunidades
3 recomendações concretas para aumentar receita ou margem, com potencial estimado em R$.`,
    async fetchData(pool) {
      const [mensal, topSkus, kpiAtual, skusTendencia, categorias, canais] = await Promise.all([
        safeQuery(pool, `
          SELECT "Ano" AS ano, "Mes" AS mes,
            ROUND(SUM(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN "Total Venda"::numeric ELSE 0 END), 2) AS receita,
            ROUND(SUM(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN "Quantidade Vendida"::numeric ELSE 0 END), 0) AS qtd,
            ROUND(SUM(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN COALESCE("Margem Produto"::numeric, 0) ELSE 0 END), 2) AS margem
          FROM bd_vendas GROUP BY "Ano", "Mes" ORDER BY "Ano" DESC, "Mes" DESC LIMIT 14
        `),
        safeQuery(pool, `
          WITH max_d AS (SELECT MAX("Data"::date) AS d FROM bd_vendas),
          total AS (SELECT SUM(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN "Total Venda"::numeric ELSE 0 END) AS t FROM bd_vendas WHERE "Data"::date >= (SELECT d FROM max_d) - INTERVAL '3 months')
          SELECT "Sku" AS sku,
            MAX("Nome Produto") AS nome_produto,
            MAX("Categoria") AS categoria,
            ROUND(SUM(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN "Total Venda"::numeric ELSE 0 END), 2) AS receita_3m,
            ROUND(SUM(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN "Quantidade Vendida"::numeric ELSE 0 END), 0) AS qtd_3m,
            ROUND(SUM(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN COALESCE("Margem Produto"::numeric, 0) ELSE 0 END), 2) AS margem_3m,
            ROUND(SUM(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN "Total Venda"::numeric ELSE 0 END) / (SELECT t FROM total) * 100, 1) AS pct_receita_total
          FROM bd_vendas
          WHERE "Sku" IS NOT NULL AND "Data"::date >= (SELECT d FROM max_d) - INTERVAL '3 months'
          GROUP BY "Sku" ORDER BY receita_3m DESC LIMIT 25
        `),
        safeQuery(pool, `
          SELECT "Ano" AS ano, "Mes" AS mes,
            ROUND(SUM(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN "Total Venda"::numeric ELSE 0 END), 2) AS receita,
            ROUND(SUM(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN "Quantidade Vendida"::numeric ELSE 0 END), 0) AS qtd,
            ROUND(SUM(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN COALESCE("Margem Produto"::numeric, 0) ELSE 0 END), 2) AS margem,
            COUNT(DISTINCT "Sku") AS skus_ativos
          FROM bd_vendas
          WHERE "Ano" = (SELECT MAX("Ano") FROM bd_vendas)
            AND "Mes" = (SELECT MAX("Mes") FROM bd_vendas WHERE "Ano" = (SELECT MAX("Ano") FROM bd_vendas))
          GROUP BY "Ano", "Mes"
        `),
        safeQuery(pool, `
          WITH m1 AS (
            SELECT "Sku" AS sku, SUM("Total Venda"::numeric) AS rec
            FROM bd_vendas WHERE "Status" !~* '(cancel|devol|n[aã]o.?pago)'
              AND "Ano"=(SELECT MAX("Ano") FROM bd_vendas)
              AND "Mes"=(SELECT MAX("Mes") FROM bd_vendas WHERE "Ano"=(SELECT MAX("Ano") FROM bd_vendas))
            GROUP BY "Sku"
          ),
          m2 AS (
            SELECT "Sku" AS sku, SUM("Total Venda"::numeric) AS rec
            FROM bd_vendas WHERE "Status" !~* '(cancel|devol|n[aã]o.?pago)'
              AND "Ano"=(SELECT MAX("Ano") FROM bd_vendas)
              AND "Mes"=(SELECT MAX("Mes") FROM bd_vendas WHERE "Ano"=(SELECT MAX("Ano") FROM bd_vendas)) - 1
            GROUP BY "Sku"
          )
          SELECT m1.sku, ROUND(m1.rec,2) AS receita_atual, ROUND(m2.rec,2) AS receita_anterior,
            ROUND((m1.rec - m2.rec) / NULLIF(m2.rec, 0) * 100, 1) AS variacao_pct
          FROM m1 LEFT JOIN m2 ON m1.sku = m2.sku
          WHERE m2.rec IS NOT NULL ORDER BY variacao_pct ASC LIMIT 15
        `),
        safeQuery(pool, `
          SELECT "Categoria" AS categoria,
            ROUND(SUM(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN "Total Venda"::numeric ELSE 0 END), 2) AS receita,
            ROUND(SUM(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN COALESCE("Margem Produto"::numeric, 0) ELSE 0 END), 2) AS margem,
            SUM(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN "Quantidade Vendida"::numeric ELSE 0 END) AS qtd,
            COUNT(DISTINCT "Sku") AS skus
          FROM bd_vendas
          WHERE "Categoria" IS NOT NULL AND TRIM("Categoria"::text) != ''
            AND "Ano" = (SELECT MAX("Ano") FROM bd_vendas)
            AND "Mes" = (SELECT MAX("Mes") FROM bd_vendas WHERE "Ano" = (SELECT MAX("Ano") FROM bd_vendas))
          GROUP BY "Categoria" ORDER BY receita DESC LIMIT 20
        `),
        safeQuery(pool, `
          SELECT COALESCE(NULLIF(TRIM("Canal Apelido"::text), ''), TRIM("Canal de venda"::text), 'Sem canal') AS canal,
            ROUND(SUM(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN "Total Venda"::numeric ELSE 0 END), 2) AS receita,
            ROUND(SUM(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN COALESCE("Margem Produto"::numeric, 0) ELSE 0 END), 2) AS margem,
            SUM(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN "Quantidade Vendida"::numeric ELSE 0 END) AS qtd
          FROM bd_vendas
          WHERE "Status" !~* '(cancel|devol|n[aã]o.?pago)'
            AND "Ano" = (SELECT MAX("Ano") FROM bd_vendas)
            AND "Mes" = (SELECT MAX("Mes") FROM bd_vendas WHERE "Ano" = (SELECT MAX("Ano") FROM bd_vendas))
          GROUP BY COALESCE(NULLIF(TRIM("Canal Apelido"::text), ''), TRIM("Canal de venda"::text), 'Sem canal')
          ORDER BY receita DESC
        `),
      ]);

      const receitaAtual = parseFloat(kpiAtual[0]?.receita) || 0;
      const receitaMesAnt = parseFloat(mensal[1]?.receita) || 0;
      const variacaoMoM = receitaMesAnt > 0 ? ((receitaAtual - receitaMesAnt) / receitaMesAnt * 100).toFixed(1) + '%' : 'N/A';
      const margemPct = receitaAtual > 0 ? ((parseFloat(kpiAtual[0]?.margem)||0) / receitaAtual * 100).toFixed(1) + '%' : 'N/A';
      const ticketMedio = (parseFloat(kpiAtual[0]?.qtd)||0) > 0 ? (receitaAtual / parseFloat(kpiAtual[0].qtd)).toFixed(2) : 'N/A';

      return {
        kpi_mes_atual: { ...kpiAtual[0], variacao_mom: variacaoMoM, margem_pct: margemPct, ticket_medio: ticketMedio },
        historico_14m: mensal,
        top_skus_3m: topSkus,
        skus_maior_queda: skusTendencia.slice(0, 10),
        breakdown_categorias: categorias,
        breakdown_canais: canais,
      };
    },
  },

  caixa: {
    label: 'Caixa',
    icon: '🏦',
    description: 'Posição de caixa, burn rate, anomalias e projeção',
    systemPrompt: `Você é controller de caixa da {companyName}, e-commerce B2C.
Os dados incluem entradas/saídas pré-calculadas, saldo líquido e as maiores transações do mês.
Foco: posição real de caixa, burn rate, anomalias e capacidade de pagamento.
Calcule projeção de dias de caixa disponível com base no burn rate médio.
Responda em português brasileiro.`,
    autoPrompt: `Raciocínio antes de escrever:
1. Qual o burn rate mensal? (média das saídas dos últimos meses)
2. Com o saldo atual, quantos dias de operação o caixa suporta?
3. Há alguma saída ou entrada pontual que distorce o mês? (outlier vs padrão)
4. O saldo livre (entradas - saídas) dos últimos meses é positivo ou negativo? Tendência?

Escreva neste formato:

## 💰 Posição de Caixa — Mês Atual
Entradas | Saídas | Saldo Líquido | Variação vs mês anterior
Calcule: dias de caixa disponível = saldo_liquido / (burn_rate_medio / 30)

## 📥 Top 8 Entradas
Tabela: Descrição | Valor | Dia — identifique se recorrente ou pontual

## 📤 Top 8 Saídas
Tabela: Descrição | Valor | Dia — classifique: fixo / variável / pontual

## 📊 Tendência de Caixa (histórico disponível)
Meses anteriores se disponíveis: saldo livre mês a mês. Melhorando ou piorando?

## 🚨 Alertas
Outliers (transações > 2x a média), saldo negativo, burn acelerado.

## 📋 Recomendações
3 ações concretas para melhorar a posição de caixa nos próximos 30 dias.`,
    async fetchData(pool) {
      const [extratoAtual, historico] = await Promise.all([
        safeQuery(pool, `
          SELECT dia, descricao, valor::numeric AS valor
          FROM caixa_extrato
          WHERE ano = (SELECT MAX(ano) FROM caixa_extrato)
            AND mes = (SELECT MAX(mes) FROM caixa_extrato WHERE ano = (SELECT MAX(ano) FROM caixa_extrato))
          ORDER BY ABS(valor::numeric) DESC LIMIT 80
        `),
        safeQuery(pool, `
          SELECT ano, mes,
            ROUND(SUM(CASE WHEN valor::numeric > 0 THEN valor::numeric ELSE 0 END) / 100.0, 2) AS entradas,
            ROUND(ABS(SUM(CASE WHEN valor::numeric < 0 THEN valor::numeric ELSE 0 END)) / 100.0, 2) AS saidas,
            ROUND(SUM(valor::numeric) / 100.0, 2) AS saldo_livre,
            COUNT(*) AS lancamentos
          FROM caixa_extrato
          GROUP BY ano, mes ORDER BY ano DESC, mes DESC LIMIT 6
        `),
      ]);

      const entradas = extratoAtual.filter(r => (parseFloat(r.valor)||0) > 0);
      const saidas   = extratoAtual.filter(r => (parseFloat(r.valor)||0) < 0);
      const totE = entradas.reduce((s,r) => s + (parseFloat(r.valor)||0), 0);
      const totS = saidas.reduce((s,r)   => s + (parseFloat(r.valor)||0), 0);
      const saldo = totE + totS;

      const burnRateMedio = historico.length > 1
        ? (historico.slice(0, 3).reduce((s,r) => s + (parseFloat(r.saidas)||0), 0) / Math.min(3, historico.length)).toFixed(2)
        : (Math.abs(totS) / 100).toFixed(2);
      const diasCaixa = parseFloat(burnRateMedio) > 0 ? Math.round((saldo / 100) / (parseFloat(burnRateMedio) / 30)) : null;
      const variacaoMoM = historico[1]?.saldo_livre != null
        ? ((parseFloat(historico[0]?.saldo_livre||0) - parseFloat(historico[1]?.saldo_livre||0))).toFixed(2)
        : null;

      return {
        periodo: historico[0] ? `${historico[0].mes}/${historico[0].ano}` : 'N/A',
        saldo_liquido_R$: (saldo / 100).toFixed(2),
        total_entradas_R$: (totE / 100).toFixed(2),
        total_saidas_R$: (Math.abs(totS) / 100).toFixed(2),
        burn_rate_medio_R$: burnRateMedio,
        dias_caixa_estimados: diasCaixa,
        variacao_saldo_vs_mes_anterior_R$: variacaoMoM,
        maiores_entradas: entradas.sort((a,b) => b.valor-a.valor).map(r => ({...r, valor_R$: (r.valor/100).toFixed(2)})).slice(0, 8),
        maiores_saidas: saidas.sort((a,b) => a.valor-b.valor).map(r => ({...r, valor_R$: (Math.abs(r.valor)/100).toFixed(2)})).slice(0, 8),
        historico_6m: historico,
      };
    },
  },
};

// ─── CALL CLAUDE ─────────────────────────────────────────────────────────────

async function callLLM({ systemPrompt, userMessage, companyName }) {
  if (!process.env.OPENROUTER_API_KEY) {
    throw new Error('OPENROUTER_API_KEY não está configurada. Obtenha gratuitamente em: https://openrouter.ai/keys');
  }
  const maxLen = 40000;
  const msg = userMessage.length > maxLen ? userMessage.slice(0, maxLen) + '\n[dados truncados]' : userMessage;
  const response = await fetch('https://openrouter.ai/api/v1/chat/completions', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${process.env.OPENROUTER_API_KEY}`,
      'HTTP-Referer': 'https://have-gestor-frontend.vercel.app',
    },
    body: JSON.stringify({
      model: 'google/gemini-2.5-pro-exp-03-25:free',
      max_tokens: 8000,
      messages: [
        { role: 'system', content: systemPrompt.replace('{companyName}', companyName) },
        { role: 'user', content: msg },
      ],
    }),
  });
  if (!response.ok) {
    const err = await response.text();
    throw new Error(`OpenRouter API error ${response.status}: ${err}`);
  }
  const data = await response.json();
  return data.choices[0].message.content;
}

// ─── HANDLER ─────────────────────────────────────────────────────────────────

module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  if (req.method === 'OPTIONS') return res.status(200).end();

  const auth = (req.headers.authorization || '').split(' ')[1];
  if (!auth) return res.status(401).json({ error: 'Token não fornecido' });

  let payload;
  try {
    payload = jwt.verify(auth, process.env.JWT_SECRET);
  } catch {
    return res.status(401).json({ error: 'Token inválido ou expirado' });
  }

  const company = payload.company || 'lanzi';
  const companyName = companies[company]?.name || company;
  const pool = getPool(company);

  // GET /api/agents?agent=sop → lista agentes OU análise automática
  if (req.method === 'GET') {
    const agentKey = req.query.agent;

    // Sem parâmetro: retorna lista de agentes disponíveis
    if (!agentKey) {
      return res.json(
        Object.entries(AGENTS).map(([key, a]) => ({
          key, label: a.label, icon: a.icon, description: a.description,
        }))
      );
    }

    const agent = AGENTS[agentKey];
    if (!agent) return res.status(400).json({ error: `Agente '${agentKey}' não encontrado` });

    try {
      const data = await agent.fetchData(pool);
      const analysis = await callLLM({
        systemPrompt: agent.systemPrompt,
        userMessage: `${agent.autoPrompt}\n\nDados do banco de dados:\n${JSON.stringify(data, null, 2)}`,
        companyName,
      });
      return res.json({ agent: agentKey, analysis, data });
    } catch (e) {
      console.error(`[AGENTS GET] ${agentKey}:`, e.message);
      return res.status(500).json({ error: e.message });
    }
  }

  // POST /api/agents → chat com agente
  if (req.method === 'POST') {
    const { agent: agentKey, message, contextData } = req.body || {};
    if (!agentKey || !message) return res.status(400).json({ error: 'agent e message são obrigatórios' });

    const agent = AGENTS[agentKey];
    if (!agent) return res.status(400).json({ error: `Agente '${agentKey}' não encontrado` });

    try {
      let data = contextData;
      if (!data) data = await agent.fetchData(pool);

      const reply = await callLLM({
        systemPrompt: agent.systemPrompt,
        userMessage: `Pergunta do gestor: ${message}\n\nContexto dos dados:\n${JSON.stringify(data, null, 2)}`,
        companyName,
      });
      return res.json({ reply });
    } catch (e) {
      console.error(`[AGENTS POST] ${agentKey}:`, e.message);
      return res.status(500).json({ error: e.message });
    }
  }

  return res.status(405).json({ error: 'Method not allowed' });
};
