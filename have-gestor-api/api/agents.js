const jwt = require('jsonwebtoken');
const { Pool } = require('pg');
const companies = require('../lib/companies');

const analysisCache = {};

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

// ─── FINANCEIRO TOOLS DEFINITIONS ────────────────────────────────────────────

const FINANCEIRO_TOOLS = [
  {
    name: 'balanco_patrimonial',
    description: 'Balanço Patrimonial usando mapeamento oficial do dfs_estrutura. Retorna totais por seção (AC, ANC, PC, PNC, PL) + indicadores calculados.',
    input_schema: {
      type: 'object',
      properties: {
        ano: { type: 'number' },
        mes: { type: 'number' },
      },
    },
  },
  {
    name: 'dre',
    description: 'DRE usando dre_structure + dre_mappings. Retorna receita bruta, deduções, lucro bruto, EBITDA, lucro líquido com % sobre receita líquida.',
    input_schema: {
      type: 'object',
      properties: {
        ano: { type: 'number' },
        mes: { type: 'number' },
        meses_acumulado: { type: 'number' },
      },
    },
  },
  {
    name: 'fluxo_caixa',
    description: 'DFC por categoria usando fcx_mappings + dfs_fluxo_caixa_diario. Retorna entradas/saídas mensais, burn rate e dias de caixa estimados.',
    input_schema: {
      type: 'object',
      properties: { meses: { type: 'number' } },
    },
  },
  {
    name: 'indicadores_financeiros',
    description: 'KPIs financeiros: liquidez corrente e seca, ROE, ROA, margem EBITDA, endividamento, giro do ativo. Cada um com benchmark (saudável/atenção/crítico).',
    input_schema: { type: 'object', properties: {} },
  },
  {
    name: 'evolucao_mensal',
    description: 'Série temporal mês a mês de receita, margem, totais do BP e principais contas. Detecta tendência automaticamente.',
    input_schema: {
      type: 'object',
      properties: {
        meses: { type: 'number' },
        contas: { type: 'array', items: { type: 'string' } },
      },
    },
  },
  {
    name: 'alertas_contabeis',
    description: 'Detecta anomalias: ativo negativo, passivo=0, variação >200%, PL negativo, caixa negativo. Classifica por gravidade (crítico/atenção/informativo).',
    input_schema: { type: 'object', properties: {} },
  },
  {
    name: 'analise_completa_financeiro',
    description: 'Roda balanco_patrimonial + dre + fluxo_caixa + indicadores_financeiros + alertas_contabeis em paralelo. Use para análise geral.',
    input_schema: {
      type: 'object',
      properties: { meses_historico: { type: 'number' } },
    },
  },
  {
    name: 'executar_sql_financeiro',
    description: 'SELECT nas tabelas financeiras: dfs_balanco, dfs_estrutura, dfs_fluxo_caixa_diario, caixa_extrato. Requer LIMIT.',
    input_schema: {
      type: 'object',
      properties: { sql: { type: 'string' } },
      required: ['sql'],
    },
  },
];

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
    tools: ESTOQUE_TOOLS,
    systemPrompt: `Analista de estoque da {companyName}. Use as tools para buscar dados reais — nunca invente números.

Regras:
- Sempre chame portfolio_summary primeiro
- Toda compra recomendada: qtd + R$ + data limite (hoje+15d)
- Separe aceleração com estoque OK de aceleração em risco de ruptura
- Responda em português, cite SKUs e valores reais

Formato da resposta:
## 📊 Portfólio: totais do portfolio_summary
## 🚀 Aceleração: SKU, tend 1m/3m, cobertura, margem, diagnóstico
## 📉 Desaceleração: SKU, queda, cobertura, classificação
## 💀 Capital Parado: top órfãos + total R$
## 🛒 Compras: tabela SKU|Qtd|R$|Data|Motivo + total vs caixa
## ⚡ 3 Prioridades: ação + impacto R$`,
    autoTools: [
      { name: 'portfolio_summary', input: {} },
      { name: 'pareto', input: {} },
      { name: 'skus_orfaos', input: { dias_sem_venda: 90 } },
      { name: 'recomendacao_compra', input: { dias_meta_cobertura: 60 } },
      { name: 'caixa_disponivel', input: {} },
    ],
    autoPrompt: `Você recebe dados pré-calculados das seguintes tools: portfolio_summary, pareto, skus_orfaos, recomendacao_compra, caixa_disponivel.
Produza a análise completa conforme o systemPrompt usando esses dados.
Para perguntas do gestor que exijam visão completa, prefira chamar a tool analise_completa em vez das tools individuais.`,
    async fetchData() { return {}; },
  },

  financeiro: {
    label: 'Financeiro',
    icon: '💰',
    description: 'DRE, Balanço, índices financeiros e tendência de caixa',
    tools: FINANCEIRO_TOOLS,
    autoTools: [{ name: 'analise_completa_financeiro', input: { meses_historico: 6 } }],
    systemPrompt: `Você é CFO analítico da {companyName}, e-commerce B2C.
Os dados vêm do plano de contas oficial mapeado pela empresa — use os totais fornecidos diretamente, não recalcule por prefixo de conta.
Interprete todos os indicadores com benchmark claro (saudável ≥ X | atenção entre X–Y | crítico < Y).
Se encontrar alertas contábeis (passivo zero, variação extrema, PL negativo), mencione como prioridade.
Responda em português brasileiro. Seja preciso, cite números reais.`,
    autoPrompt: `Você recebe dados financeiros pré-calculados do plano de contas oficial da empresa.
Use os campos exatos fornecidos. NÃO calcule totais por prefixo de conta — os totais já estão calculados corretamente via mapeamento.

Escreva neste formato:

## 💼 Posição Patrimonial (mês mais recente)
Ativo Total | Passivo Total | Patrimônio Líquido | Endividamento (%)

## 📈 Indicadores Financeiros
Liquidez Corrente | Giro do Ativo | ROE | ROA | Margem EBITDA | Margem Líquida
Para cada um: valor + benchmark (saudável/atenção/crítico) + interpretação em 1 frase.

## � DRE — Resultado do Período
Receita Bruta → Deduções → Receita Líquida → CMV → Lucro Bruto → EBITDA → Lucro Líquido
Cite margens (%). Identifique o maior dreno de margem.

## 💧 Fluxo de Caixa — Tendência
Tabela mês a mês: Entradas | Saídas | Saldo Livre. Diagnóstico de tendência + dias de caixa estimados.

## 🚨 Alertas Contábeis
Liste todos os alertas recebidos por gravidade. Se nenhum → "Sem anomalias detectadas".

## 📋 3 Prioridades Financeiras
Ação | Impacto estimado em R$ | Urgência`,
    toolExecutor: 'financeiro',
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

// ─── ORCHESTRATOR PROMPTS ────────────────────────────────────────────────────

const ORCHESTRATOR_ROUTING_PROMPT = `Você é um roteador de agentes de análise. Dado uma pergunta do gestor, retorne um JSON com os agentes a acionar.

Agentes disponíveis:
- sop: Cobertura de dias, rupturas iminentes, ponto de pedido, reposição urgente
- estoque: Portfólio, giro, tendências, aceleração/desaceleração, descontinuação de SKUs
- financeiro: DRE, balanço patrimonial, liquidez, endividamento, indicadores contábeis
- vendas: Performance comercial, crescimento MoM, mix de canal, margem por SKU
- caixa: Fluxo de caixa, burn rate, posição de caixa, entradas e saídas bancárias

Regras:
- Selecione SOMENTE os agentes necessários para responder a pergunta
- Para análises completas de saúde do negócio, selecione todos
- Retorne APENAS JSON válido, sem markdown, sem explicação extra

Formato de resposta obrigatório:
{"agents": ["sop", "caixa"], "rationale": "Explicação em 1 frase de por que esses agentes foram selecionados"}`;

const ORCHESTRATOR_SYNTHESIS_PROMPT = `Você é o Diretor Executivo integrado da {companyName}, acumulando as funções de CFO, S&OP Director, Controller e Head de Vendas.

Você recebe dados pré-calculados de MÚLTIPLOS módulos do negócio simultaneamente.
Sua missão é produzir uma análise CROSS-MÓDULO — identificar conexões, causalidades e riscos que NENHUM analista isolado enxergaria.

Regras obrigatórias:
- SEMPRE cruzar dados de módulos diferentes em pelo menos uma seção
- Cite números reais dos dados. Proibido generalizar sem referência a valores concretos
- Identifique causalidades: ex. "A ruptura do SKU X está comprimindo a margem do canal Y"
- Priorize decisões por impacto financeiro estimado em R$
- Responda em português brasileiro

Formato obrigatório da resposta:

## 🔍 Diagnóstico Integrado
O que os dados revelam quando lidos EM CONJUNTO. Cruzamentos entre módulos. 3–5 achados principais com valores.

## ⚠️ Riscos Cross-Módulo
Riscos que só aparecem ao combinar os módulos — ex: "ruptura + caixa baixo = impossível repor". Calcule impacto em R$.

## 📊 Situação por Módulo
Resumo executivo de cada módulo ativado (2–3 linhas cada, apenas os mais críticos).

## 🎯 Decisões Prioritárias — Próximos 30 dias
No máximo 5 ações, ordenadas por impacto financeiro estimado. Formato: Ação | Módulo | Impacto R$ | Urgência`;

// ─── CALL LLM (síntese principal — Anthropic Claude) ────────────────────────

const SYSTEM_FOOTER = `

REGRAS ABSOLUTAS DE INTERPRETAÇÃO DE VALORES:
- Todos os valores numéricos estão em REAIS (R$) exatos. O que está no dado É o valor real.
- 850000 = R$ 850.000,00 (oitocentos e cinquenta MIL reais) — NÃO é milhão.
- 1500000 = R$ 1.500.000,00 (um milhão e meio) — NÃO é 1,5 bilhão.
- NUNCA multiplique nem divida os valores recebidos.
- Sempre formate usando padrão brasileiro: ponto para milhar, vírgula para decimal.
- Exemplo correto: R$ 850.000,00 | R$ 1.234.567,89`;

async function callLLM({ systemPrompt, userMessage, companyName, model, maxTokens }) {
  if (!process.env.ANTHROPIC_API_KEY) {
    throw new Error('ANTHROPIC_API_KEY não está configurada. Obtenha em: https://console.anthropic.com/settings/keys');
  }
  const maxLen = 40000;
  const msg = userMessage.length > maxLen ? userMessage.slice(0, maxLen) + '\n[dados truncados]' : userMessage;
  const response = await fetch('https://api.anthropic.com/v1/messages', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'x-api-key': process.env.ANTHROPIC_API_KEY,
      'anthropic-version': '2023-06-01',
    },
    body: JSON.stringify({
      model: model || 'claude-sonnet-4-5',
      max_tokens: maxTokens || 2000,
      system: systemPrompt.replace('{companyName}', companyName) + SYSTEM_FOOTER,
      messages: [
        { role: 'user', content: msg },
      ],
    }),
  });
  if (!response.ok) {
    const errText = await response.text();
    throw new Error(`Anthropic API error ${response.status}: ${errText}`);
  }
  const data = await response.json();
  return data.content[0].text;
}

// ─── CALL FLASH (roteamento leve — Anthropic Claude Haiku) ─────────────────

async function callFlash(question) {
  if (!process.env.ANTHROPIC_API_KEY) throw new Error('ANTHROPIC_API_KEY não configurada');
  const response = await fetch('https://api.anthropic.com/v1/messages', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'x-api-key': process.env.ANTHROPIC_API_KEY,
      'anthropic-version': '2023-06-01',
    },
    body: JSON.stringify({
      model: 'claude-sonnet-4-5',
      max_tokens: 300,
      system: ORCHESTRATOR_ROUTING_PROMPT,
      messages: [
        { role: 'user', content: question },
      ],
    }),
  });
  if (!response.ok) throw new Error(`Flash routing error ${response.status}`);
  const data = await response.json();
  const raw = (data.content[0].text || '').trim();
  try {
    const clean = raw.replace(/```json|```/g, '').trim();
    return JSON.parse(clean);
  } catch {
    console.warn('[callFlash] parse failed, activating all agents:', raw);
    return { agents: Object.keys(AGENTS), rationale: 'Fallback: todos os agentes ativados' };
  }
}

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
          SELECT UPPER(TRIM(sku::text)) AS sku_k, estoque_atual::numeric AS estoque
          FROM ponto_pedido WHERE sku IS NOT NULL
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
          SELECT UPPER(TRIM(sku::text)) AS sku, estoque_atual::numeric AS estoque
          FROM ponto_pedido WHERE sku IS NOT NULL AND estoque_atual::numeric > 0
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
          SELECT UPPER(TRIM(sku::text)) AS sku_k, estoque_atual::numeric AS estoque
          FROM ponto_pedido WHERE sku IS NOT NULL
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
          SELECT ROUND(SUM(GREATEST(0, m.media_3m * 2 - COALESCE(pp.estoque_atual::numeric, 0)) * COALESCE(m.custo_unit, 0)), 2) AS custo_reposicao_criticos_R$
          FROM ponto_pedido pp
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

// ─── TOOL EXECUTOR — FINANCEIRO ──────────────────────────────────────────────

const SEC_ID_TO_LABEL = {
  'ativo-circulante-body':       'Ativo Circulante',
  'ativo-nao-circulante-body':   'Ativo Não Circulante',
  'passivo-circulante-body':     'Passivo Circulante',
  'passivo-nao-circulante-body': 'Passivo Não Circulante',
  'pl-body':                     'Patrimônio Líquido',
};

const ATIVO_SECS  = ['ativo-circulante-body', 'ativo-nao-circulante-body'];
const PASSIVO_SECS = ['passivo-circulante-body', 'passivo-nao-circulante-body'];

async function loadEstrutura(pool, company, tipos) {
  const rows = await safeQuery(
    pool,
    `SELECT tipo, dados FROM dfs_estrutura WHERE empresa = $1 AND tipo = ANY($2::text[])`,
    [company, tipos]
  );
  const out = {};
  rows.forEach(r => { out[r.tipo] = r.dados || {}; });
  return out;
}

function resolveBalancoTotals(structure, mappings, balMap) {
  const secTotals = {};
  Object.keys(SEC_ID_TO_LABEL).forEach(secId => {
    const rows = structure[secId] || [];
    let secTotal = 0;
    const rowDetails = [];
    rows.forEach(row => {
      const key = `${secId}:${row.code}`;
      const contas = mappings[key];
      if (!contas) return;
      const contasList = Array.isArray(contas) ? contas : [contas];
      const valor = contasList.reduce((s, c) => s + (parseFloat(balMap[String(c)] || 0)), 0);
      rowDetails.push({ nome: row.name, valor: +valor.toFixed(2) });
      secTotal += valor;
    });
    secTotals[secId] = { label: SEC_ID_TO_LABEL[secId], total: +secTotal.toFixed(2), rows: rowDetails };
  });
  return secTotals;
}

async function executeFinanceiroTool(toolName, input, pool, company) {
  const BLOCKED_FIN = ['usuarios', 'belvo_links'];

  switch (toolName) {

    case 'balanco_patrimonial': {
      const estrutura = await loadEstrutura(pool, company, ['structure', 'mappings']);
      const structure = estrutura.structure || {};
      const mappings  = estrutura.mappings  || {};

      if (!Object.keys(structure).length) {
        return { erro: 'Mapeamento do Balanço não configurado. Acesse Configurações → Plano de Contas para mapear as contas.' };
      }

      const [periodo, balRows] = await Promise.all([
        safeQuery(pool, `SELECT MAX(ano) AS ano, MAX(mes) FILTER (WHERE ano=(SELECT MAX(ano) FROM dfs_balanco WHERE empresa=$1)) AS mes FROM dfs_balanco WHERE empresa=$1`, [company]),
        (async () => {
          const ano = input.ano || null;
          const mes = input.mes || null;
          if (ano && mes) {
            return safeQuery(pool, `SELECT conta, saldo_atual::numeric AS saldo FROM dfs_balanco WHERE empresa=$1 AND ano=$2 AND mes=$3`, [company, ano, mes]);
          }
          return safeQuery(pool, `SELECT conta, saldo_atual::numeric AS saldo FROM dfs_balanco WHERE empresa=$1 AND ano=(SELECT MAX(ano) FROM dfs_balanco WHERE empresa=$1) AND mes=(SELECT MAX(mes) FROM dfs_balanco WHERE empresa=$1 AND ano=(SELECT MAX(ano) FROM dfs_balanco WHERE empresa=$1))`, [company]);
        })(),
      ]);

      const balMap = {};
      balRows.forEach(r => { balMap[String(r.conta || '').trim()] = parseFloat(r.saldo) || 0; });

      const secTotals = resolveBalancoTotals(structure, mappings, balMap);

      const ativoCirc  = secTotals['ativo-circulante-body']?.total || 0;
      const ativoNCirc = secTotals['ativo-nao-circulante-body']?.total || 0;
      const passCirc   = secTotals['passivo-circulante-body']?.total || 0;
      const passNCirc  = secTotals['passivo-nao-circulante-body']?.total || 0;
      const pl         = secTotals['pl-body']?.total || 0;
      const ativoTotal = ativoCirc + ativoNCirc;
      const passTotal  = passCirc + passNCirc;
      const plCalc     = ativoTotal - passTotal;

      const liquidezCorr = passCirc > 0 ? +(ativoCirc / passCirc).toFixed(2) : null;
      const endividamento = ativoTotal > 0 ? +((passTotal / ativoTotal) * 100).toFixed(1) : null;

      return {
        periodo: { ano: periodo[0]?.ano, mes: periodo[0]?.mes },
        ativo_circulante: ativoCirc,
        ativo_nao_circulante: ativoNCirc,
        ativo_total: ativoTotal,
        passivo_circulante: passCirc,
        passivo_nao_circulante: passNCirc,
        passivo_total: passTotal,
        patrimonio_liquido_mapeado: pl,
        patrimonio_liquido_calculado: +plCalc.toFixed(2),
        indicadores: {
          liquidez_corrente: liquidezCorr,
          endividamento_pct: endividamento,
          benchmark_liquidez: liquidezCorr === null ? 'sem_dados' : liquidezCorr >= 1.5 ? 'saudavel' : liquidezCorr >= 1 ? 'atencao' : 'critico',
          benchmark_endividamento: endividamento === null ? 'sem_dados' : endividamento < 50 ? 'saudavel' : endividamento < 70 ? 'atencao' : 'critico',
        },
        detalhe_por_secao: secTotals,
      };
    }

    case 'dre': {
      const estrutura = await loadEstrutura(pool, company, ['dre_structure', 'dre_mappings']);
      const dreStruct  = estrutura.dre_structure || {};
      const dreMappings = estrutura.dre_mappings  || {};

      if (!Object.keys(dreStruct).filter(k => !k.startsWith('_')).length) {
        return { erro: 'Mapeamento da DRE não configurado. Acesse Configurações → Plano de Contas DRE.' };
      }

      const signs    = dreStruct._signs    || {};
      const calcMode = dreStruct._calcMode || {};

      const mesRef = input.mes || null;
      const anoRef = input.ano || null;
      const mesesAcum = Math.min(input.meses_acumulado || 1, 12);

      const balRows = await (async () => {
        if (anoRef && mesRef) {
          return safeQuery(pool, `SELECT conta, mes, saldo_atual::numeric AS saldo, debito::numeric AS debito, credito::numeric AS credito FROM dfs_balanco WHERE empresa=$1 AND ano=$2 AND mes BETWEEN $3 AND $4`, [company, anoRef, Math.max(1, mesRef - mesesAcum + 1), mesRef]);
        }
        return safeQuery(pool, `SELECT conta, mes, saldo_atual::numeric AS saldo, debito::numeric AS debito, credito::numeric AS credito FROM dfs_balanco WHERE empresa=$1 AND ano=(SELECT MAX(ano) FROM dfs_balanco WHERE empresa=$1) AND mes=(SELECT MAX(mes) FROM dfs_balanco WHERE empresa=$1 AND ano=(SELECT MAX(ano) FROM dfs_balanco WHERE empresa=$1))`, [company]);
      })();

      const balByContaMes = {};
      balRows.forEach(r => {
        const k = `${r.conta}:${r.mes}`;
        balByContaMes[k] = { saldo: parseFloat(r.saldo) || 0, debito: parseFloat(r.debito) || 0, credito: parseFloat(r.credito) || 0 };
      });

      const dreBodyIdToLabel = {
        'dre-rob-body': 'Receita Bruta', 'dre-ded-body': 'Deduções', 'dre-cpe-body': 'CMV/CPV',
        'dre-lbr-body': 'Lucro Bruto', 'dre-dop-body': 'Despesas Operacionais',
        'dre-ebt-body': 'EBITDA', 'dre-daf-body': 'Resultado Financeiro',
        'dre-lai-body': 'Resultado Antes do IR', 'dre-imp-body': 'IR/CSLL',
        'dre-liq-body': 'Lucro Líquido',
      };

      const mesIds = [...new Set(balRows.map(r => r.mes))];
      const secResults = {};

      Object.keys(dreBodyIdToLabel).forEach(secId => {
        const rows = dreStruct[secId] || [];
        let secTotal = 0;
        rows.forEach(row => {
          const key = `${secId}:${row.code}`;
          const contas = dreMappings[key];
          if (!contas) return;
          const contasList = Array.isArray(contas) ? contas : [contas];
          contasList.forEach(conta => {
            mesIds.forEach(m => {
              const entry = balByContaMes[`${conta}:${m}`];
              if (!entry) return;
              const mode = calcMode[m] || 'saldo';
              const raw = mode === 'saldo' ? entry.saldo : entry.debito - entry.credito;
              const sign = signs[secId] !== undefined ? Number(signs[secId]) : -1;
              secTotal += sign * raw;
            });
          });
        });
        secResults[secId] = { label: dreBodyIdToLabel[secId], valor: +secTotal.toFixed(2) };
      });

      const receitaBruta   = secResults['dre-rob-body']?.valor || 0;
      const deducoes       = secResults['dre-ded-body']?.valor || 0;
      const receitaLiquida = receitaBruta + deducoes;
      const cmv            = secResults['dre-cpe-body']?.valor || 0;
      const lucroB         = receitaLiquida + cmv;
      const despOp         = secResults['dre-dop-body']?.valor || 0;
      const ebitda         = lucroB + despOp;
      const resFinanceiro  = secResults['dre-daf-body']?.valor || 0;
      const lucroLiq       = secResults['dre-liq-body']?.valor || 0;

      const pct = v => receitaLiquida !== 0 ? +((v / receitaLiquida) * 100).toFixed(1) : null;

      return {
        receita_bruta: receitaBruta,
        deducoes,
        receita_liquida: +receitaLiquida.toFixed(2),
        cmv,
        lucro_bruto: +lucroB.toFixed(2),
        despesas_operacionais: despOp,
        ebitda: +ebitda.toFixed(2),
        resultado_financeiro: resFinanceiro,
        lucro_liquido: lucroLiq,
        margens: {
          margem_bruta_pct: pct(lucroB),
          margem_ebitda_pct: pct(ebitda),
          margem_liquida_pct: pct(lucroLiq),
        },
        por_secao: secResults,
        meses_acumulado: mesesAcum,
      };
    }

    case 'fluxo_caixa': {
      const meses = Math.min(input.meses || 6, 12);
      const hist = await safeQuery(pool, `
        SELECT ano, mes,
          ROUND(SUM(CASE WHEN valor > 0 THEN valor ELSE 0 END) / 100.0, 2) AS entradas,
          ROUND(ABS(SUM(CASE WHEN valor < 0 THEN valor ELSE 0 END)) / 100.0, 2) AS saidas,
          ROUND(SUM(valor) / 100.0, 2) AS saldo_livre
        FROM dfs_fluxo_caixa_diario
        WHERE empresa = $1
        GROUP BY ano, mes ORDER BY ano DESC, mes DESC LIMIT $2
      `, [company, meses]);

      const burnRateMedio = hist.length > 0
        ? +(hist.slice(0, 3).reduce((s, r) => s + (parseFloat(r.saidas) || 0), 0) / Math.min(3, hist.length)).toFixed(2)
        : 0;
      const saldoAtual = parseFloat(hist[0]?.saldo_livre) || 0;
      const diasCaixa = burnRateMedio > 0 ? Math.round(saldoAtual / (burnRateMedio / 30)) : null;

      return {
        historico: hist,
        burn_rate_medio_R$: burnRateMedio,
        saldo_ultimo_mes_R$: saldoAtual,
        dias_caixa_estimados: diasCaixa,
        tendencia: hist.length >= 3
          ? (hist[0].saldo_livre > hist[2].saldo_livre ? 'melhorando' : hist[0].saldo_livre < hist[2].saldo_livre ? 'piorando' : 'estavel')
          : 'insuficiente',
      };
    }

    case 'indicadores_financeiros': {
      const [bp, dreData] = await Promise.all([
        executeFinanceiroTool('balanco_patrimonial', {}, pool, company),
        executeFinanceiroTool('dre', {}, pool, company),
      ]);
      if (bp.erro) return { erro: bp.erro };

      const ac  = bp.ativo_circulante  || 0;
      const at  = bp.ativo_total       || 0;
      const pc  = bp.passivo_circulante || 0;
      const pt  = bp.passivo_total     || 0;
      const pl  = bp.patrimonio_liquido_calculado || 0;
      const rl  = dreData.receita_liquida || 0;
      const ebt = dreData.ebitda || 0;
      const ll  = dreData.lucro_liquido || 0;

      const liqCorr = pc > 0 ? +(ac / pc).toFixed(2) : null;
      const roe     = pl > 0 ? +((ll / pl) * 100).toFixed(1) : null;
      const roa     = at > 0 ? +((ll / at) * 100).toFixed(1) : null;
      const endiv   = at > 0 ? +((pt / at) * 100).toFixed(1) : null;
      const giroAt  = at > 0 && rl !== 0 ? +(rl / at).toFixed(2) : null;
      const margEbt = rl !== 0 ? +((ebt / rl) * 100).toFixed(1) : null;

      function bench(val, good, warn, dir = 'asc') {
        if (val === null) return 'sem_dados';
        if (dir === 'asc') return val >= good ? 'saudavel' : val >= warn ? 'atencao' : 'critico';
        return val <= good ? 'saudavel' : val <= warn ? 'atencao' : 'critico';
      }

      return {
        liquidez_corrente:  { valor: liqCorr, benchmark: bench(liqCorr, 1.5, 1.0) },
        roe_pct:            { valor: roe,     benchmark: bench(roe, 15, 5) },
        roa_pct:            { valor: roa,     benchmark: bench(roa, 5, 2) },
        endividamento_pct:  { valor: endiv,   benchmark: bench(endiv, 50, 70, 'desc') },
        giro_ativo:         { valor: giroAt,  benchmark: bench(giroAt, 1, 0.5) },
        margem_ebitda_pct:  { valor: margEbt, benchmark: bench(margEbt, 20, 10) },
        margem_liquida_pct: { valor: dreData.margens?.margem_liquida_pct, benchmark: bench(dreData.margens?.margem_liquida_pct, 10, 3) },
      };
    }

    case 'evolucao_mensal': {
      const meses = Math.min(input.meses || 6, 24);
      const contasFiltro = (input.contas || []).map(c => String(c).trim());
      const estrutura = await loadEstrutura(pool, company, ['structure', 'mappings']);
      const structure = estrutura.structure || {};
      const mappings  = estrutura.mappings  || {};

      const periodos = await safeQuery(pool, `
        SELECT ano, mes FROM dfs_balanco WHERE empresa=$1
        GROUP BY ano, mes ORDER BY ano DESC, mes DESC LIMIT $2
      `, [company, meses]);

      const results = await Promise.all(periodos.map(async p => {
        const balRows = await safeQuery(pool, `SELECT conta, saldo_atual::numeric AS saldo FROM dfs_balanco WHERE empresa=$1 AND ano=$2 AND mes=$3`, [company, p.ano, p.mes]);
        const balMap = {};
        balRows.forEach(r => { balMap[String(r.conta || '').trim()] = parseFloat(r.saldo) || 0; });
        const secTotals = resolveBalancoTotals(structure, mappings, balMap);
        const ativoTotal  = (secTotals['ativo-circulante-body']?.total || 0) + (secTotals['ativo-nao-circulante-body']?.total || 0);
        const passTotal   = (secTotals['passivo-circulante-body']?.total || 0) + (secTotals['passivo-nao-circulante-body']?.total || 0);
        const entry = { ano: p.ano, mes: p.mes, ativo_total: ativoTotal, passivo_total: passTotal, pl: +(ativoTotal - passTotal).toFixed(2) };
        if (contasFiltro.length) {
          contasFiltro.forEach(c => { entry[`conta_${c}`] = +(balMap[c] || 0).toFixed(2); });
        }
        return entry;
      }));

      results.sort((a, b) => a.ano !== b.ano ? a.ano - b.ano : a.mes - b.mes);
      for (let i = 1; i < results.length; i++) {
        const prev = results[i - 1].ativo_total;
        const curr = results[i].ativo_total;
        results[i].variacao_ativo_pct = prev !== 0 ? +((curr - prev) / Math.abs(prev) * 100).toFixed(1) : null;
      }
      return { series: results, meses_consultados: meses };
    }

    case 'alertas_contabeis': {
      const estrutura = await loadEstrutura(pool, company, ['structure', 'mappings']);
      const structure = estrutura.structure || {};
      const mappings  = estrutura.mappings  || {};

      const periodos = await safeQuery(pool, `SELECT DISTINCT ano, mes FROM dfs_balanco WHERE empresa=$1 ORDER BY ano DESC, mes DESC LIMIT 2`, [company]);
      if (!periodos.length) return { alertas: [], aviso: 'Nenhum período encontrado no dfs_balanco.' };

      const [atualRows, anteriorRows] = await Promise.all([
        safeQuery(pool, `SELECT conta, nome, saldo_atual::numeric AS saldo FROM dfs_balanco WHERE empresa=$1 AND ano=$2 AND mes=$3`, [company, periodos[0].ano, periodos[0].mes]),
        periodos[1] ? safeQuery(pool, `SELECT conta, saldo_atual::numeric AS saldo FROM dfs_balanco WHERE empresa=$1 AND ano=$2 AND mes=$3`, [company, periodos[1].ano, periodos[1].mes]) : [],
      ]);

      const atualMap = {};
      atualRows.forEach(r => { atualMap[String(r.conta || '').trim()] = { saldo: parseFloat(r.saldo) || 0, nome: r.nome }; });
      const anteriorMap = {};
      anteriorRows.forEach(r => { anteriorMap[String(r.conta || '').trim()] = parseFloat(r.saldo) || 0; });

      const secTotals = resolveBalancoTotals(structure, mappings, Object.fromEntries(Object.entries(atualMap).map(([k, v]) => [k, v.saldo])));
      const ativoTotal = (secTotals['ativo-circulante-body']?.total || 0) + (secTotals['ativo-nao-circulante-body']?.total || 0);
      const passTotal  = (secTotals['passivo-circulante-body']?.total || 0) + (secTotals['passivo-nao-circulante-body']?.total || 0);
      const acTotal    = secTotals['ativo-circulante-body']?.total || 0;

      const alertas = [];

      if (ativoTotal > 0 && passTotal === 0) alertas.push({ gravidade: 'critico', tipo: 'PASSIVO_ZERO', descricao: `Passivo total = R$ 0 com Ativo de R$ ${ativoTotal.toFixed(2)}. Mapeamento incompleto ou dados ausentes.` });
      if (ativoTotal - passTotal < 0) alertas.push({ gravidade: 'critico', tipo: 'PL_NEGATIVO', descricao: `PL calculado negativo: R$ ${(ativoTotal - passTotal).toFixed(2)}. Passivo supera Ativo.` });
      if (acTotal < 0) alertas.push({ gravidade: 'critico', tipo: 'AC_NEGATIVO', descricao: `Ativo Circulante negativo: R$ ${acTotal.toFixed(2)}.` });

      atualRows.forEach(r => {
        const conta = String(r.conta || '').trim();
        const saldo = parseFloat(r.saldo) || 0;
        const anterior = anteriorMap[conta];
        if (anterior !== undefined && anterior !== 0) {
          const varPct = Math.abs((saldo - anterior) / Math.abs(anterior) * 100);
          if (varPct > 200) alertas.push({ gravidade: 'critico', tipo: 'VARIACAO_EXTREMA', conta, nome: r.nome, variacao_pct: +varPct.toFixed(0), saldo_atual: saldo, saldo_anterior: anterior });
          else if (varPct > 50) alertas.push({ gravidade: 'atencao', tipo: 'VARIACAO_ALTA', conta, nome: r.nome, variacao_pct: +varPct.toFixed(0), saldo_atual: saldo, saldo_anterior: anterior });
        }
        if (saldo < 0 && ATIVO_SECS.some(sec => {
          const rows = structure[sec] || [];
          return rows.some(row => {
            const contas = mappings[`${sec}:${row.code}`];
            const list = contas ? (Array.isArray(contas) ? contas : [contas]) : [];
            return list.includes(conta);
          });
        })) {
          alertas.push({ gravidade: 'atencao', tipo: 'ATIVO_NEGATIVO', conta, nome: r.nome, saldo });
        }
      });

      alertas.sort((a, b) => ({ critico: 0, atencao: 1, informativo: 2 }[a.gravidade] - { critico: 0, atencao: 1, informativo: 2 }[b.gravidade]));
      return { periodo: periodos[0], total_alertas: alertas.length, alertas: alertas.slice(0, 20) };
    }

    case 'analise_completa_financeiro': {
      const mesesHist = input.meses_historico || 6;
      const [bp, dreData, fcx, indicadores, alertas] = await Promise.all([
        executeFinanceiroTool('balanco_patrimonial', {}, pool, company),
        executeFinanceiroTool('dre', {}, pool, company),
        executeFinanceiroTool('fluxo_caixa', { meses: mesesHist }, pool, company),
        executeFinanceiroTool('indicadores_financeiros', {}, pool, company),
        executeFinanceiroTool('alertas_contabeis', {}, pool, company),
      ]);
      return { balanco_patrimonial: bp, dre: dreData, fluxo_caixa: fcx, indicadores_financeiros: indicadores, alertas_contabeis: alertas };
    }

    case 'executar_sql_financeiro': {
      const sql = (input.sql || '').trim();
      if (!/^\s*SELECT\s/i.test(sql)) return { error: 'Apenas queries SELECT são permitidas' };
      if (!/LIMIT\s+\d+/i.test(sql)) return { error: 'Query deve conter LIMIT. Máximo recomendado: 100' };
      if (BLOCKED_FIN.some(t => sql.toLowerCase().includes(t))) return { error: 'Tabela bloqueada para agentes' };
      return await safeQuery(pool, sql);
    }

    default:
      return { error: `Tool financeiro '${toolName}' não reconhecida` };
  }
}

// ─── CALL LLM WITH TOOLS (agentic loop) ──────────────────────────────────────

const TOOL_RESULT_MAX_CHARS = 1500;
const TOOL_RESULT_MAX_CHARS_LARGE = 5000;

async function anthropicRequest(body) {
  const maxRetries = 3;
  for (let attempt = 0; attempt < maxRetries; attempt++) {
    const response = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': process.env.ANTHROPIC_API_KEY,
        'anthropic-version': '2023-06-01',
      },
      body: JSON.stringify(body),
    });
    if (response.status === 429) {
      const wait = (attempt + 1) * 20000;
      console.warn(`[anthropicRequest] 429 rate limit, aguardando ${wait/1000}s (tentativa ${attempt+1}/${maxRetries})`);
      await new Promise(r => setTimeout(r, wait));
      continue;
    }
    if (!response.ok) {
      const errText = await response.text();
      throw new Error(`Anthropic API error ${response.status}: ${errText}`);
    }
    return response.json();
  }
  throw new Error('Anthropic API: limite de tentativas esgotado após rate limit 429');
}

async function callLLMWithTools({ systemPrompt, userMessage, tools, executeTool, companyName, maxRounds = 12, maxTokens = 4000 }) {
  if (!process.env.ANTHROPIC_API_KEY) throw new Error('ANTHROPIC_API_KEY não configurada');

  const sysPrompt = systemPrompt.replace('{companyName}', companyName) + SYSTEM_FOOTER;
  const messages = [{ role: 'user', content: userMessage }];
  let lastStopReason = '';

  for (let round = 0; round < maxRounds; round++) {
    const data = await anthropicRequest({
      model: 'claude-haiku-4-5-20251001',
      max_tokens: maxTokens,
      system: sysPrompt,
      tools,
      messages,
    });

    lastStopReason = data.stop_reason;
    messages.push({ role: 'assistant', content: data.content });

    if (data.stop_reason === 'end_turn') {
      const textBlock = data.content.find(b => b.type === 'text');
      return textBlock?.text || '';
    }

    if (data.stop_reason === 'tool_use') {
      const toolUses = data.content.filter(b => b.type === 'tool_use');
      const toolResults = await Promise.all(toolUses.map(async tu => {
        let result;
        try {
          result = await executeTool(tu.name, tu.input || {});
        } catch (e) {
          console.error(`[tool:${tu.name}]`, e.message);
          result = { error: e.message };
        }
        let content = JSON.stringify(result);
        const LARGE_TOOLS = ['analise_completa', 'analise_completa_financeiro'];
        const maxChars = LARGE_TOOLS.includes(tu.name) ? TOOL_RESULT_MAX_CHARS_LARGE : TOOL_RESULT_MAX_CHARS;
        if (content.length > maxChars) {
          content = content.slice(0, maxChars) + '... [truncado para economizar tokens]';
        }
        return { type: 'tool_result', tool_use_id: tu.id, content };
      }));
      messages.push({ role: 'user', content: toolResults });
    } else {
      break;
    }
  }

  const lastAssistant = [...messages].reverse().find(m => m.role === 'assistant');
  const textBlock = (lastAssistant?.content || []).find(b => b.type === 'text');
  if (textBlock?.text) return textBlock.text;
  return 'Análise não concluída — tente novamente.';
}

// ─── ORCHESTRATOR ─────────────────────────────────────────────────────────────

async function runOrchestrator({ pool, companyName, question, agentKeys }) {
  const keys = (agentKeys || []).filter(k => AGENTS[k]);
  if (!keys.length) throw new Error('Nenhum agente válido selecionado');

  const dataResults = await Promise.allSettled(keys.map(k => AGENTS[k].fetchData(pool)));
  const combinedData = {};
  keys.forEach((k, i) => {
    if (dataResults[i].status === 'fulfilled') combinedData[k] = dataResults[i].value;
    else { console.error(`[orchestrator] fetchData ${k}:`, dataResults[i].reason?.message); combinedData[k] = { erro: dataResults[i].reason?.message }; }
  });

  const moduloDescriptions = keys.map(k => `### Módulo: ${AGENTS[k].label}\n${JSON.stringify(combinedData[k], null, 2)}`).join('\n\n');
  const userMsg = question
    ? `Pergunta do gestor: ${question}\n\nDados dos módulos ativados:\n${moduloDescriptions}`
    : `Análise completa de saúde do negócio — todos os módulos ativos.\n\nDados:\n${moduloDescriptions}`;

  const analysis = await callLLM({
    systemPrompt: ORCHESTRATOR_SYNTHESIS_PROMPT,
    userMessage: userMsg,
    companyName,
  });

  return { analysis, agents_activated: keys, data_summary: combinedData };
}

// ─── TOOL EXECUTOR DISPATCHER ────────────────────────────────────────────────

function getToolExecutor(agentKey, pool, company) {
  const agent = AGENTS[agentKey];
  if (agent?.toolExecutor === 'financeiro') {
    return (name, inp) => executeFinanceiroTool(name, inp, pool, company);
  }
  return (name, inp) => executeEstoqueTool(name, inp, pool, company);
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

    // Orquestrador: ativa todos os agentes em paralelo
    if (agentKey === 'orchestrator') {
      try {
        const result = await runOrchestrator({ pool, companyName, question: null, agentKeys: Object.keys(AGENTS) });
        return res.json({ agent: 'orchestrator', ...result });
      } catch (e) {
        console.error('[AGENTS GET] orchestrator:', e.message);
        return res.status(500).json({ error: e.message });
      }
    }

    const agent = AGENTS[agentKey];
    if (!agent) return res.status(400).json({ error: `Agente '${agentKey}' não encontrado` });

    const cacheKey = `${company}_${agentKey}`;
    const cached = analysisCache[cacheKey];
    if (cached && Date.now() - cached.ts < 30 * 60 * 1000) {
      return res.json({ agent: agentKey, analysis: cached.analysis, cached: true });
    }

    try {
      let analysis;
      const execTool = getToolExecutor(agentKey, pool, company);
      if (agent.autoTools) {
        const results = await Promise.all(
          agent.autoTools.map(t => execTool(t.name, t.input))
        );
        const toolData = {};
        agent.autoTools.forEach((t, i) => { toolData[t.name] = results[i]; });
        analysis = await callLLM({
          systemPrompt: agent.systemPrompt,
          userMessage: `${agent.autoPrompt}\n\nDados:\n${JSON.stringify(toolData, null, 2)}`,
          companyName,
          maxTokens: 5000,
        });
        analysisCache[cacheKey] = { analysis, ts: Date.now() };
        return res.json({ agent: agentKey, analysis });
      } else if (agent.tools) {
        analysis = await callLLMWithTools({
          systemPrompt: agent.systemPrompt,
          userMessage: agent.autoPrompt,
          tools: agent.tools,
          executeTool: execTool,
          companyName,
          maxTokens: 5000,
        });
        return res.json({ agent: agentKey, analysis });
      } else {
        const data = await agent.fetchData(pool);
        analysis = await callLLM({
          systemPrompt: agent.systemPrompt,
          userMessage: `${agent.autoPrompt}\n\nDados do banco de dados:\n${JSON.stringify(data, null, 2)}`,
          companyName,
          model: 'claude-sonnet-4-5',
          maxTokens: 1500,
        });
        analysisCache[cacheKey] = { analysis, ts: Date.now() };
        return res.json({ agent: agentKey, analysis, data });
      }
    } catch (e) {
      console.error(`[AGENTS GET] ${agentKey}:`, e.message);
      return res.status(500).json({ error: e.message });
    }
  }

  // POST /api/agents → chat com agente OU orquestrador
  if (req.method === 'POST') {
    const { agent: agentKey, message, contextData } = req.body || {};
    if (!agentKey || !message) return res.status(400).json({ error: 'agent e message são obrigatórios' });

    // Orquestrador: roteamento via Flash + síntese via Pro
    if (agentKey === 'orchestrator') {
      try {
        const routing = await callFlash(message);
        console.log('[orchestrator] roteamento:', routing);
        const result = await runOrchestrator({ pool, companyName, question: message, agentKeys: routing.agents });
        return res.json({ agent: 'orchestrator', routing_rationale: routing.rationale, ...result });
      } catch (e) {
        console.error('[AGENTS POST] orchestrator:', e.message);
        return res.status(500).json({ error: e.message });
      }
    }

    const agent = AGENTS[agentKey];
    if (!agent) return res.status(400).json({ error: `Agente '${agentKey}' não encontrado` });

    try {
      let reply;
      if (agent.tools) {
        const execTool = getToolExecutor(agentKey, pool, company);
        reply = await callLLMWithTools({
          systemPrompt: agent.systemPrompt,
          userMessage: `Pergunta do gestor: ${message}\n\nUse apenas as tools necessárias para responder. Não é preciso chamar todas.`,
          tools: agent.tools,
          executeTool: execTool,
          companyName,
          maxTokens: 5000,
        });
      } else {
        let data = contextData;
        if (!data) data = await agent.fetchData(pool);
        reply = await callLLM({
          systemPrompt: agent.systemPrompt,
          userMessage: `Pergunta do gestor: ${message}\n\nContexto dos dados:\n${JSON.stringify(data, null, 2)}`,
          companyName,
          model: 'claude-sonnet-4-5',
          maxTokens: 1500,
        });
      }
      return res.json({ reply });
    } catch (e) {
      console.error(`[AGENTS POST] ${agentKey}:`, e.message);
      return res.status(500).json({ error: e.message });
    }
  }

  return res.status(405).json({ error: 'Method not allowed' });
};
