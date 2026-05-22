// ── Orquestrador de Agentes — AGENTS, prompts, callLLM, callFlash, loop ──────

const { SYSTEM_FOOTER, anthropicRequest } = require('../llm');
const { safeQuery, getSopcParams, TOOL_RESULT_MAX_CHARS, TOOL_RESULT_MAX_CHARS_LARGE, TOOL_RESULT_MAX_CHARS_SOPC, TOOL_RESULT_MAX_CHARS_SOPC_ITEM } = require('./shared');
const { ESTOQUE_TOOLS, executeEstoqueTool }     = require('./tools-estoque');
const { FINANCEIRO_TOOLS, executeFinanceiroTool } = require('./tools-financeiro');
const { SOPC_TOOLS, executeSopcTool }           = require('./tools-sopc');
const { VENDAS_TOOLS, executeVendasTool }       = require('./tools-vendas');

// ─── AGENTS DEFINITION ───────────────────────────────────────────────────────

const AGENTS = {

  sop: {
    label: 'S&OP',
    icon: '📦',
    description: 'Cobertura de dias, rupturas iminentes e reposição urgente',
    tools: SOPC_TOOLS,
    toolExecutor: 'sop',
    systemPrompt: `Analista S&OP sênior da {companyName}. Use as tools para buscar dados reais.

Regras:
- Base do portfólio é bd_vendas, não ponto_pedido
- Metas de cobertura configuradas para esta empresa: Curva A = {meta_a}d | Curva B = {meta_b}d | Curva C = {meta_c}d
- Lead time padrão: {lead_time} dias
- Toda reposição recomendada: SKU + qtd + R$ + data limite
- Separar SKUs acelerando COM estoque de acelerando EM RISCO de ruptura
- Receita em risco = receita_media_mensal real do bd_vendas
- Responder em português, citar SKUs e valores reais

Formato da resposta:
## 🚨 Rupturas (dias < {alerta_ruptura_dias}): tabela SKU|Estoque|Dias|Receita Risco/mês|Tendência|Ação
## ⚠️ Reposição Urgente ({alerta_ruptura_dias}-{alerta_abaixo_meta} dias): SKU + qtd sugerida + prazo
## 📦 Saúde ABC: quantos SKUs de cada curva por status
## ⚠️ Risco Encalhe: SKUs > {encalhe_dias} dias + capital imobilizado R$
## 🛒 Plano de Compras: tabela SKU|Curva|Qtd|R$|Data|Urgência + total vs caixa
## 🔍 Qualidade dos Dados: resultado do diagnostico_base
## ⚡ 3 Prioridades: ação + impacto R$ + prazo`,
    autoPrompt: `Chame as tools nesta ordem:
1. sopc_analise_completa (roda tudo em paralelo)
2. Se houver SKUs curva A em ruptura, chame sopc_rupturas_impacto para detalhar
3. Produza a análise completa conforme o systemPrompt`,
    async fetchData() { return {}; },
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

## 📊 DRE — Resultado do Período
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
    tools: VENDAS_TOOLS,
    toolExecutor: 'vendas',
    systemPrompt: `Você é analista comercial sênior da {companyName}, e-commerce B2C.
Use as tools para buscar dados reais — nunca invente números.

Regras:
- "Margem Produto" no bd_vendas é margem BRUTA (sem comissão de canal)
- Use vendas_margem_real_por_canal para mostrar margem real após taxas do marketplace
- Para quedas MoM > 10%: sempre chame vendas_diagnostico_queda para identificar causa raiz
- Separe sazonalidade de queda estrutural usando vendas_sazonalidade_historica
- Cite SKUs e canais com valores reais (R$ e %)
- Responda em português brasileiro

Formato da resposta:
## 📊 KPIs do Mês: receita | qtd | margem bruta | ticket | variação MoM e YoY
## 📈 Sazonalidade: índice histórico do mês atual vs esperado — é normal ou estrutural?
## 🔍 Diagnóstico de Queda: se MoM < -10%, decomposição volume/ticket/mix/canal/categoria
## 💰 Margem Real por Canal: tabela canal | receita | margem bruta% | comissão% | margem líquida% | status
## 🔄 Cohort de Portfólio: SKUs perdidos vs ganhos vs mesmo mês ano anterior
## ⚠️ Concentração de Risco: Herfindahl + top 5% + cenário ruptura top 3
## ⚡ 3 Prioridades: ação + impacto R$ + prazo`,
    autoPrompt: `Chame as tools nesta ordem:
1. vendas_analise_completa (kpi + sazonalidade + cohort + concentração em paralelo)
2. Se variacao_mom_pct < -10: chame vendas_diagnostico_queda para causa raiz
3. Chame vendas_margem_real_por_canal (meses:3) para margem após comissão
4. Se top_5_pct_receita > 40: destacar risco de concentração
5. Produza análise completa conforme o systemPrompt`,
  },

  caixa: {
    label: 'Caixa',
    icon: '🏦',
    description: 'Posição de caixa, burn rate, anomalias e projeção',
    systemPrompt: `Você é controller de caixa da {companyName}, e-commerce B2C.
Os dados incluem entradas/saídas pré-calculadas, saldo líquido e as maiores transações do mês.
Foco: posição real de caixa, burn rate, anomalias e capacidade de pagamento.
Calcule projeção de dias de caixa disponível com base no burn rate médio.
Se o campo fonte_dados for 'dfs_fluxo_caixa_diario', informe que os dados vêm do sistema contábil (não do extrato bancário).
Se o campo fonte_dados for 'sem_dados', informe claramente que nenhuma fonte de dados está disponível e oriente a configurar a integração bancária ou importar o extrato.
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
    async fetchData(pool, company) {
      const [extratoAtual, historico] = await Promise.all([
        safeQuery(pool, `
          SELECT dia, descricao, valor::numeric AS valor
          FROM caixa_extrato
          WHERE empresa = $1
            AND ano = (SELECT MAX(ano) FROM caixa_extrato WHERE empresa = $1)
            AND mes = (SELECT MAX(mes) FROM caixa_extrato WHERE empresa = $1
                       AND ano = (SELECT MAX(ano) FROM caixa_extrato WHERE empresa = $1))
          ORDER BY ABS(valor::numeric) DESC LIMIT 80
        `, [company]),
        safeQuery(pool, `
          SELECT ano, mes,
            ROUND(SUM(CASE WHEN valor::numeric > 0 THEN valor::numeric ELSE 0 END) / 100.0, 2) AS entradas,
            ROUND(ABS(SUM(CASE WHEN valor::numeric < 0 THEN valor::numeric ELSE 0 END)) / 100.0, 2) AS saidas,
            ROUND(SUM(valor::numeric) / 100.0, 2) AS saldo_livre,
            COUNT(*) AS lancamentos
          FROM caixa_extrato
          WHERE empresa = $1
          GROUP BY ano, mes ORDER BY ano DESC, mes DESC LIMIT 6
        `, [company]),
      ]);

      const temExtrato = historico.length > 0;

      if (temExtrato) {
        const entradas = extratoAtual.filter(r => (parseFloat(r.valor) || 0) > 0);
        const saidas   = extratoAtual.filter(r => (parseFloat(r.valor) || 0) < 0);
        const totE = entradas.reduce((s, r) => s + (parseFloat(r.valor) || 0), 0);
        const totS = saidas.reduce((s, r)   => s + (parseFloat(r.valor) || 0), 0);
        const saldo = totE + totS;

        const burnRateMedio = historico.length >= 1
          ? (historico.slice(0, 3).reduce((s, r) => s + (parseFloat(r.saidas) || 0), 0) / Math.min(3, historico.length)).toFixed(2)
          : '0.00';
        const diasCaixa = parseFloat(burnRateMedio) > 0 ? Math.round((saldo / 100) / (parseFloat(burnRateMedio) / 30)) : null;
        const variacaoMoM = historico[1]?.saldo_livre != null
          ? (parseFloat(historico[0]?.saldo_livre || 0) - parseFloat(historico[1]?.saldo_livre || 0)).toFixed(2)
          : null;

        return {
          fonte_dados: 'caixa_extrato',
          periodo: `${historico[0].mes}/${historico[0].ano}`,
          saldo_liquido_R$: (saldo / 100).toFixed(2),
          total_entradas_R$: (totE / 100).toFixed(2),
          total_saidas_R$: (Math.abs(totS) / 100).toFixed(2),
          burn_rate_medio_R$: burnRateMedio,
          dias_caixa_estimados: diasCaixa,
          variacao_saldo_vs_mes_anterior_R$: variacaoMoM,
          maiores_entradas: entradas.sort((a, b) => b.valor - a.valor).map(r => ({ ...r, valor_R$: (r.valor / 100).toFixed(2) })).slice(0, 8),
          maiores_saidas: saidas.sort((a, b) => a.valor - b.valor).map(r => ({ ...r, valor_R$: (Math.abs(r.valor) / 100).toFixed(2) })).slice(0, 8),
          historico_6m: historico,
        };
      }

      const dfsHist = await safeQuery(pool, `
        SELECT ano, mes,
          ROUND(SUM(CASE WHEN valor > 0 THEN valor ELSE 0 END) / 100.0, 2) AS entradas,
          ROUND(ABS(SUM(CASE WHEN valor < 0 THEN valor ELSE 0 END)) / 100.0, 2) AS saidas,
          ROUND(SUM(valor) / 100.0, 2) AS saldo_livre,
          COUNT(*) AS lancamentos
        FROM dfs_fluxo_caixa_diario
        WHERE empresa = $1
        GROUP BY ano, mes ORDER BY ano DESC, mes DESC LIMIT 6
      `, [company]);

      if (dfsHist.length === 0) {
        return {
          fonte_dados: 'sem_dados',
          periodo: 'N/A',
          saldo_liquido_R$: '0.00',
          total_entradas_R$: '0.00',
          total_saidas_R$: '0.00',
          burn_rate_medio_R$: '0.00',
          dias_caixa_estimados: null,
          variacao_saldo_vs_mes_anterior_R$: null,
          maiores_entradas: [],
          maiores_saidas: [],
          historico_6m: [],
        };
      }

      const dfsBurnRate = +(dfsHist.slice(0, 3).reduce((s, r) => s + (parseFloat(r.saidas) || 0), 0) / Math.min(3, dfsHist.length)).toFixed(2);
      const dfsSaldo    = parseFloat(dfsHist[0]?.saldo_livre) || 0;
      const dfsDias     = dfsBurnRate > 0 ? Math.round(dfsSaldo / (dfsBurnRate / 30)) : null;
      const dfsVariacao = dfsHist[1]?.saldo_livre != null
        ? (parseFloat(dfsHist[0]?.saldo_livre || 0) - parseFloat(dfsHist[1]?.saldo_livre || 0)).toFixed(2)
        : null;

      return {
        fonte_dados: 'dfs_fluxo_caixa_diario',
        periodo: `${dfsHist[0].mes}/${dfsHist[0].ano}`,
        saldo_liquido_R$: dfsSaldo.toFixed(2),
        total_entradas_R$: parseFloat(dfsHist[0]?.entradas || 0).toFixed(2),
        total_saidas_R$: parseFloat(dfsHist[0]?.saidas || 0).toFixed(2),
        burn_rate_medio_R$: dfsBurnRate.toFixed(2),
        dias_caixa_estimados: dfsDias,
        variacao_saldo_vs_mes_anterior_R$: dfsVariacao,
        maiores_entradas: [],
        maiores_saidas: [],
        historico_6m: dfsHist,
        aviso: 'Dados do sistema contábil (dfs_fluxo_caixa_diario). Para análise detalhada por transação, configure a integração bancária via Pluggy ou importe o extrato bancário.',
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

// ─── CALL LLM (síntese principal — Anthropic Claude) ─────────────────────────

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
      messages: [{ role: 'user', content: msg }],
    }),
  });
  if (!response.ok) {
    const errText = await response.text();
    throw new Error(`Anthropic API error ${response.status}: ${errText}`);
  }
  const data = await response.json();
  return data.content[0].text;
}

// ─── CALL FLASH (roteamento leve — Anthropic Claude Haiku) ───────────────────

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
      messages: [{ role: 'user', content: question }],
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

// ─── CALL LLM WITH TOOLS (agentic loop) ─────────────────────────────────────

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
        const LARGE_TOOLS     = ['analise_completa', 'analise_completa_financeiro'];
        const SOPC_MEGA_TOOLS = ['sopc_analise_completa'];
        const SOPC_ITEM_TOOLS = ['sopc_portfolio_saude', 'sopc_rupturas_impacto', 'sopc_reposicao_priorizada', 'sopc_encalhe_risco'];
        const maxChars = SOPC_MEGA_TOOLS.includes(tu.name) ? TOOL_RESULT_MAX_CHARS_SOPC
          : SOPC_ITEM_TOOLS.includes(tu.name) ? TOOL_RESULT_MAX_CHARS_SOPC_ITEM
          : LARGE_TOOLS.includes(tu.name) ? TOOL_RESULT_MAX_CHARS_LARGE
          : TOOL_RESULT_MAX_CHARS;
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

async function runOrchestrator({ pool, company, companyName, question, agentKeys }) {
  const keys = (agentKeys || []).filter(k => AGENTS[k]);
  if (!keys.length) throw new Error('Nenhum agente válido selecionado');

  const dataResults = await Promise.allSettled(keys.map(k => AGENTS[k].fetchData(pool, company)));
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
  if (agent?.toolExecutor === 'sop') {
    return (name, inp) => executeSopcTool(name, inp, pool, company);
  }
  if (agent?.toolExecutor === 'vendas') {
    return (name, inp) => executeVendasTool(name, inp, pool, company);
  }
  return (name, inp) => executeEstoqueTool(name, inp, pool, company);
}

module.exports = {
  AGENTS,
  callLLM,
  callFlash,
  callLLMWithTools,
  runOrchestrator,
  getToolExecutor,
  getSopcParams,
};
