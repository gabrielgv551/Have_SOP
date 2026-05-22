// ── Agente Financeiro — definição de tools e executor ────────────────────────

const { safeQuery } = require('./shared');

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

// ─── Helpers internos ────────────────────────────────────────────────────────

const SEC_ID_TO_LABEL = {
  'ativo-circulante-body':       'Ativo Circulante',
  'ativo-nao-circulante-body':   'Ativo Não Circulante',
  'passivo-circulante-body':     'Passivo Circulante',
  'passivo-nao-circulante-body': 'Passivo Não Circulante',
  'pl-body':                     'Patrimônio Líquido',
};

const ATIVO_SECS   = ['ativo-circulante-body', 'ativo-nao-circulante-body'];

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

// ─── TOOL EXECUTOR — FINANCEIRO ──────────────────────────────────────────────

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
      balRows.forEach(r => { balMap[String(r.conta || '').trim()] = (parseFloat(r.saldo) || 0) / 100; });

      const secTotals = resolveBalancoTotals(structure, mappings, balMap);

      const ativoCirc  = secTotals['ativo-circulante-body']?.total || 0;
      const ativoNCirc = secTotals['ativo-nao-circulante-body']?.total || 0;
      const passCirc   = secTotals['passivo-circulante-body']?.total || 0;
      const passNCirc  = secTotals['passivo-nao-circulante-body']?.total || 0;
      const pl         = secTotals['pl-body']?.total || 0;
      const ativoTotal = ativoCirc + ativoNCirc;
      const passTotal  = passCirc + passNCirc;
      const plCalc     = ativoTotal - Math.abs(passTotal);

      const liquidezCorr  = passCirc !== 0 ? +(ativoCirc / Math.abs(passCirc)).toFixed(2) : null;
      const endividamento = ativoTotal > 0 ? +((Math.abs(passTotal) / ativoTotal) * 100).toFixed(1) : null;

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
      const dreStruct   = estrutura.dre_structure || {};
      const dreMappings = estrutura.dre_mappings  || {};

      if (!Object.keys(dreStruct).filter(k => !k.startsWith('_')).length) {
        return { erro: 'Mapeamento da DRE não configurado. Acesse Configurações → Plano de Contas DRE.' };
      }

      const signs    = dreStruct._signs    || {};
      const calcMode = dreStruct._calcMode || {};

      const mesRef     = input.mes || null;
      const anoRef     = input.ano || null;
      const mesesAcum  = Math.min(input.meses_acumulado || 1, 12);

      const balRows = await (async () => {
        if (anoRef && mesRef) {
          return safeQuery(pool, `SELECT conta, mes, saldo_atual::numeric AS saldo, debito::numeric AS debito, credito::numeric AS credito FROM dfs_balanco WHERE empresa=$1 AND ano=$2 AND mes BETWEEN $3 AND $4`, [company, anoRef, Math.max(1, mesRef - mesesAcum + 1), mesRef]);
        }
        return safeQuery(pool, `SELECT conta, mes, saldo_atual::numeric AS saldo, debito::numeric AS debito, credito::numeric AS credito FROM dfs_balanco WHERE empresa=$1 AND ano=(SELECT MAX(ano) FROM dfs_balanco WHERE empresa=$1) AND mes=(SELECT MAX(mes) FROM dfs_balanco WHERE empresa=$1 AND ano=(SELECT MAX(ano) FROM dfs_balanco WHERE empresa=$1))`, [company]);
      })();

      const balByContaMes = {};
      balRows.forEach(r => {
        const k = `${r.conta}:${r.mes}`;
        balByContaMes[k] = { saldo: (parseFloat(r.saldo) || 0) / 100, debito: (parseFloat(r.debito) || 0) / 100, credito: (parseFloat(r.credito) || 0) / 100 };
      });

      const dreBodyIdToLabel = {};
      Object.keys(dreStruct).filter(k => !k.startsWith('_')).forEach(k => { dreBodyIdToLabel[k] = k; });

      const mesIds = [...new Set(balRows.map(r => r.mes))];
      const secResults = {};

      const allSecIds = new Set([
        ...Object.keys(dreBodyIdToLabel),
        ...Object.keys(dreMappings).map(k => k.split(':')[0]),
      ]);

      allSecIds.forEach(secId => {
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
              const raw  = mode === 'saldo' ? entry.saldo : entry.debito - entry.credito;
              const sign = signs[secId] !== undefined ? Number(signs[secId]) : -1;
              secTotal += sign * raw;
            });
          });
        });

        const directKey = `${secId}:__direct__`;
        if (dreMappings[directKey]) {
          const contas = dreMappings[directKey];
          const contasList = Array.isArray(contas) ? contas : [contas];
          contasList.forEach(conta => {
            mesIds.forEach(m => {
              const entry = balByContaMes[`${conta}:${m}`];
              if (!entry) return;
              const mode = calcMode[m] || 'saldo';
              const raw  = mode === 'saldo' ? entry.saldo : entry.debito - entry.credito;
              const sign = signs[secId] !== undefined ? Number(signs[secId]) : -1;
              secTotal += sign * raw;
            });
          });
        }

        secResults[secId] = { label: dreBodyIdToLabel[secId] || secId, valor: +secTotal.toFixed(2) };
      });

      const getVal = (...ids) => {
        for (const id of ids) {
          if (secResults[id] && secResults[id].valor !== 0) return secResults[id].valor;
        }
        return 0;
      };

      const receitaBruta   = getVal('dre-rob-body');
      const deducoes       = getVal('dre-deducoes-body', 'dre-ded-body');
      const receitaLiquida = receitaBruta + deducoes;
      const cmv            = getVal('dre-cmv-body', 'dre-cpe-body');
      const lucroB         = receitaLiquida + cmv;
      const despOp         = getVal('dre-desp-fixas-body', 'dre-dop-body') + getVal('dre-outros-custos-body');
      const ebitda         = lucroB + despOp;
      const resFinanceiro  = getVal('dre-fin-body', 'dre-daf-body');
      const ircsll         = getVal('dre-ircsll-body', 'dre-imp-body');
      const creditos       = getVal('dre-creditos-body');
      const lucroLiqCalc   = +Object.values(secResults).reduce((s, r) => s + (r.valor || 0), 0).toFixed(2);
      const lucroLiq       = secResults['dre-liq-body']?.valor || lucroLiqCalc;

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
      const diasCaixa  = burnRateMedio > 0 ? Math.round(saldoAtual / (burnRateMedio / 30)) : null;

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

      const liqCorr = pc !== 0 ? +(ac / Math.abs(pc)).toFixed(2) : null;
      const roe     = pl > 0 ? +((ll / pl) * 100).toFixed(1) : null;
      const roa     = at > 0 ? +((ll / at) * 100).toFixed(1) : null;
      const endiv   = at > 0 ? +((Math.abs(pt) / at) * 100).toFixed(1) : null;
      const giroAt  = at > 0 && rl !== 0 ? +((rl * 12) / at).toFixed(2) : null;
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
        balRows.forEach(r => { balMap[String(r.conta || '').trim()] = (parseFloat(r.saldo) || 0) / 100; });
        const secTotals  = resolveBalancoTotals(structure, mappings, balMap);
        const ativoTotal = (secTotals['ativo-circulante-body']?.total || 0) + (secTotals['ativo-nao-circulante-body']?.total || 0);
        const passTotal  = (secTotals['passivo-circulante-body']?.total || 0) + (secTotals['passivo-nao-circulante-body']?.total || 0);
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
      atualRows.forEach(r => { atualMap[String(r.conta || '').trim()] = { saldo: (parseFloat(r.saldo) || 0) / 100, nome: r.nome }; });
      const anteriorMap = {};
      anteriorRows.forEach(r => { anteriorMap[String(r.conta || '').trim()] = (parseFloat(r.saldo) || 0) / 100; });

      const secTotals  = resolveBalancoTotals(structure, mappings, Object.fromEntries(Object.entries(atualMap).map(([k, v]) => [k, v.saldo])));
      const ativoTotal = (secTotals['ativo-circulante-body']?.total || 0) + (secTotals['ativo-nao-circulante-body']?.total || 0);
      const passTotal  = (secTotals['passivo-circulante-body']?.total || 0) + (secTotals['passivo-nao-circulante-body']?.total || 0);
      const acTotal    = secTotals['ativo-circulante-body']?.total || 0;

      const alertas = [];

      if (ativoTotal > 0 && passTotal === 0) alertas.push({ gravidade: 'critico', tipo: 'PASSIVO_ZERO', descricao: `Passivo total = R$ 0 com Ativo de R$ ${ativoTotal.toFixed(2)}. Mapeamento incompleto ou dados ausentes.` });
      if (ativoTotal - Math.abs(passTotal) < 0) alertas.push({ gravidade: 'critico', tipo: 'PL_NEGATIVO', descricao: `PL calculado negativo: R$ ${(ativoTotal - Math.abs(passTotal)).toFixed(2)}. Passivo supera Ativo.` });
      if (acTotal < 0) alertas.push({ gravidade: 'critico', tipo: 'AC_NEGATIVO', descricao: `Ativo Circulante negativo: R$ ${acTotal.toFixed(2)}.` });

      atualRows.forEach(r => {
        const conta    = String(r.conta || '').trim();
        const saldo    = (parseFloat(r.saldo) || 0) / 100;
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

module.exports = { FINANCEIRO_TOOLS, executeFinanceiroTool };
