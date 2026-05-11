'use strict';

/**
 * Motor de Projeção de Cenários — função pura, sem acesso ao banco.
 *
 * projetar(snapshot, eventos, horizonte) → ProjecaoResult
 *
 * @param {Array}  snapshot  Linhas de cenarios_snapshot_base
 *                           [{ mes, categoria, valor_centavos, origem }]
 *                           (pode ser diário; o engine agrega por mes+categoria)
 * @param {Array}  eventos   Linhas de cenario_eventos
 *                           [{ tipo, nome, data_inicio, parametros, ativo, ordem }]
 * @param {Object} horizonte { ano, mes_inicio, mes_fim }
 *
 * @returns {Object} {
 *   ano,
 *   meses: {
 *     4: { categorias: { Cat: valorCentavos }, entradas, saidas, liquido },
 *     5: { ... },
 *     ...
 *   }
 * }
 */
function projetar(snapshot, eventos, horizonte) {
  const { ano, mes_inicio, mes_fim } = horizonte;

  // Meses do horizonte (1-based integers)
  const meses = [];
  for (let m = mes_inicio; m <= mes_fim; m++) meses.push(m);

  // ── 1. Construir baseline a partir do snapshot ──────────────────────────
  const projecao = {};
  for (const m of meses) projecao[m] = {};

  for (const row of snapshot) {
    const m = typeof row.mes === 'number' ? row.mes : parseInt(row.mes);
    if (!projecao[m]) continue;
    const cat = row.categoria;
    const val = typeof row.valor_centavos === 'number'
      ? row.valor_centavos
      : parseInt(row.valor_centavos || 0);
    projecao[m][cat] = (projecao[m][cat] || 0) + val;
  }

  // ── 2. Aplicar eventos em ordem ─────────────────────────────────────────
  const ativos = (eventos || [])
    .filter(e => e.ativo !== false)
    .sort((a, b) => ((a.ordem || 0) - (b.ordem || 0)) || (a.id - b.id));

  for (const ev of ativos) {
    const p = ev.parametros || {};
    switch (ev.tipo) {
      case 'emprestimo':
        _aplicarEmprestimo(projecao, p, ano, meses);
        break;
      case 'compra_estoque':
        _aplicarCompraEstoque(projecao, p, ano, meses);
        break;
      case 'ajuste_faturamento':
        _aplicarAjusteFaturamento(projecao, p, meses);
        break;
      case 'venda':
        _aplicarVenda(projecao, p, ano, meses);
        break;
      case 'dividendo':
        _aplicarDividendo(projecao, p, ano);
        break;
      case 'imobilizado':
        _aplicarImobilizado(projecao, p, ano);
        break;
      case 'receita_recorrente':
        _aplicarReceitaRecorrente(projecao, p, ano, meses);
        break;
      case 'despesa_recorrente':
        _aplicarDespesaRecorrente(projecao, p, ano, meses);
        break;
      default:
        // tipos futuros ignorados sem erro
        break;
    }
  }

  // ── 3. Calcular totais por mês ───────────────────────────────────────────
  const resultado = { ano, meses: {} };
  for (const m of meses) {
    const cats = projecao[m];
    let entradas = 0;
    let saidas = 0;
    for (const v of Object.values(cats)) {
      if (v > 0) entradas += v;
      else if (v < 0) saidas += v;
    }
    resultado.meses[m] = {
      categorias: { ...cats },
      entradas,
      saidas,
      liquido: entradas + saidas,
    };
  }

  return resultado;
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers de data
// ─────────────────────────────────────────────────────────────────────────────

/** "2026-05-15" → { ano: 2026, mes: 5 } */
function _parseMes(dateStr) {
  const parts = String(dateStr).split('-');
  return { ano: parseInt(parts[0]), mes: parseInt(parts[1]) };
}

/** Avança n meses a partir de (ano, mes), respeitando rollover anual */
function _addMeses(ano, mes, n) {
  let a = ano;
  let m = mes + n;
  while (m > 12) { m -= 12; a++; }
  while (m < 1)  { m += 12; a--; }
  return { ano: a, mes: m };
}

/** Adiciona valor v à categoria cat no mês m (só se o mês está no horizonte) */
function _add(projecao, m, cat, v) {
  if (projecao[m] === undefined) return;
  if (!cat) return;
  projecao[m][cat] = (projecao[m][cat] || 0) + v;
}

// ─────────────────────────────────────────────────────────────────────────────
// Tabelas de amortização
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Price (amortização francesa) — prestação constante.
 * Retorna array de { principal, juros, total } com `n` elementos.
 */
function calcPrice(principal, taxaMensal, n) {
  if (n <= 0) return [];
  if (taxaMensal === 0) {
    const pmt = Math.round(principal / n);
    return Array.from({ length: n }, () => ({ principal: pmt, juros: 0, total: pmt }));
  }
  const i = taxaMensal;
  const pmt = Math.round(principal * (i * Math.pow(1 + i, n)) / (Math.pow(1 + i, n) - 1));
  let saldo = principal;
  return Array.from({ length: n }, () => {
    const juros = Math.round(saldo * i);
    const amort = Math.min(pmt - juros, saldo); // evita overshooting no último mês
    saldo = Math.max(0, saldo - amort);
    return { principal: amort, juros, total: amort + juros };
  });
}

/**
 * SAC (Sistema de Amortização Constante) — amortização fixa, juros decrescentes.
 * Retorna array de { principal, juros, total } com `n` elementos.
 */
function calcSAC(principal, taxaMensal, n) {
  if (n <= 0) return [];
  const amort = Math.round(principal / n);
  let saldo = principal;
  return Array.from({ length: n }, (_, idx) => {
    const isLast = idx === n - 1;
    const p = isLast ? saldo : amort; // último pega o resto por arredondamento
    const juros = Math.round(saldo * taxaMensal);
    saldo = Math.max(0, saldo - p);
    return { principal: p, juros, total: p + juros };
  });
}

// ─────────────────────────────────────────────────────────────────────────────
// Aplicadores
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Empréstimo / Captação de Dívida
 *
 * parametros: {
 *   valor_principal        : integer (centavos)
 *   tipo_amortizacao       : "price" | "sac"     (default: "price")
 *   taxa_juros_mensal      : float  (ex: 0.015)  (default: 0)
 *   carencia_meses         : integer             (default: 0)
 *   prazo_meses            : integer
 *   data_inicio            : "YYYY-MM-DD"        — mês em que o dinheiro entra
 *   categoria_entrada      : string              (default: "Captação de Recursos")
 *   categoria_parcela      : string              (default: "Serviço da Dívida")
 * }
 */
function _aplicarEmprestimo(projecao, p, anoBase, _meses) {
  const principal      = p.valor_principal || 0;
  const tipoAmort      = p.tipo_amortizacao || 'price';
  const taxa           = p.taxa_juros_mensal || 0;
  const carencia       = p.carencia_meses || 0;
  const prazo          = p.prazo_meses || 12;
  const catEntrada     = p.categoria_entrada || 'Captação de Recursos';
  const catParcela     = p.categoria_parcela || 'Serviço da Dívida';

  if (!p.data_inicio || !principal || !prazo) return;

  const { ano: anoEnt, mes: mesEnt } = _parseMes(p.data_inicio);

  // Entrada de caixa no mês da captação (apenas se dentro do ano-base)
  if (anoEnt === anoBase) {
    _add(projecao, mesEnt, catEntrada, principal);
  }

  // Gerar cronograma de amortização
  const schedule = tipoAmort === 'sac'
    ? calcSAC(principal, taxa, prazo)
    : calcPrice(principal, taxa, prazo);

  for (let i = 0; i < schedule.length; i++) {
    const { ano: aP, mes: mP } = _addMeses(anoEnt, mesEnt, carencia + i);
    if (aP === anoBase) {
      _add(projecao, mP, catParcela, -schedule[i].total);
    }
  }
}

/**
 * Compra de Estoque / Pedido de Compra
 *
 * parametros: {
 *   valor_total_centavos  : integer (centavos)
 *   data_pagamento        : "YYYY-MM-DD"   — mês do primeiro pagamento
 *   parcelas              : integer         (default 1 — à vista)
 *   categoria_saida       : string         (default: "Pagamento Fornecedores")
 * }
 */
function _aplicarCompraEstoque(projecao, p, anoBase, _meses) {
  const valor    = p.valor_total_centavos || 0;
  const parcelas = Math.max(1, parseInt(p.parcelas) || 1);
  const catSaida = p.categoria_saida || 'Pagamento Fornecedores';

  if (!p.data_pagamento || !valor) return;

  const { ano: anoIni, mes: mesIni } = _parseMes(p.data_pagamento);
  const parcela = Math.round(valor / parcelas);

  for (let i = 0; i < parcelas; i++) {
    const { ano: aP, mes: mP } = _addMeses(anoIni, mesIni, i);
    if (aP === anoBase) {
      _add(projecao, mP, catSaida, -parcela);
    }
  }
}

/**
 * Venda Pontual ou Parcelada
 *
 * parametros: {
 *   valor_total_centavos  : integer (centavos)
 *   data_recebimento      : "YYYY-MM-DD"   — mês do primeiro recebimento
 *   parcelas              : integer         (default 1 — à vista)
 *   categoria_receita     : string          (default "Receita de Vendas")
 * }
 */
function _aplicarVenda(projecao, p, anoBase, _meses) {
  const valor    = p.valor_total_centavos || 0;
  const parcelas = Math.max(1, parseInt(p.parcelas) || 1);
  const catRec   = p.categoria_receita || 'Receita de Vendas';

  if (!p.data_recebimento || !valor) return;

  const { ano: anoRec, mes: mesRec } = _parseMes(p.data_recebimento);
  const parcela = Math.round(valor / parcelas);

  for (let i = 0; i < parcelas; i++) {
    const { ano: aP, mes: mP } = _addMeses(anoRec, mesRec, i);
    if (aP === anoBase) {
      _add(projecao, mP, catRec, parcela);
    }
  }
}

/**
 * Dividendo / Distribuição de Lucros
 *
 * parametros: {
 *   valor_centavos  : integer (centavos)
 *   data_pagamento  : "YYYY-MM-DD"
 *   categoria_saida : string   (default "Distribuição de Lucros")
 * }
 */
function _aplicarDividendo(projecao, p, anoBase) {
  const valor   = p.valor_centavos || 0;
  const catSai  = p.categoria_saida || 'Distribuição de Lucros';

  if (!p.data_pagamento || !valor) return;

  const { ano, mes } = _parseMes(p.data_pagamento);
  if (ano === anoBase) {
    _add(projecao, mes, catSai, -valor);
  }
}

/**
 * Imobilizado / CAPEX — compra de ativo fixo, à vista ou parcelado
 *
 * parametros: {
 *   valor_total_centavos : integer (centavos)
 *   data_inicio          : "YYYY-MM-DD"   — mês do primeiro pagamento
 *   parcelas             : integer         (default 1)
 *   categoria_saida      : string          (default "Investimento em Imobilizado")
 * }
 */
function _aplicarImobilizado(projecao, p, anoBase) {
  const valor    = p.valor_total_centavos || 0;
  const parcelas = Math.max(1, parseInt(p.parcelas) || 1);
  const catSai   = p.categoria_saida || 'Investimento em Imobilizado';

  if (!p.data_inicio || !valor) return;

  const { ano: anoIni, mes: mesIni } = _parseMes(p.data_inicio);
  const parcela = Math.round(valor / parcelas);

  for (let i = 0; i < parcelas; i++) {
    const { ano: aP, mes: mP } = _addMeses(anoIni, mesIni, i);
    if (aP === anoBase) {
      _add(projecao, mP, catSai, -parcela);
    }
  }
}

/**
 * Receita Recorrente — novo contrato/cliente por N meses
 *
 * parametros: {
 *   valor_mensal_centavos : integer (centavos por mês)
 *   mes_inicio            : integer (1-12)
 *   duracao_meses         : integer
 *   categoria_receita     : string  (default "Receita Recorrente")
 * }
 */
function _aplicarReceitaRecorrente(projecao, p, anoBase, _meses) {
  const valorMensal = p.valor_mensal_centavos || 0;
  const mesInicio   = parseInt(p.mes_inicio) || 1;
  const duracao     = parseInt(p.duracao_meses) || 1;
  const catRec      = p.categoria_receita || 'Receita Recorrente';

  if (!valorMensal || !duracao) return;

  for (let i = 0; i < duracao; i++) {
    const { ano: aP, mes: mP } = _addMeses(anoBase, mesInicio - 1, i + 1);
    if (aP === anoBase) {
      _add(projecao, mP, catRec, valorMensal);
    }
  }
}

/**
 * Ajuste de Faturamento (crescimento / queda de receita)
 *
 * parametros: {
 *   fator                 : float    (ex: 1.10 = +10%, 0.90 = -10%)
 *   meses                 : int[]    — null ou ausente = todos os meses do horizonte
 *   categoria_receita     : string
 *   manter_margem         : boolean  (se true, ajusta CMV proporcionalmente)
 *   categoria_cmv         : string   (necessário quando manter_margem = true)
 * }
 */
function _aplicarAjusteFaturamento(projecao, p, meses) {
  const fator    = p.fator != null ? p.fator : 1;
  const catRec   = p.categoria_receita;
  const alvos    = Array.isArray(p.meses) && p.meses.length ? p.meses : meses;

  if (!catRec || fator === 1) return;

  for (const m of alvos) {
    if (projecao[m] === undefined) continue;
    const recAtual = projecao[m][catRec] || 0;
    projecao[m][catRec] = Math.round(recAtual * fator);

    if (p.manter_margem && p.categoria_cmv) {
      const cmv = projecao[m][p.categoria_cmv];
      if (cmv !== undefined) {
        projecao[m][p.categoria_cmv] = Math.round(cmv * fator);
      }
    }
  }
}

/**
 * Despesa Recorrente — custo fixo mensal por N meses (ex: marketing, aluguel)
 *
 * parametros: {
 *   valor_mensal_centavos : integer (centavos por mês)
 *   mes_inicio            : integer (1-12)
 *   duracao_meses         : integer
 *   categoria_saida       : string  (default "OUTRAS SAÍDAS")
 * }
 */
function _aplicarDespesaRecorrente(projecao, p, anoBase, _meses) {
  const valorMensal = p.valor_mensal_centavos || 0;
  const mesInicio   = parseInt(p.mes_inicio) || 1;
  const duracao     = parseInt(p.duracao_meses) || 1;
  const catSai      = p.categoria_saida || 'OUTRAS SAÍDAS';

  if (!valorMensal || !duracao) return;

  for (let i = 0; i < duracao; i++) {
    const { ano: aP, mes: mP } = _addMeses(anoBase, mesInicio - 1, i + 1);
    if (aP === anoBase) {
      _add(projecao, mP, catSai, -valorMensal);
    }
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Exports
// ─────────────────────────────────────────────────────────────────────────────

module.exports = {
  projetar,
  calcPrice,
  calcSAC,
};
