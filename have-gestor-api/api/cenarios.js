// ── Playground de Cenários — Fluxo de Caixa What-If ──────────────────────────
// Endpoint único com roteamento por ?action=...
// Todas as operações isoladas dos dados reais.

const jwt = require('jsonwebtoken');
const { getCompanyPool } = require('../lib/db');
const companies = require('../lib/companies');
const { callGroq, callGemini, callAI } = require('../lib/llm');
const { nextBizDay, consolidarAnual } = require('../lib/consolidar-caixa');
const {
  ensureTables,
  createSnapshot,
  consolidarCenario,
  aplicarRegra,
  refreshSnapshot,
  registrarHistorico,
  undo,
  redo,
  sugestoes,
} = require('../lib/cenarios-utils');
const { projetar } = require('../lib/cenarios-engine');

// ─── LLM (Groq + Gemini via lib/llm.js) ─────────────────────────────────────

async function _extractEvento(messages, cenario, planoContas, eventosAtuais) {
  const evStr = eventosAtuais.length
    ? eventosAtuais.map(e => `${e.tipo}: ${e.nome}`).join('; ')
    : 'Nenhum ainda';

  // Montar histórico recente da conversa para contexto
  const convStr = (messages || []).slice(-8)
    .map(m => `${m.role === 'user' ? 'Usuário' : 'Assistente'}: ${m.content}`)
    .join('\n');

  // Montar string hierárquica do plano de contas para o LLM
  function _buildPlanoStr(cats) {
    const byParent = {};
    for (const c of cats) {
      if (c.tipo === 'item') {
        if (!byParent[c.parent]) byParent[c.parent] = [];
        byParent[c.parent].push(c.nome);
      }
    }
    const sectionNames = cats.filter(c => c.tipo === 'section').map(c => c.nome);
    return sectionNames
      .filter(s => byParent[s])
      .map(s => `${s}: ${byParent[s].join(' | ')}`)
      .join('\n');
  }
  const planoStr = planoContas.length ? _buildPlanoStr(planoContas) : '(sem plano de contas)';

  const system = `Você é um assistente especializado em modelagem financeira what-if.
Cenário: "${cenario.nome}" — Ano ${cenario.ano}, meses ${cenario.mes_inicio}–${cenario.mes_fim}
Eventos já existentes: ${evStr}

PLANO DE CONTAS (use estes nomes EXATAMENTE para categorias — não invente novos):
${planoStr}

Sua função é extrair parâmetros de simulação financeira da conversa.
Antes de criar um evento, verifique se as informações OBRIGATÓRIAS estão presentes.
Se não estiverem, faça perguntas de clarificação em português brasileiro, de forma amigável e direta.

INFORMAÇÕES OBRIGATÓRIAS por tipo:
- emprestimo: valor e mês de captação (prazo=12, taxa=1.8%/mês, amort=price são defaults aceitáveis)
- compra_estoque: valor e mês de pagamento
- venda: valor e mês de recebimento
- dividendo: valor e mês de pagamento
- imobilizado: valor e mês de aquisição
- receita_recorrente: valor mensal, mês de início e duração
- despesa_recorrente: valor mensal, mês de início e duração
- ajuste_faturamento: percentual e categoria de receita

REGRAS DE CONVERSÃO (ao criar evento):
- "R$ 1 milhão" → 100000000 centavos | "R$ 500 mil" → 50000000 centavos | "R$ 500" → 50000 centavos
- "carência 90 dias" → carencia_meses: 3 | "60 parcelas" → prazo_meses: 60
- "1,5% ao mês" → taxa_juros_mensal: 0.015
- Data sem especificar dia → usar dia "01" | Ano SEMPRE ${cenario.ano}
- defaults obrigatórios: categoria_entrada="CAPTAÇÃO DE EMPRÉSTIMOS" | categoria_parcela="PAGAMENTO DE EMPRÉSTIMOS" | categoria_saida compra="FORNECEDORES" | categoria_saida dividendo="DIVIDENDOS" | categoria_saida imobilizado="IMOBILIZADO"
- Para despesa/receita recorrente: escolha a categoria mais próxima do plano de contas acima

TIPOS E PARAMETROS (valores monetários SEMPRE em centavos inteiros):
emprestimo: { valor_principal, tipo_amortizacao("price"|"sac"), taxa_juros_mensal, carencia_meses, prazo_meses, data_inicio(YYYY-MM-DD), categoria_entrada, categoria_parcela }
compra_estoque: { valor_total_centavos, data_pagamento(YYYY-MM-DD), parcelas(default 1), categoria_saida }
venda: { valor_total_centavos, data_recebimento(YYYY-MM-DD), parcelas(default 1), categoria_receita }
dividendo: { valor_centavos, data_pagamento(YYYY-MM-DD), categoria_saida }
imobilizado: { valor_total_centavos, data_inicio(YYYY-MM-DD), parcelas(default 1), categoria_saida }
receita_recorrente: { valor_mensal_centavos, mes_inicio(1-12), duracao_meses, categoria_receita }
despesa_recorrente: { valor_mensal_centavos, mes_inicio(1-12), duracao_meses, categoria_saida }
ajuste_faturamento: { fator(ex:1.10=+10%), meses(int[]|null), categoria_receita, manter_margem(bool), categoria_cmv }

Retorne SOMENTE JSON válido, sem markdown, sem texto extra. Escolha UMA das opções:
Se tem tudo necessário → {"acao":"criar_evento","evento":{"tipo":"...","nome":"...","data_inicio":"YYYY-MM-DD","parametros":{...}}}
Se falta info obrigatória → {"acao":"perguntar","pergunta":"Texto amigável em PT-BR perguntando exatamente o que falta, de forma conversacional. Mencione os defaults que serão usados para campos opcionais."}
Se não é pedido de evento → {"acao":null}`;

  const raw = await callAI([
    { role: 'system', content: system },
    { role: 'user', content: convStr },
  ], 700);

  try {
    const clean = raw.replace(/```json|```/g, '').trim();
    return JSON.parse(clean);
  } catch {
    console.warn('[_extractEvento] parse falhou:', raw);
    return { acao: null };
  }
}

async function _explicarImpacto(cenario, evento, antes, depois, userMsg) {
  const meses = Object.keys(depois.meses).map(Number).sort((a, b) => a - b);
  const _fmt = v => `R$ ${(v / 100).toLocaleString('pt-BR', { minimumFractionDigits: 0, maximumFractionDigits: 0 })}`;
  const rows = meses.map(m => {
    const a = antes.meses[m] || { liquido: 0, entradas: 0, saidas: 0 };
    const d = depois.meses[m] || { liquido: 0, entradas: 0, saidas: 0 };
    const delta = d.liquido - a.liquido;
    return { mes: m, antes: a.liquido, depois: d.liquido, delta };
  });
  const totalDelta = rows.reduce((s, r) => s + r.delta, 0);

  const tabela = rows.map(r =>
    `Mês ${r.mes}: ${_fmt(r.antes)} → ${_fmt(r.depois)} (${r.delta >= 0 ? '+' : ''}${_fmt(r.delta)})`
  ).join('\n');

  const system = `Você é um analista financeiro explicando impacto de um evento no fluxo de caixa de um cenário what-if.
REGRAS OBRIGATÓRIAS:
1. Use APENAS os números da tabela abaixo — NUNCA invente ou deduza valores por conta própria.
2. NÃO diga que o impacto é neutro a menos que todos os deltas sejam literalmente zero.
3. Empréstimos têm impacto POSITIVO no mês da captação (entrada de caixa) e NEGATIVO nos meses de parcelas.
4. Reportar veredicto correto: se mês 5 tem +R$500k, isso é uma MELHORA naquele mês.
5. Responda em português brasileiro, markdown, conciso, com emojis moderados.`;

  const user = `Evento: ${evento.tipo} — "${evento.nome}"
Parâmetros salvos: ${JSON.stringify(evento.parametros)}

TABELA DE IMPACTO (delta = diferença no fluxo de caixa líquido após aplicar o evento):
${tabela}

Total acumulado no período: ${totalDelta >= 0 ? '+' : ''}${_fmt(totalDelta)}

Com base EXCLUSIVAMENTE nos números acima, explique em 4–6 linhas:
- O que acontece no mês da captação (entrada de caixa)
- Quando e quanto são as parcelas mensais
- Impacto líquido total no período analisado
- Veredicto financeiro claro (melhora liquidez no curto prazo, comprime no longo prazo, etc)`;

  return callAI([
    { role: 'system', content: system },
    { role: 'user', content: user },
  ], 800);
}

async function _chatResponder(messages, cenario, eventosAtuais, projecaoAtual) {
  const meses = Object.keys(projecaoAtual.meses).map(Number).sort((a, b) => a - b);
  const resumo = meses.map(m => {
    const d = projecaoAtual.meses[m];
    return `Mês ${m}: líquido R$ ${(d.liquido / 100).toFixed(0)}`;
  }).join(', ');

  const system = `Você é um assistente financeiro especializado em cenários what-if de fluxo de caixa.
Cenário atual: "${cenario.nome}" — ${cenario.ano}
Eventos configurados: ${eventosAtuais.length ? eventosAtuais.map(e => e.nome).join(', ') : 'nenhum'}
Projeção mensal atual (líquido): ${resumo}
Responda em português brasileiro, seja conciso e útil.
Para criar eventos, oriente o usuário a descrever: tipo, valor, data e prazo.`;

  return callAI([
    { role: 'system', content: system },
    ...messages.slice(-6),
  ], 800);
}


function verifyToken(req, res) {
  const auth = (req.headers.authorization || '').split(' ')[1];
  if (!auth) { res.status(401).json({ error: 'Token nao fornecido' }); return null; }
  try { return jwt.verify(auth, (process.env.JWT_SECRET || '').trim()); }
  catch { res.status(401).json({ error: 'Token invalido' }); return null; }
}

module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  if (req.method === 'OPTIONS') return res.status(200).end();

  const payload = verifyToken(req, res);
  if (!payload) return;
  const { company, pool } = getCompanyPool(payload);

  const { action, id, aid, rid, ids } = req.query;

  try {
    await ensureTables(pool);
    // ═══════════════════════════════════════════════════════════════════════
    // POST action=criar — Cria cenário + snapshot base
    // ═══════════════════════════════════════════════════════════════════════
    if (req.method === 'POST' && action === 'criar') {
      const { nome, descricao, ano, mes_inicio, mes_fim, cenario_pai_id } = req.body;
      if (!nome || !ano || !mes_inicio || !mes_fim)
        return res.status(400).json({ error: 'Informe nome, ano, mes_inicio e mes_fim' });

      const mI = parseInt(mes_inicio), mF = parseInt(mes_fim);
      if (mI < 1 || mI > 12 || mF < 1 || mF > 12 || mI > mF)
        return res.status(400).json({ error: 'mes_inicio e mes_fim devem ser 1-12 e inicio <= fim' });

      const client = await pool.connect();
      try {
        await client.query('BEGIN');

        // If branching from parent, copy snapshot + ajustes
        if (cenario_pai_id) {
          const paiR = await client.query('SELECT * FROM cenarios WHERE id=$1 AND empresa=$2', [parseInt(cenario_pai_id), company]);
          if (!paiR.rows.length) {
            await client.query('ROLLBACK');
            return res.status(404).json({ error: 'Cenário pai não encontrado' });
          }

          const cR = await client.query(
            `INSERT INTO cenarios (empresa, nome, descricao, ano, mes_inicio, mes_fim, cenario_pai_id, criado_por)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING *`,
            [company, nome, descricao || null, parseInt(ano), mI, mF, parseInt(cenario_pai_id), payload.nome || payload.usuario || null]
          );
          const cenarioId = cR.rows[0].id;

          // Copy snapshot from parent
          await client.query(`
            INSERT INTO cenarios_snapshot_base (cenario_id, mes, categoria, dia, valor_centavos, origem, capturado_em)
            SELECT $1, mes, categoria, dia, valor_centavos, origem, capturado_em
            FROM cenarios_snapshot_base WHERE cenario_id=$2
          `, [cenarioId, parseInt(cenario_pai_id)]);

          // Copy ajustes from parent
          await client.query(`
            INSERT INTO cenarios_ajustes (cenario_id, tipo, mes, categoria, dia, valor_original_centavos, valor_novo_centavos, descricao)
            SELECT $1, tipo, mes, categoria, dia, valor_original_centavos, valor_novo_centavos, descricao
            FROM cenarios_ajustes WHERE cenario_id=$2
          `, [cenarioId, parseInt(cenario_pai_id)]);

          await client.query('COMMIT');
          return res.json({ ok: true, cenario: cR.rows[0] });
        }

        // New cenário — create record only (fast). Snapshot is taken on first Refresh.
        const cR = await client.query(
          `INSERT INTO cenarios (empresa, nome, descricao, ano, mes_inicio, mes_fim, criado_por)
           VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING *`,
          [company, nome, descricao || null, parseInt(ano), mI, mF, payload.nome || payload.usuario || null]
        );
        await client.query('COMMIT');
        return res.json({ ok: true, cenario: cR.rows[0], snapshot_ok: false, snapshot_error: 'pending' });
      } catch (e) {
        try { await client.query('ROLLBACK'); } catch (_) {}
        throw e;
      } finally {
        client.release();
      }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // GET action=listar — Lista cenários
    // ═══════════════════════════════════════════════════════════════════════
    if (req.method === 'GET' && action === 'listar') {
      const { ano, arquivado, pai_id } = req.query;
      let q = 'SELECT * FROM cenarios WHERE empresa=$1';
      const params = [company];
      let pIdx = 2;

      if (ano) { q += ` AND ano=$${pIdx++}`; params.push(parseInt(ano)); }
      if (arquivado === 'true') { q += ` AND arquivado=true`; }
      else if (arquivado === 'false' || !arquivado) { q += ` AND arquivado=false`; }
      if (pai_id) { q += ` AND cenario_pai_id=$${pIdx++}`; params.push(parseInt(pai_id)); }

      q += ' ORDER BY criado_em DESC';
      const r = await pool.query(q, params);
      return res.json({ cenarios: r.rows });
    }

    // ═══════════════════════════════════════════════════════════════════════
    // GET action=detalhe — Retorna cenário consolidado (base + ajustes)
    // ═══════════════════════════════════════════════════════════════════════
    if (req.method === 'GET' && action === 'detalhe') {
      if (!id) return res.status(400).json({ error: 'Informe id' });

      // Verify ownership
      const ownerR = await pool.query('SELECT empresa FROM cenarios WHERE id=$1', [parseInt(id)]);
      if (!ownerR.rows.length) return res.status(404).json({ error: 'Cenário não encontrado' });
      if (ownerR.rows[0].empresa !== company) return res.status(403).json({ error: 'Sem permissão' });

      const cenId = parseInt(id);
      const result = await consolidarCenario(pool, cenId);

      // Injetar impacto de cenario_eventos no grid (dia=1 de cada mês)
      const { rows: evAtivos } = await pool.query(
        `SELECT * FROM cenario_eventos WHERE cenario_id=$1 AND ativo=true ORDER BY ordem, id`,
        [cenId]
      );
      if (evAtivos.length > 0) {
        const { rows: snapAgg } = await pool.query(
          `SELECT mes, categoria, SUM(valor_centavos) AS valor_centavos
           FROM cenarios_snapshot_base WHERE cenario_id=$1
           GROUP BY mes, categoria`, [cenId]
        );
        const c = result.cenario;
        const horizonte = { ano: c.ano, mes_inicio: c.mes_inicio, mes_fim: c.mes_fim };
        const projBase  = projetar(snapAgg, [], horizonte);
        const projEvs   = projetar(snapAgg, evAtivos, horizonte);

        for (const monthData of result.months) {
          const m = monthData.mes;
          const baseCats = projBase.meses[m]?.categorias || {};
          const evCats   = projEvs.meses[m]?.categorias  || {};

          for (const [cat, val] of Object.entries(evCats)) {
            const delta = val - (baseCats[cat] || 0);
            if (delta === 0) continue;
            // Adiciona como lançamento no dia 1 em valores_previsao
            if (!monthData.valores_previsao[cat]) monthData.valores_previsao[cat] = {};
            monthData.valores_previsao[cat][1] = (monthData.valores_previsao[cat][1] || 0) + delta;
          }
        }
        result.eventos_count = evAtivos.length;

        // Adicionar categorias sintéticas para categorias criadas por eventos que não existem em caixa_categorias
        // Isso garante que apareçam no grid sob a seção correta (ANO ENTRADAS / ANO SAÍDAS)
        const catNamesExistentes = new Set(result.categorias.map(c => c.nome));
        const anoEntradasCat = result.categorias.find(c => c.nome === 'ANO ENTRADAS');
        const anoSaidasCat   = result.categorias.find(c => c.nome === 'ANO SAÍDAS');
        const catsSinteticas = new Set();

        for (const monthData of result.months) {
          for (const [cat, dias] of Object.entries(monthData.valores_previsao || {})) {
            if (catNamesExistentes.has(cat)) continue;
            catsSinteticas.add(cat);
          }
        }

        // Classificar como entrada ou saída baseado no sinal dominante no período
        let maxOrdem = Math.max(0, ...result.categorias.map(c => c.ordem || 0));
        for (const cat of catsSinteticas) {
          let totalDelta = 0;
          for (const monthData of result.months) {
            for (const [d, v] of Object.entries(monthData.valores_previsao?.[cat] || {})) {
              totalDelta += v;
            }
          }
          // Entrada positiva → filho de ANO ENTRADAS; saída negativa → filho de ANO SAÍDAS
          const parentCat = totalDelta >= 0 ? anoEntradasCat : anoSaidasCat;
          result.categorias.push({
            id: null,
            nome: cat,
            tipo: 'item',
            parent: parentCat ? parentCat.nome : null,
            ordem: ++maxOrdem,
          });
          catNamesExistentes.add(cat);
        }
      }

      return res.json(result);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // DELETE action=deletar — Soft delete (arquivar)
    // ═══════════════════════════════════════════════════════════════════════
    if (req.method === 'DELETE' && action === 'deletar') {
      if (!id) return res.status(400).json({ error: 'Informe id' });
      await pool.query('UPDATE cenarios SET arquivado=true, atualizado_em=CURRENT_TIMESTAMP WHERE id=$1 AND empresa=$2', [parseInt(id), company]);
      return res.json({ ok: true });
    }

    // ═══════════════════════════════════════════════════════════════════════
    // POST action=refresh — Refresh snapshot base (all months, may timeout)
    // ═══════════════════════════════════════════════════════════════════════
    if (req.method === 'POST' && action === 'refresh') {
      if (!id) return res.status(400).json({ error: 'Informe id' });

      const ownerR = await pool.query('SELECT empresa FROM cenarios WHERE id=$1', [parseInt(id)]);
      if (!ownerR.rows.length) return res.status(404).json({ error: 'Cenário não encontrado' });
      if (ownerR.rows[0].empresa !== company) return res.status(403).json({ error: 'Sem permissão' });

      const result = await refreshSnapshot(pool, parseInt(id));
      return res.json(result);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // POST action=refresh_mes — Refresh snapshot for ONE month only (fast)
    // Uses consolidarAnual (same as Fluxo Diário) then extracts target month
    // ═══════════════════════════════════════════════════════════════════════
    if (req.method === 'POST' && action === 'refresh_mes') {
      if (!id) return res.status(400).json({ error: 'Informe id' });
      const mes = parseInt(req.body?.mes || req.query.mes);
      if (!mes || mes < 1 || mes > 12) return res.status(400).json({ error: 'Informe mes (1-12)' });

      const cenR = await pool.query('SELECT * FROM cenarios WHERE id=$1 AND empresa=$2', [parseInt(id), company]);
      if (!cenR.rows.length) return res.status(404).json({ error: 'Cenário não encontrado' });
      const cenario = cenR.rows[0];

      // Use consolidarAnual (same engine as Fluxo Diário) — extract target month
      const anualData = await consolidarAnual(pool, company, cenario.ano, { apenas_futuros: false });
      const monthData = (anualData.months || []).find(m => m.mes === mes) || { valores: {}, valores_previsao: {} };

      // Build rows to insert
      const today = new Date().toISOString().slice(0, 10);
      const rows = [];
      for (const [cat, dias] of Object.entries(monthData.valores || {})) {
        for (const [dia, val] of Object.entries(dias)) {
          if (val !== 0) rows.push([cenario.id, mes, cat, parseInt(dia), parseInt(val), 'realizado']);
        }
      }
      // Previsão: combina valores_previsao + valores_previsao_pc (pedidos de compra)
      const previsaoMerged = {};
      for (const bucket of [monthData.valores_previsao || {}, monthData.valores_previsao_pc || {}]) {
        for (const [cat, dias] of Object.entries(bucket)) {
          for (const [dia, val] of Object.entries(dias)) {
            if (!val) continue;
            if (!previsaoMerged[cat]) previsaoMerged[cat] = {};
            previsaoMerged[cat][dia] = (previsaoMerged[cat][dia] || 0) + parseInt(val);
          }
        }
      }
      for (const [cat, dias] of Object.entries(previsaoMerged)) {
        for (const [dia, val] of Object.entries(dias)) {
          if (!val) continue;
          const dStr = `${cenario.ano}-${String(mes).padStart(2,'0')}-${String(parseInt(dia)).padStart(2,'0')}`;
          if (dStr < today) continue; // extrato já cobre datas passadas
          rows.push([cenario.id, mes, cat, parseInt(dia), val, 'previsao']);
        }
      }

      const client = await pool.connect();
      try {
        await client.query('BEGIN');
        await client.query('DELETE FROM cenarios_snapshot_base WHERE cenario_id=$1 AND mes=$2', [cenario.id, mes]);
        if (rows.length > 0) {
          const CHUNK = 200;
          for (let i = 0; i < rows.length; i += CHUNK) {
            const chunk = rows.slice(i, i + CHUNK);
            const vals = [], params = [];
            chunk.forEach((r, idx) => {
              const b = idx * 6;
              vals.push(`($${b+1},$${b+2},$${b+3},$${b+4},$${b+5},$${b+6})`);
              params.push(...r);
            });
            await client.query(
              `INSERT INTO cenarios_snapshot_base (cenario_id, mes, categoria, dia, valor_centavos, origem) VALUES ${vals.join(',')}`,
              params
            );
          }
        }
        await client.query('COMMIT');
        return res.json({ ok: true, mes, rows_inserted: rows.length });
      } catch (e) {
        await client.query('ROLLBACK');
        throw e;
      } finally {
        client.release();
      }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // POST action=ajuste — Cria ajuste célula
    // ═══════════════════════════════════════════════════════════════════════
    if (req.method === 'POST' && action === 'ajuste') {
      if (!id) return res.status(400).json({ error: 'Informe id' });
      const { tipo, mes, categoria, dia, valor_novo_centavos, descricao } = req.body;

      if (!mes || !categoria || !dia || valor_novo_centavos == null)
        return res.status(400).json({ error: 'Informe mes, categoria, dia e valor_novo_centavos' });

      const ajTipo = tipo === 'lancamento_novo' ? 'lancamento_novo' : 'override';

      const client = await pool.connect();
      try {
        await client.query('BEGIN');

        // Get original value from snapshot (for override)
        let valorOriginal = null;
        if (ajTipo === 'override') {
          const snapR = await client.query(
            'SELECT valor_centavos FROM cenarios_snapshot_base WHERE cenario_id=$1 AND mes=$2 AND categoria=$3 AND dia=$4 LIMIT 1',
            [parseInt(id), parseInt(mes), categoria, parseInt(dia)]
          );
          valorOriginal = snapR.rows.length ? parseInt(snapR.rows[0].valor_centavos) : 0;
        }

        // For override, remove existing override on same cell (last one wins)
        if (ajTipo === 'override') {
          await client.query(
            `DELETE FROM cenarios_ajustes WHERE cenario_id=$1 AND tipo='override' AND mes=$2 AND categoria=$3 AND dia=$4 AND regra_id IS NULL`,
            [parseInt(id), parseInt(mes), categoria, parseInt(dia)]
          );
        }

        const ajR = await client.query(
          `INSERT INTO cenarios_ajustes (cenario_id, tipo, mes, categoria, dia, valor_original_centavos, valor_novo_centavos, descricao)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING *`,
          [parseInt(id), ajTipo, parseInt(mes), categoria, parseInt(dia), valorOriginal, parseInt(valor_novo_centavos), descricao || null]
        );

        // Record in history
        await registrarHistorico(client, parseInt(id), 'criar_ajuste', {
          ajuste: ajR.rows[0],
        }, {
          ajuste_id: ajR.rows[0].id,
        });

        // Clear redo stack (any undone events)
        await client.query(
          'DELETE FROM cenarios_historico WHERE cenario_id=$1 AND desfeito_em IS NOT NULL',
          [parseInt(id)]
        );

        await client.query('UPDATE cenarios SET atualizado_em=CURRENT_TIMESTAMP WHERE id=$1', [parseInt(id)]);
        await client.query('COMMIT');

        return res.json({ ok: true, ajuste: ajR.rows[0] });
      } catch (e) {
        await client.query('ROLLBACK');
        throw e;
      } finally {
        client.release();
      }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // DELETE action=ajuste — Remove ajuste
    // ═══════════════════════════════════════════════════════════════════════
    if (req.method === 'DELETE' && action === 'ajuste') {
      if (!id || !aid) return res.status(400).json({ error: 'Informe id e aid' });

      const client = await pool.connect();
      try {
        await client.query('BEGIN');

        // Get the ajuste before deleting (for undo)
        const ajR = await client.query(
          'SELECT * FROM cenarios_ajustes WHERE id=$1 AND cenario_id=$2',
          [parseInt(aid), parseInt(id)]
        );
        if (!ajR.rows.length) {
          await client.query('ROLLBACK');
          return res.status(404).json({ error: 'Ajuste não encontrado' });
        }

        await client.query('DELETE FROM cenarios_ajustes WHERE id=$1 AND cenario_id=$2', [parseInt(aid), parseInt(id)]);

        await registrarHistorico(client, parseInt(id), 'remover_ajuste', {
          ajuste_id: parseInt(aid),
        }, {
          ajuste: ajR.rows[0],
        });

        await client.query(
          'DELETE FROM cenarios_historico WHERE cenario_id=$1 AND desfeito_em IS NOT NULL',
          [parseInt(id)]
        );

        await client.query('UPDATE cenarios SET atualizado_em=CURRENT_TIMESTAMP WHERE id=$1', [parseInt(id)]);
        await client.query('COMMIT');

        return res.json({ ok: true });
      } catch (e) {
        await client.query('ROLLBACK');
        throw e;
      } finally {
        client.release();
      }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // POST action=regra — Cria regra + expande ajustes
    // ═══════════════════════════════════════════════════════════════════════
    if (req.method === 'POST' && action === 'regra') {
      if (!id) return res.status(400).json({ error: 'Informe id' });
      const { nome, tipo, parametro, escopo_json } = req.body;

      if (!nome || !tipo || parametro == null)
        return res.status(400).json({ error: 'Informe nome, tipo e parametro' });

      if (!['percentual', 'valor_fixo', 'substituicao'].includes(tipo))
        return res.status(400).json({ error: 'tipo deve ser percentual, valor_fixo ou substituicao' });

      const client = await pool.connect();
      try {
        await client.query('BEGIN');

        const result = await aplicarRegra(pool, client, parseInt(id), {
          nome, tipo, parametro: parseFloat(parametro), escopo_json: escopo_json || {},
        });

        await client.query(
          'DELETE FROM cenarios_historico WHERE cenario_id=$1 AND desfeito_em IS NOT NULL',
          [parseInt(id)]
        );

        await client.query('UPDATE cenarios SET atualizado_em=CURRENT_TIMESTAMP WHERE id=$1', [parseInt(id)]);
        await client.query('COMMIT');

        return res.json({ ok: true, ...result });
      } catch (e) {
        await client.query('ROLLBACK');
        throw e;
      } finally {
        client.release();
      }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // DELETE action=regra — Reverte regra (remove ajustes filhos)
    // ═══════════════════════════════════════════════════════════════════════
    if (req.method === 'DELETE' && action === 'regra') {
      if (!id || !rid) return res.status(400).json({ error: 'Informe id e rid' });

      const client = await pool.connect();
      try {
        await client.query('BEGIN');

        // Get rule + its ajustes for undo
        const rR = await client.query(
          'SELECT * FROM cenarios_regras WHERE id=$1 AND cenario_id=$2',
          [parseInt(rid), parseInt(id)]
        );
        if (!rR.rows.length) {
          await client.query('ROLLBACK');
          return res.status(404).json({ error: 'Regra não encontrada' });
        }
        const regra = rR.rows[0];

        const ajR = await client.query(
          'SELECT mes, categoria, dia, valor_original_centavos AS valor_original, valor_novo_centavos AS valor_novo FROM cenarios_ajustes WHERE regra_id=$1 AND cenario_id=$2',
          [parseInt(rid), parseInt(id)]
        );

        // Delete ajustes and rule
        await client.query('DELETE FROM cenarios_ajustes WHERE regra_id=$1 AND cenario_id=$2', [parseInt(rid), parseInt(id)]);
        await client.query('DELETE FROM cenarios_regras WHERE id=$1 AND cenario_id=$2', [parseInt(rid), parseInt(id)]);

        await registrarHistorico(client, parseInt(id), 'reverter_regra', {
          regra_id: parseInt(rid),
        }, {
          regra: {
            nome: regra.nome, tipo: regra.tipo, parametro: regra.parametro,
            escopo_json: regra.escopo_json,
          },
          ajustes: ajR.rows,
        });

        await client.query(
          'DELETE FROM cenarios_historico WHERE cenario_id=$1 AND desfeito_em IS NOT NULL',
          [parseInt(id)]
        );

        await client.query('UPDATE cenarios SET atualizado_em=CURRENT_TIMESTAMP WHERE id=$1', [parseInt(id)]);
        await client.query('COMMIT');

        return res.json({ ok: true, ajustes_removidos: ajR.rowCount });
      } catch (e) {
        await client.query('ROLLBACK');
        throw e;
      } finally {
        client.release();
      }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // POST action=undo / action=redo
    // ═══════════════════════════════════════════════════════════════════════
    if (req.method === 'POST' && action === 'undo') {
      if (!id) return res.status(400).json({ error: 'Informe id' });
      const result = await undo(pool, parseInt(id));
      return res.json(result);
    }

    if (req.method === 'POST' && action === 'redo') {
      if (!id) return res.status(400).json({ error: 'Informe id' });
      const result = await redo(pool, parseInt(id));
      return res.json(result);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // GET action=historico — Lista eventos
    // ═══════════════════════════════════════════════════════════════════════
    if (req.method === 'GET' && action === 'historico') {
      if (!id) return res.status(400).json({ error: 'Informe id' });
      const r = await pool.query(
        'SELECT id, operacao, payload_json, criado_em, desfeito_em FROM cenarios_historico WHERE cenario_id=$1 ORDER BY criado_em DESC LIMIT 100',
        [parseInt(id)]
      );
      return res.json({ historico: r.rows });
    }

    // ═══════════════════════════════════════════════════════════════════════
    // GET action=compare — Diff N cenários
    // ═══════════════════════════════════════════════════════════════════════
    if (req.method === 'GET' && action === 'compare') {
      if (!ids) return res.status(400).json({ error: 'Informe ids (comma-separated)' });
      const idArr = ids.split(',').map(i => parseInt(i.trim())).filter(i => i > 0);
      if (idArr.length < 2) return res.status(400).json({ error: 'Informe pelo menos 2 ids' });
      if (idArr.length > 5) return res.status(400).json({ error: 'Máximo 5 cenários para comparação' });

      // Verify all belong to this company
      const verR = await pool.query(
        'SELECT id FROM cenarios WHERE empresa=$1 AND id = ANY($2)',
        [company, idArr]
      );
      if (verR.rows.length !== idArr.length) {
        return res.status(403).json({ error: 'Nem todos os cenários pertencem a esta empresa' });
      }

      const cenarios = [];
      for (const cId of idArr) {
        const result = await consolidarCenario(pool, cId);
        cenarios.push(result);
      }

      // Build diff: for each (mes, categoria, dia), flag divergences
      const divergencias = [];
      if (cenarios.length >= 2) {
        const ref = cenarios[0];
        for (const month of ref.months) {
          const m = month.mes;
          for (const bucket of ['valores', 'valores_previsao']) {
            for (const [cat, dias] of Object.entries(month[bucket] || {})) {
              for (const [dia, val] of Object.entries(dias)) {
                const valores = [val];
                for (let c = 1; c < cenarios.length; c++) {
                  const cm = cenarios[c].months.find(mm => mm.mes === m);
                  const cv = cm && cm[bucket] && cm[bucket][cat] && cm[bucket][cat][dia];
                  valores.push(cv || 0);
                }
                const allSame = valores.every(v => v === valores[0]);
                if (!allSame) {
                  divergencias.push({
                    mes: m, categoria: cat, dia: parseInt(dia), bucket,
                    valores: idArr.map((cid, idx) => ({ cenario_id: cid, valor: valores[idx] })),
                    delta_abs: Math.max(...valores) - Math.min(...valores),
                    delta_pct: valores[0] !== 0 ? Math.round(((valores[1] - valores[0]) / Math.abs(valores[0])) * 10000) / 100 : null,
                  });
                }
              }
            }
          }
        }
      }

      return res.json({ cenarios, divergencias });
    }

    // ═══════════════════════════════════════════════════════════════════════
    // GET action=sugestoes — Contexto histórico para uma célula
    // ═══════════════════════════════════════════════════════════════════════
    if (req.method === 'GET' && action === 'sugestoes') {
      const { categoria, mes, dia } = req.query;
      if (!categoria || !mes || !dia)
        return res.status(400).json({ error: 'Informe categoria, mes e dia' });

      const result = await sugestoes(pool, company, categoria, parseInt(mes), parseInt(dia));
      return res.json(result);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // GET action=projetar — motor de projeção com eventos
    // ═══════════════════════════════════════════════════════════════════════
    if (req.method === 'GET' && action === 'projetar') {
      const id = parseInt(req.query.id);
      if (!id) return res.status(400).json({ error: 'id required' });

      const { rows: [cenario] } = await pool.query(
        'SELECT * FROM cenarios WHERE id=$1 AND empresa=$2', [id, company]
      );
      if (!cenario) return res.status(404).json({ error: 'Cenário não encontrado' });

      // Baseline: snapshot aggregado por mes+categoria
      const { rows: snapshot } = await pool.query(
        `SELECT mes, categoria, SUM(valor_centavos) AS valor_centavos
         FROM cenarios_snapshot_base
         WHERE cenario_id=$1
         GROUP BY mes, categoria`, [id]
      );

      // Eventos do cenário
      const { rows: eventos } = await pool.query(
        `SELECT * FROM cenario_eventos
         WHERE cenario_id=$1
         ORDER BY ordem, id`, [id]
      );

      const horizonte = {
        ano: cenario.ano,
        mes_inicio: cenario.mes_inicio,
        mes_fim: cenario.mes_fim,
      };

      const projecao = projetar(snapshot, eventos, horizonte);
      return res.json({ ok: true, projecao, cenario, eventos });
    }

    // ═══════════════════════════════════════════════════════════════════════
    // GET action=eventos — listar eventos de um cenário
    // ═══════════════════════════════════════════════════════════════════════
    if (req.method === 'GET' && action === 'eventos') {
      const id = parseInt(req.query.id);
      if (!id) return res.status(400).json({ error: 'id required' });

      const { rows: eventos } = await pool.query(
        `SELECT * FROM cenario_eventos
         WHERE cenario_id=$1
         ORDER BY ordem, id`, [id]
      );
      return res.json({ ok: true, eventos });
    }

    // ═══════════════════════════════════════════════════════════════════════
    // POST action=evento_criar — criar evento
    // ═══════════════════════════════════════════════════════════════════════
    if (req.method === 'POST' && action === 'evento_criar') {
      const id = parseInt(req.query.id);
      if (!id) return res.status(400).json({ error: 'id required' });

      const { tipo, nome, data_inicio, data_fim, parametros, ordem } = req.body || {};
      if (!tipo || !nome || !data_inicio)
        return res.status(400).json({ error: 'tipo, nome e data_inicio são obrigatórios' });

      const tiposValidos = ['emprestimo', 'compra_estoque', 'ajuste_faturamento'];
      if (!tiposValidos.includes(tipo))
        return res.status(400).json({ error: `tipo inválido. Use: ${tiposValidos.join(', ')}` });

      const { rows: [ev] } = await pool.query(
        `INSERT INTO cenario_eventos
           (cenario_id, tipo, nome, data_inicio, data_fim, parametros, ordem)
         VALUES ($1,$2,$3,$4,$5,$6,$7)
         RETURNING *`,
        [id, tipo, nome, data_inicio, data_fim || null,
         JSON.stringify(parametros || {}), ordem || 0]
      );
      return res.json({ ok: true, evento: ev });
    }

    // ═══════════════════════════════════════════════════════════════════════
    // PUT action=evento_atualizar — atualizar evento
    // ═══════════════════════════════════════════════════════════════════════
    if (req.method === 'PUT' && action === 'evento_atualizar') {
      const cenId = parseInt(req.query.id);
      const evId  = parseInt(req.query.eid);
      if (!cenId || !evId) return res.status(400).json({ error: 'id e eid required' });

      const { nome, data_inicio, data_fim, parametros, ordem, ativo } = req.body || {};

      const sets = [];
      const vals = [];
      let idx = 1;

      if (nome        !== undefined) { sets.push(`nome=$${idx++}`);        vals.push(nome); }
      if (data_inicio !== undefined) { sets.push(`data_inicio=$${idx++}`); vals.push(data_inicio); }
      if (data_fim    !== undefined) { sets.push(`data_fim=$${idx++}`);    vals.push(data_fim); }
      if (parametros  !== undefined) { sets.push(`parametros=$${idx++}`);  vals.push(JSON.stringify(parametros)); }
      if (ordem       !== undefined) { sets.push(`ordem=$${idx++}`);       vals.push(ordem); }
      if (ativo       !== undefined) { sets.push(`ativo=$${idx++}`);       vals.push(ativo); }

      if (!sets.length) return res.status(400).json({ error: 'Nenhum campo para atualizar' });

      sets.push(`atualizado_em=NOW()`);
      vals.push(evId, cenId);

      const { rows: [ev] } = await pool.query(
        `UPDATE cenario_eventos SET ${sets.join(', ')}
         WHERE id=$${idx} AND cenario_id=$${idx + 1}
         RETURNING *`,
        vals
      );
      if (!ev) return res.status(404).json({ error: 'Evento não encontrado' });
      return res.json({ ok: true, evento: ev });
    }

    // ═══════════════════════════════════════════════════════════════════════
    // DELETE action=evento_deletar — remover evento
    // ═══════════════════════════════════════════════════════════════════════
    if (req.method === 'DELETE' && action === 'evento_deletar') {
      const cenId = parseInt(req.query.id);
      const evId  = parseInt(req.query.eid);
      if (!cenId || !evId) return res.status(400).json({ error: 'id e eid required' });

      await pool.query(
        'DELETE FROM cenario_eventos WHERE id=$1 AND cenario_id=$2', [evId, cenId]
      );
      return res.json({ ok: true });
    }

    // ═══════════════════════════════════════════════════════════════════════
    // POST action=chat — agente IA "e se..." para criação de eventos
    // ═══════════════════════════════════════════════════════════════════════
    if (req.method === 'POST' && action === 'chat') {
      const cenId = parseInt(req.query.id);
      const { messages } = req.body || {};
      if (!cenId || !Array.isArray(messages) || !messages.length)
        return res.status(400).json({ error: 'id e messages são obrigatórios' });

      const lastUserMsg = [...messages].reverse().find(m => m.role === 'user')?.content || '';

      const { rows: [cenario] } = await pool.query(
        'SELECT * FROM cenarios WHERE id=$1 AND empresa=$2', [cenId, company]
      );
      if (!cenario) return res.status(404).json({ error: 'Cenário não encontrado' });

      const { rows: snapshot } = await pool.query(
        `SELECT mes, categoria, SUM(valor_centavos) AS valor_centavos
         FROM cenarios_snapshot_base WHERE cenario_id=$1
         GROUP BY mes, categoria`, [cenId]
      );
      const { rows: eventosAtuais } = await pool.query(
        `SELECT * FROM cenario_eventos WHERE cenario_id=$1 ORDER BY ordem, id`, [cenId]
      );

      const horizonte = { ano: cenario.ano, mes_inicio: cenario.mes_inicio, mes_fim: cenario.mes_fim };
      const projecaoAntes = projetar(snapshot, eventosAtuais, horizonte);

      // Buscar plano de contas real para o LLM escolher categorias corretas
      const { rows: planoContas } = await pool.query(
        'SELECT nome, tipo, parent FROM caixa_categorias WHERE empresa=$1 ORDER BY ordem',
        [company]
      );

      // Step 1 — tentar extrair evento da mensagem (passa histórico + plano de contas)
      const extracted = await _extractEvento(messages, cenario, planoContas, eventosAtuais);

      // Se o LLM pediu mais informações, retorna a pergunta diretamente
      if (extracted.acao === 'perguntar' && extracted.pergunta) {
        return res.json({ reply: extracted.pergunta });
      }

      if (extracted.acao === 'criar_evento' && extracted.evento) {
        const ev = extracted.evento;

        // Normalização: se valor parece estar em reais (< 1000 centavos = R$10)
        // provavelmente o LLM esqueceu de converter — multiplica por 100
        if (ev.tipo === 'emprestimo' && ev.parametros?.valor_principal) {
          const vp = Number(ev.parametros.valor_principal);
          if (vp > 0 && vp < 1000) { // menos de R$10 em centavos → claramente em reais
            ev.parametros.valor_principal = Math.round(vp * 100);
          }
        }
        const _camposValor = [
          ['compra_estoque',    'valor_total_centavos'],
          ['venda',             'valor_total_centavos'],
          ['dividendo',         'valor_centavos'],
          ['imobilizado',       'valor_total_centavos'],
          ['receita_recorrente','valor_mensal_centavos'],
          ['despesa_recorrente','valor_mensal_centavos'],
        ];
        for (const [tipo, campo] of _camposValor) {
          if (ev.tipo === tipo && ev.parametros?.[campo]) {
            const v = Number(ev.parametros[campo]);
            if (v > 0 && v < 1000) ev.parametros[campo] = Math.round(v * 100);
          }
        }

        // Garante que data_inicio usa o ano do cenário — tanto no top-level quanto em parametros
        const _fixYear = (dateStr) => {
          if (!dateStr) return dateStr;
          const s = String(dateStr);
          if (s.startsWith(String(cenario.ano))) return s;
          const parts = s.split('-');
          return `${cenario.ano}-${parts[1] || '01'}-${parts[2] || '01'}`;
        };
        if (ev.data_inicio) ev.data_inicio = _fixYear(ev.data_inicio);
        // Campos de data dentro de parametros que devem respeitar o ano do cenário
        const _camposData = ['data_inicio','data_pagamento','data_recebimento'];
        for (const campo of _camposData) {
          if (ev.parametros?.[campo]) {
            ev.parametros[campo] = _fixYear(ev.parametros[campo]);
          }
        }

        // CRÍTICO: propagar ev.data_inicio para o campo de data correto dentro de parametros
        // O LLM frequentemente coloca a data só no top-level; o engine lê de parametros.
        if (ev.data_inicio && ev.parametros) {
          const _dateFieldByTipo = {
            emprestimo:        'data_inicio',
            imobilizado:       'data_inicio',
            compra_estoque:    'data_pagamento',
            dividendo:         'data_pagamento',
            venda:             'data_recebimento',
          };
          const campoData = _dateFieldByTipo[ev.tipo];
          if (campoData && !ev.parametros[campoData]) {
            ev.parametros[campoData] = ev.data_inicio;
          }
        }

        console.log('[chat] ev extraído:', JSON.stringify({ tipo: ev.tipo, data_inicio: ev.data_inicio, parametros: ev.parametros }));
        console.log('[chat] cenario.ano:', cenario.ano, 'horizonte:', horizonte);

        const { rows: [eventoNovo] } = await pool.query(
          `INSERT INTO cenario_eventos
             (cenario_id, tipo, nome, data_inicio, data_fim, parametros, ordem)
           VALUES ($1,$2,$3,$4,$5,$6,$7)
           RETURNING *`,
          [cenId, ev.tipo, ev.nome || 'Evento criado pela IA',
           ev.data_inicio, ev.data_fim || null,
           JSON.stringify(ev.parametros || {}), 0]
        );

        const projecaoDepois = projetar(snapshot, [...eventosAtuais, eventoNovo], horizonte);
        console.log('[chat] projecaoAntes.meses:', JSON.stringify(projecaoAntes.meses));
        console.log('[chat] projecaoDepois.meses:', JSON.stringify(projecaoDepois.meses));

        // Step 2 — explicar impacto
        const reply = await _explicarImpacto(cenario, eventoNovo, projecaoAntes, projecaoDepois, lastUserMsg);

        return res.json({ reply, evento_criado: eventoNovo, projecao: projecaoDepois });
      }

      // Sem evento — resposta de chat normal
      const reply = await _chatResponder(messages, cenario, eventosAtuais, projecaoAntes);
      return res.json({ reply });
    }

    // ═══════════════════════════════════════════════════════════════════════
    // POST action=evento_toggle — ligar/desligar evento
    // ═══════════════════════════════════════════════════════════════════════
    if (req.method === 'POST' && action === 'evento_toggle') {
      const cenId = parseInt(req.query.id);
      const evId  = parseInt(req.query.eid);
      if (!cenId || !evId) return res.status(400).json({ error: 'id e eid required' });

      const { rows: [ev] } = await pool.query(
        `UPDATE cenario_eventos SET ativo = NOT ativo, atualizado_em=NOW()
         WHERE id=$1 AND cenario_id=$2
         RETURNING *`,
        [evId, cenId]
      );
      if (!ev) return res.status(404).json({ error: 'Evento não encontrado' });
      return res.json({ ok: true, evento: ev });
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Fallback
    // ═══════════════════════════════════════════════════════════════════════
    return res.status(400).json({ error: 'action inválida ou method não suportado' });

  } catch (e) {
    console.error('[CENARIOS]', e.message, e.stack);
    if (!res.headersSent) res.status(500).json({ error: e.message });
  }
};
