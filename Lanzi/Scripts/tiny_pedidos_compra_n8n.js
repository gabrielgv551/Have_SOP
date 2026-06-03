// ============================================================
//  OLIST TINY — Pedidos de Compra em Aberto (n8n Code Node)
// ============================================================

const TOKEN        = "adad5861e5d6cc4e25f4e0d6e2d17eafd87e7c90a2a535d3690a885761fd644e";
const BASE_URL     = "https://api.tiny.com.br/api2";
const CONCORRENCIA = 5;

// ─── Helpers ─────────────────────────────────────────────────

function parseNum(v) {
  if (v == null || v === "") return 0;
  return parseFloat(String(v).replace(",", ".")) || 0;
}

function parseDate(v) {
  if (!v) return null;
  // Tiny retorna datas em formato dd/MM/yyyy ou yyyy-MM-dd
  const d = String(v).trim();
  if (d.match(/^\d{2}\/\d{2}\/\d{4}$/)) {
    const [dd, mm, yyyy] = d.split("/");
    return `${yyyy}-${mm}-${dd}`;
  }
  if (d.match(/^\d{4}-\d{2}-\d{2}$/)) return d;
  return null;
}

async function tinyPost(endpoint, params) {
  const allParams = { token: TOKEN, formato: "JSON", ...params };
  const body = Object.entries(allParams)
    .map(([k, v]) => encodeURIComponent(k) + "=" + encodeURIComponent(v))
    .join("&");

  const resp = await this.helpers.httpRequest({
    method : "POST",
    url    : `${BASE_URL}/${endpoint}`,
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body,
  });
  return resp;
}

function parsarResposta(resp) {
  if (typeof resp === "string") {
    try { return JSON.parse(resp); } catch(e) { return {}; }
  }
  return resp || {};
}

async function emLotes(items, tamanho, fn) {
  const resultados = [];
  for (let i = 0; i < items.length; i += tamanho) {
    const lote = items.slice(i, i + tamanho);
    const res  = await Promise.all(lote.map(fn));
    resultados.push(...res);
  }
  return resultados;
}

// ─── 1. Coleta todos os pedidos de compra (paginado) ───────

const todosPedidos = [];
let pagina  = 1;
let temMais = true;

while (temMais) {
  let resp;
  try {
    resp = await tinyPost.call(this, "pedidos.compra.pesquisa.php", { pagina });
  } catch (err) {
    break;
  }

  let dados = parsarResposta(resp);
  const retorno  = dados?.retorno || dados || {};
  const status   = retorno.status;
  const pedidos  = retorno.pedidos || [];

  if (status !== "OK" || pedidos.length === 0) {
    temMais = false;
  } else {
    for (const item of pedidos) {
      const p = item.pedido_compra || item;
      if (p.id) {
        todosPedidos.push({
          id          : String(p.id),
          numero      : p.numero           || "",
          fornecedor  : p.fornecedor?.nome || p.fornecedor || "",
          data        : parseDate(p.data),
          data_prevista: parseDate(p.data_prevista),
          situacao    : (p.situacao || "").trim(),
          valor_total : parseNum(p.valor_total),
        });
      }
    }
    temMais = pedidos.length >= 100;
    pagina++;
  }
}

// ─── 2. Filtra apenas pedidos em aberto ─────────────────────
// Situações consideradas "em aberto": Aberto, Em andamento, Parcial
// Situações ignoradas: Finalizado, Cancelado, Concluído

const SITUACOES_FECHADAS = new Set(["finalizado", "cancelado", "concluido", "concluído"]);

const pedidosEmAberto = todosPedidos.filter(p => {
  const sit = p.situacao.toLowerCase().normalize("NFD").replace(/[\u0300-\u036f]/g, "");
  return !SITUACOES_FECHADAS.has(sit);
});

// ─── 3. Busca detalhes de cada pedido em aberto ──────────────

const detalhes = await emLotes(pedidosEmAberto, CONCORRENCIA, async (pedido) => {
  try {
    const r = parsarResposta(await tinyPost.call(this, "pedido.compra.obter.php", { id: pedido.id }));
    const ret = r?.retorno || r || {};

    if (ret.status !== "OK") {
      return { ...pedido, itens: [], _dbg_fonte: "obter_falhou", _dbg_erro: JSON.stringify(ret.erros || ret.codigo_erro || "") };
    }

    const p = ret.pedido_compra || ret.pedido || {};

    // Extrai itens do pedido
    const rawItens = Array.isArray(p.itens) ? p.itens : [];
    const itens = rawItens.map(entry => {
      const item = entry.item || entry;
      return {
        sku          : (item.codigo || "").trim(),
        descricao    : item.descricao  || "",
        quantidade   : parseNum(item.quantidade),
        valor_unitario: parseNum(item.valor_unitario),
        valor_total  : parseNum(item.valor_total),
      };
    }).filter(i => i.sku && i.sku !== "0");

    return {
      ...pedido,
      fornecedor   : p.fornecedor?.nome || p.fornecedor || pedido.fornecedor,
      data         : parseDate(p.data) || pedido.data,
      data_prevista: parseDate(p.data_prevista) || pedido.data_prevista,
      observacoes  : p.observacoes || "",
      itens,
      _dbg_fonte   : "obter",
      _dbg_erro    : "",
    };

  } catch (err) {
    return { ...pedido, itens: [], _dbg_fonte: "exception", _dbg_erro: String(err) };
  }
});

// ─── 4. Monta linhas de saída ────────────────────────────────

const linhas = [];
const agora = new Date().toISOString();

for (const pedido of detalhes) {
  // Se o pedido tiver itens, explode 1 linha por item
  if (pedido.itens && pedido.itens.length > 0) {
    for (const item of pedido.itens) {
      linhas.push({ json: {
        pedido_id       : pedido.id,
        pedido_numero   : pedido.numero,
        fornecedor      : pedido.fornecedor,
        situacao        : pedido.situacao,
        data_pedido     : pedido.data,
        data_prevista   : pedido.data_prevista,
        valor_total     : pedido.valor_total,
        sku             : item.sku,
        nome_produto    : item.descricao,
        quantidade      : item.quantidade,
        valor_unitario  : item.valor_unitario,
        valor_item      : item.valor_total,
        observacoes     : pedido.observacoes,
        atualizado_em   : agora,
        _dbg_fonte      : pedido._dbg_fonte,
        _dbg_erro       : pedido._dbg_erro,
      }});
    }
  } else {
    // Pedido sem itens → 1 linha mesmo assim
    linhas.push({ json: {
      pedido_id       : pedido.id,
      pedido_numero   : pedido.numero,
      fornecedor      : pedido.fornecedor,
      situacao        : pedido.situacao,
      data_pedido     : pedido.data,
      data_prevista   : pedido.data_prevista,
      valor_total     : pedido.valor_total,
      sku             : null,
      nome_produto    : null,
      quantidade      : null,
      valor_unitario  : null,
      valor_item      : null,
      observacoes     : pedido.observacoes,
      atualizado_em   : agora,
      _dbg_fonte      : pedido._dbg_fonte,
      _dbg_erro       : pedido._dbg_erro,
    }});
  }
}

return linhas.length > 0
  ? linhas
  : [{ json: { erro: "Nenhum pedido de compra em aberto encontrado", total_pesquisados: todosPedidos.length, em_aberto: pedidosEmAberto.length } }];
