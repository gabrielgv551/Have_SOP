// ============================================================
//  OLIST TINY — Estoque por Depósito (n8n Code Node)
// ============================================================

const TOKEN        = "adad5861e5d6cc4e25f4e0d6e2d17eafd87e7c90a2a535d3690a885761fd644e";
const CONCORRENCIA = 5;
const BASE_URL     = "https://api.tiny.com.br/api2";

// ─── Helpers ─────────────────────────────────────────────────

// Tiny API v2 retorna números no formato BR: "220,00" → parseFloat falha.
// parseNum normaliza vírgula → ponto antes de converter.
function parseNum(v) {
  if (v == null || v === "") return 0;
  return parseFloat(String(v).replace(",", ".")) || 0;
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

async function emLotes(items, tamanho, fn) {
  const resultados = [];
  for (let i = 0; i < items.length; i += tamanho) {
    const lote = items.slice(i, i + tamanho);
    const res  = await Promise.all(lote.map(fn));
    resultados.push(...res);
  }
  return resultados;
}

function parsarResposta(resp) {
  if (typeof resp === "string") {
    try { return JSON.parse(resp); } catch(e) { return {}; }
  }
  return resp || {};
}

// ─── 1. Coleta todos os produtos (paginado) ──────────────────

const todosProdutos = [];
let pagina  = 1;
let temMais = true;

while (temMais) {
  let resp;
  try {
    resp = await tinyPost.call(this, "produtos.pesquisa.php", { pagina });
  } catch (err) {
    break;
  }

  let dados = resp;
  if (typeof resp === "string") {
    try { dados = JSON.parse(resp); } catch(e) { dados = {}; }
  }

  const retorno  = dados?.retorno || dados || {};
  const status   = retorno.status;
  const produtos = retorno.produtos || [];

  if (status !== "OK" || produtos.length === 0) {
    temMais = false;
  } else {
    for (const item of produtos) {
      const p = item.produto || item;
      const sku1 = (p.codigo || "").trim();
      if (p.id && sku1 && sku1 !== "0") {  // ignora produtos sem SKU ou com codigo="0"
        todosProdutos.push({
          id      : String(p.id),
          sku     : sku1,
          nome    : p.nome     || "",
          preco   : parseNum(p.preco),
          unidade : p.unidade  || "",
          situacao: p.situacao || "",
          tipo    : p.tipo     || "",
        });
      }
    }
    temMais = produtos.length >= 100;
    pagina++;
  }
}

// ─── 2. Busca estoque de cada produto ────────────────────────

const estoques = await emLotes(todosProdutos, CONCORRENCIA, async (produto) => {
  try {
    // ── Tentativa 1: endpoint de estoque ─────────────────────
    const r1      = parsarResposta(await tinyPost.call(this, "produto.obter.estoque.php", { id: produto.id }));
    const ret1    = r1?.retorno || r1 || {};
    const _dbg_estoque_status = ret1.status || "(sem status)";
    const _dbg_estoque_erro   = JSON.stringify(ret1.erros || ret1.codigo_erro || "");

    if (ret1.status === "OK") {
      const p = ret1.produto || {};
      const rawDepositos = Array.isArray(p.depositos) ? p.depositos : [];
      const depositos = rawDepositos.map(entry => {
        const d = entry.deposito || entry;
        return {
          id           : d.id            || "",
          nome         : d.nome          || "",
          desconsiderar: d.desconsiderar || "N",
          empresa      : d.empresa       || "",
          saldo        : parseNum(d.saldo),
        };
      });
      return {
        produto,
        saldoTotal    : parseNum(p.saldo),
        saldoReservado: parseNum(p.saldoReservado),
        depositos,
        _dbg_estoque_status, _dbg_estoque_erro,
        _dbg_fonte: "estoque",
      };
    }

    // ── Tentativa 2: produto.obter.php (tem saldo em algumas versões) ─
    const r2   = parsarResposta(await tinyPost.call(this, "produto.obter.php", { id: produto.id }));
    const ret2 = r2?.retorno || r2 || {};

    if (ret2.status === "OK") {
      const p = ret2.produto || {};
      const rawDepositos = Array.isArray(p.depositos) ? p.depositos : [];
      const depositos = rawDepositos.map(entry => {
        const d = entry.deposito || entry;
        return {
          id           : d.id            || "",
          nome         : d.nome          || "",
          desconsiderar: d.desconsiderar || "N",
          empresa      : d.empresa       || "",
          saldo        : parseNum(d.saldo),
        };
      });
      // Enriquece produto com campos do detalhe
      const produtoRico = {
        ...produto,
        nome    : p.nome             || produto.nome,
        preco   : parseNum(p.preco)  || produto.preco,
        unidade : p.unidade          || produto.unidade,
        situacao: p.situacao         || produto.situacao,
        tipo    : p.tipo             || produto.tipo,
      };
      return {
        produto       : produtoRico,
        saldoTotal    : parseNum(p.saldo),
        saldoReservado: parseNum(p.saldoReservado),
        depositos,
        _dbg_estoque_status, _dbg_estoque_erro,
        _dbg_fonte: "obter (saldo=" + String(p.saldo ?? "MISSING") + " | deps=" + rawDepositos.length + ")",
      };
    }

    return { produto, depositos: [], saldoTotal: 0, saldoReservado: 0,
      _dbg_estoque_status, _dbg_estoque_erro, _dbg_fonte: "ambos_falharam" };

  } catch (err) {
    return { produto, depositos: [], saldoTotal: 0, saldoReservado: 0,
      _dbg_estoque_status: "EXCEPTION", _dbg_estoque_erro: String(err), _dbg_fonte: "exception" };
  }
});

// ─── 3. Monta linhas ─────────────────────────────────────────

const linhas = [];

const agora = new Date().toISOString();

for (const { produto, depositos, saldoTotal, saldoReservado,
              _dbg_estoque_status, _dbg_estoque_erro, _dbg_fonte } of estoques) {

  if (!produto.sku || produto.sku === "0") continue;  // ignora produtos sem SKU ou com codigo="0"

  const base = {
    produto_id          : produto.id,
    sku                 : produto.sku,
    nome                : produto.nome,
    preco               : produto.preco,
    unidade             : produto.unidade,
    situacao            : produto.situacao,
    tipo                : produto.tipo,
    saldo_total         : saldoTotal,
    saldo_reservado     : saldoReservado,
    atualizado_em       : agora,
    _dbg_estoque_status,
    _dbg_estoque_erro,
    _dbg_fonte,
  };

  if (depositos.length === 0) {
    linhas.push({ json: {
      ...base,
      deposito_id     : null,
      deposito_nome   : "Nosso Depósito",
      deposito_empresa: null,
      desconsiderar   : "N",
      quantidade      : Math.max(0, saldoTotal),
    }});
    continue;
  }

  for (const dep of depositos) {
    if (!dep.nome) continue;
    linhas.push({ json: {
      ...base,
      deposito_id     : dep.id,
      deposito_nome   : dep.nome,
      deposito_empresa: dep.empresa,
      desconsiderar   : dep.desconsiderar,
      quantidade      : Math.max(0, dep.saldo),
    }});
  }
}

return linhas.length > 0
  ? linhas
  : [{ json: { erro: "Nenhum produto retornado", total: todosProdutos.length } }];
