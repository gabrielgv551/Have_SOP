// ============================================================
//  OLIST TINY — Kits e Componentes (n8n Code Node)
//  Retorna uma linha por (kit × componente).
//  Campos: sku_kit, nome_kit, total_componentes,
//          sku_componente, nome_componente, quantidade
// ============================================================

const TOKEN        = "adad5861e5d6cc4e25f4e0d6e2d17eafd87e7c90a2a535d3690a885761fd644e";
const CONCORRENCIA = 5;
const BASE_URL     = "https://api.tiny.com.br/api2";

// ─── Helpers ─────────────────────────────────────────────────

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

function parseDados(resp) {
  if (typeof resp === "string") {
    try { return JSON.parse(resp); } catch(e) { return {}; }
  }
  return resp || {};
}

// ─── 1. Coleta todos os produtos (paginado) ─────────────────
// Nota: produtos.pesquisa.php NÃO retorna o campo "tipo".
// Por isso coletamos todos e filtramos por tipo=K no passo 2.

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

  const dados    = parseDados(resp);
  const retorno  = dados?.retorno || dados || {};
  const status   = retorno.status;
  const produtos = retorno.produtos || [];

  if (status !== "OK" || produtos.length === 0) {
    temMais = false;
  } else {
    for (const item of produtos) {
      const p = item.produto || item;
      if (p.id && p.codigo) {
        todosProdutos.push({
          id  : String(p.id),
          sku : p.codigo.trim(),
          nome: p.nome || "",
        });
      }
    }
    temMais = produtos.length >= 100;
    pagina++;
  }
}

// ─── 2. Busca detalhes e filtra tipo "K" (kit) ───────────────
// produto.obter.php retorna tipo + array kit[] com os componentes.

const kitsDetalhados = [];

await emLotes(todosProdutos, CONCORRENCIA, async (produto) => {
  try {
    const resp    = await tinyPost.call(this, "produto.obter.php", { id: produto.id });
    const dados   = parseDados(resp);
    const retorno = dados?.retorno || dados || {};

    if (retorno.status !== "OK") return;

    const p = retorno.produto || {};

    // Ignora se não for kit
    if (p.tipo !== "K") return;

    // p.kit = [{item: {id, codigo, descricao, quantidade}}, ...]
    const rawKit      = Array.isArray(p.kit) ? p.kit : [];
    const componentes = rawKit
      .map(entry => {
        const c = entry.item || entry;
        return {
          sku      : (c.codigo || "").trim(),
          nome     : c.descricao || c.nome || "",
          quantidade: Number(c.quantidade) || 1,
        };
      })
      .filter(c => c.sku);

    kitsDetalhados.push({ kit: produto, componentes });
  } catch (err) {
    // silencia erros individuais
  }
});

// ─── 3. Monta linhas ─────────────────────────────────────────

const linhas      = [];
const agora       = new Date().toISOString();

for (const { kit, componentes } of kitsDetalhados) {

  if (componentes.length === 0) {
    // kit sem componentes cadastrados — registra para visibilidade
    linhas.push({ json: {
      sku_kit          : kit.sku,
      nome_kit         : kit.nome,
      total_componentes: 0,
      sku_componente   : null,
      nome_componente  : null,
      quantidade       : null,
      atualizado_em    : agora,
    }});
    continue;
  }

  for (const comp of componentes) {
    linhas.push({ json: {
      sku_kit          : kit.sku,
      nome_kit         : kit.nome,
      total_componentes: componentes.length,
      sku_componente   : comp.sku,
      nome_componente  : comp.nome,
      quantidade       : comp.quantidade,
      atualizado_em    : agora,
    }});
  }
}

return linhas.length > 0
  ? linhas
  : [{ json: { aviso: "Nenhum kit encontrado (tipo K)", total_produtos_buscados: todosProdutos.length } }];
