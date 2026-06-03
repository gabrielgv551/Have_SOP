"""
╔══════════════════════════════════════════════════════════════╗
║  S&OP Intelligence · Sync Kits Tiny v2 → sku_kits          ║
║  API: produtos.pesquisa.php → produto.obter.php            ║
╚══════════════════════════════════════════════════════════════╝

COMO USAR:
  1. Cole o token v2 do Tiny na linha 18
  2. Configure a senha do banco na linha 23 (se diferente)
  3. Rode: python sync_kits_tiny_prompt.py

O script mostra uma barra de progresso e grava automaticamente
os kits encontrados na tabela sku_kits do banco Marcon.
"""
import os
import sys
import time
import requests
import json
from concurrent.futures import ThreadPoolExecutor
from sqlalchemy import create_engine, text

# ══════════════════════════════════════════════════════════════
# CONFIGURAÇÃO — ALTERE AQUI
# ══════════════════════════════════════════════════════════════
TINY_TOKEN = "adad5861e5d6cc4e25f4e0d6e2d17eafd87e7c90a2a535d3690a885761fd644e"
DB_PASSWORD = "131105Gv"  # <-- senha do postgres
# ══════════════════════════════════════════════════════════════

BASE_URL = "https://api.tiny.com.br/api2"
DB_CONFIG = {
    "host": "37.60.236.200", "port": 5432,
    "database": "Marcon", "user": "postgres",
    "password": DB_PASSWORD,
}
EMPRESA = "marcon"
CONCORRENCIA = 8

engine = create_engine(
    f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
    f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
)

# ── spinner / progress bar ──
def barra(pct, largura=30):
    cheio = int(pct * largura / 100)
    return "█" * cheio + "░" * (largura - cheio)

def tiny_post(endpoint, params):
    r = requests.post(
        f"{BASE_URL}/{endpoint}",
        data={"token": TINY_TOKEN, "formato": "JSON", **params},
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()

# ══════════════════════════════════════════════════════════════
# 1. LISTAR TODOS OS PRODUTOS
# ══════════════════════════════════════════════════════════════
print("\n[1/3] Buscando lista de produtos do Tiny...")
produtos = []
pagina = 1
while True:
    try:
        d = tiny_post("produtos.pesquisa.php", {"pagina": str(pagina)})
    except Exception as e:
        print(f"\n  Erro na página {pagina}: {e}")
        break
    ret = d.get("retorno") or {}
    prods = ret.get("produtos") or []
    if ret.get("status") != "OK" or not prods:
        break
    for item in prods:
        p = item.get("produto") or item
        if p.get("id") and p.get("codigo"):
            produtos.append({
                "id": str(p["id"]),
                "sku": p["codigo"].strip(),
                "nome": p.get("nome", ""),
            })
    print(f"  Página {pagina}: +{len(prods):3d} produtos  (total: {len(produtos)})")
    if len(prods) < 100:
        break
    pagina += 1

print(f"\n  ✅ {len(produtos)} produtos carregados.\n")

# ══════════════════════════════════════════════════════════════
# 2. VERIFICAR DETALHES (só produtos que parecem kit + amostra)
# ══════════════════════════════════════════════════════════════
# Verificar TODOS os produtos (kits podem não ter "KIT" no SKU)
candidatos = produtos

print(f"[2/3] Verificando {len(candidatos)} produtos candidatos na API (tipo=K)...")
print(f"      {barra(0)} 0%")

kits = []
verificados = 0

def verificar(p):
    try:
        d = tiny_post("produto.obter.php", {"id": p["id"]})
        ret = d.get("retorno") or {}
        if ret.get("status") != "OK":
            return None
        prod = ret.get("produto") or {}
        raw = prod.get("kit") or []
        if not raw:
            return None
        comps = []
        for entry in raw:
            item = entry.get("item") or entry
            sku_c = (item.get("codigo") or "").strip()
            qtd = float(item.get("quantidade", 1) or 1)
            if sku_c:
                comps.append({"sku": sku_c, "qtd": qtd})
        if comps:
            return {
                "sku_kit": prod.get("codigo", p["sku"]).strip(),
                "nome": prod.get("nome", p["nome"]),
                "componentes": comps,
            }
    except Exception:
        pass
    return None

# Processa em lotes para atualizar a barra
lote_tam = CONCORRENCIA
for i in range(0, len(candidatos), lote_tam):
    lote = candidatos[i:i + lote_tam]
    with ThreadPoolExecutor(max_workers=CONCORRENCIA) as pool:
        resultados = list(pool.map(verificar, lote))
    for r in resultados:
        if r:
            kits.append(r)
    verificados += len(lote)
    pct = int(verificados / len(candidatos) * 100)
    print(f"\r      {barra(pct)} {pct}%  ({verificados}/{len(candidatos)} verificados, {len(kits)} kits)", end="")
    time.sleep(0.05)

print(f"\n\n  ✅ {len(kits)} kits encontrados.\n")

# ══════════════════════════════════════════════════════════════
# 3. GRAVAR NO BANCO
# ══════════════════════════════════════════════════════════════
if not kits:
    print("[3/3] Nenhum kit encontrado. Nada a gravar.")
    sys.exit(0)

print("[3/3] Gravando de-para em sku_kits...")
gravados = 0
with engine.begin() as conn:
    for k in kits:
        for c in k["componentes"]:
            conn.execute(
                text("""
                    INSERT INTO sku_kits (empresa, sku_kit, sku_componente, quantidade, ativo)
                    VALUES (:e, :sk, :sc, :q, TRUE)
                    ON CONFLICT (empresa, sku_kit, sku_componente) DO UPDATE SET
                        quantidade = EXCLUDED.quantidade,
                        ativo      = TRUE
                """),
                {"e": EMPRESA, "sk": k["sku_kit"], "sc": c["sku"], "q": c["qtd"]},
            )
            gravados += 1

print(f"\n  ✅ {gravados} registros gravados em sku_kites.\n")
print("Kits sincronizados:")
for k in kits:
    comps = ", ".join([f"{c['sku']} x{int(c['qtd'])}" for c in k["componentes"]])
    print(f"    {k['sku_kit']} → {comps}")

print("\n────────────────────────────────────────")
print("Pronto! Agora pode rodar os scripts S&OP")
print("(PREVISÃO 12M.py, Estoque_Seguranca.py, Ponto_Pedido.py)")
print("────────────────────────────────────────")
