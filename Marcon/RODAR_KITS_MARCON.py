# HAVE GESTOR - Sync Kits Tiny -> sku_kits (Marcon)
# Token: API v2 do Tiny ERP
# Uso: python RODAR_KITS_MARCON.py
import os
import sys
import time
import requests
from sqlalchemy import create_engine, text

# ---------------------------------------------------------------
# CONFIGURACAO
# ---------------------------------------------------------------
TINY_TOKEN = "adad5861e5d6cc4e25f4e0d6e2d17eafd87e7c90a2a535d3690a885761fd644e"
DB_PASSWORD = "131105Gv"

BASE_URL = "https://api.tiny.com.br/api2"
ENGINE = create_engine(
    f"postgresql+psycopg2://postgres:{DB_PASSWORD}@37.60.236.200:5432/Marcon"
)

def tiny_post(endpoint, params):
    r = requests.post(
        f"{BASE_URL}/{endpoint}",
        data={"token": TINY_TOKEN, "formato": "JSON", **params},
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()

# ---------------------------------------------------------------
# 1. LISTAR PRODUTOS
# ---------------------------------------------------------------
print("=" * 60)
print("PASSO 1/3: Listando produtos do Tiny...")
print("=" * 60)

produtos = []
pagina = 1
while True:
    try:
        d = tiny_post("produtos.pesquisa.php", {"pagina": str(pagina)})
    except Exception as e:
        print(f"  ERRO página {pagina}: {e}")
        break

    ret = d.get("retorno") or {}
    if ret.get("status") != "OK":
        print(f"  API retornou status: {ret.get('status')}")
        break

    prods = ret.get("produtos") or []
    if not prods:
        break

    for item in prods:
        p = item.get("produto") or item
        if p.get("id") and p.get("codigo"):
            produtos.append({
                "id": str(p["id"]),
                "sku": p["codigo"].strip(),
                "nome": p.get("nome", ""),
            })

    print(f"  Pag {pagina:2d}: +{len(prods):3d} produtos  (total: {len(produtos)})")

    if len(prods) < 100:
        break
    pagina += 1
    time.sleep(0.3)

print(f"\n  Total de produtos: {len(produtos)}\n")

if not produtos:
    print("ERRO: Nenhum produto retornado. Token pode estar bloqueado.")
    sys.exit(1)

# ---------------------------------------------------------------
# 2. VERIFICAR KITS (com delay para evitar rate limit)
# ---------------------------------------------------------------
print("=" * 60)
print("PASSO 2/3: Verificando detalhes (0.4s entre cada)...")
print("=" * 60)

kits = []
total = len(produtos)

for i, p in enumerate(produtos):
    # Barra de progresso
    pct = int((i + 1) / total * 100)
    cheio = pct // 3
    vazio = 33 - cheio
    barra = "#" * cheio + "-" * vazio
    print(f"\r  {barra} {pct:3d}%  ({i+1}/{total} verificados, {len(kits)} kits)", end="")

    try:
        d = tiny_post("produto.obter.php", {"id": p["id"]})
        ret = d.get("retorno") or {}
        if ret.get("status") != "OK":
            time.sleep(0.4)
            continue

        prod = ret.get("produto") or {}
        raw = prod.get("kit") or []
        if not raw:
            time.sleep(0.4)
            continue

        comps = []
        for entry in raw:
            item = entry.get("item") or entry
            sku_c = (item.get("codigo") or "").strip()
            qtd = float(item.get("quantidade", 1) or 1)
            if sku_c:
                comps.append({"sku": sku_c, "qtd": qtd})

        if comps:
            kits.append({
                "sku_kit": prod.get("codigo", p["sku"]).strip(),
                "nome": prod.get("nome", p["nome"]),
                "componentes": comps,
            })

    except Exception:
        pass

    time.sleep(0.4)

print(f"\n\n  Kits encontrados: {len(kits)}\n")

# ---------------------------------------------------------------
# 3. GRAVAR NO BANCO
# ---------------------------------------------------------------
print("=" * 60)
print("PASSO 3/3: Gravando em sku_kits...")
print("=" * 60)

if not kits:
    print("  Nenhum kit encontrado. Nada a gravar.")
    sys.exit(0)

gravados = 0
with ENGINE.begin() as conn:
    for k in kits:
        for c in k["componentes"]:
            conn.execute(
                text("""
                    INSERT INTO sku_kits (empresa, sku_kit, sku_componente, quantidade, ativo)
                    VALUES ('marcon', :sk, :sc, :q, TRUE)
                    ON CONFLICT (empresa, sku_kit, sku_componente) DO UPDATE SET
                        quantidade = EXCLUDED.quantidade,
                        ativo      = TRUE
                """),
                {"sk": k["sku_kit"], "sc": c["sku"], "q": c["qtd"]},
            )
            gravados += 1

print(f"\n  {gravados} registros gravados em sku_kits.\n")
print("Kits sincronizados:")
for k in kits:
    comps = ", ".join([f"{c['sku']} x{int(c['qtd'])}" for c in k["componentes"]])
    print(f"    {k['sku_kit']} -> {comps}")

print("\n" + "=" * 60)
print("PRONTO! Agora pode rodar os scripts S&OP:")
print("  PREVISAO 12M.py")
print("  Estoque_Seguranca.py")
print("  Ponto_Pedido.py")
print("=" * 60)
