"""
╔══════════════════════════════════════════════════════════════╗
║  S&OP Intelligence · Sync Kits Tiny v2 → sku_kits            ║
║  API: produtos.pesquisa.php → produto.obter.php            ║
╚══════════════════════════════════════════════════════════════╝

USO:
  python sync_kits_tiny_v2.py

Precisa configurar TINY_TOKEN no início do script.
"""
import os
import sys
import time
import requests
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy import create_engine, text

# ══════════════════════════════════════════════════════════════
# CONFIGURAÇÃO
# ══════════════════════════════════════════════════════════════
TINY_TOKEN = os.getenv("TINY_TOKEN", "").strip()  # <-- cole o token v2 aqui
if not TINY_TOKEN:
    print("ERRO: defina TINY_TOKEN no início do script ou via env var.")
    sys.exit(1)

BASE_URL = "https://api.tiny.com.br/api2"
DB_CONFIG = {
    "host": "37.60.236.200", "port": 5432,
    "database": "Marcon", "user": "postgres",
    "password": "131105Gv",
}
EMPRESA = "marcon"
CONCORRENCIA = 5

engine = create_engine(
    f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
    f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
)


def tiny_post(endpoint, params):
    """Faz POST para endpoint da API v2 do Tiny."""
    body = {
        "token": TINY_TOKEN,
        "formato": "JSON",
        **params,
    }
    try:
        r = requests.post(
            f"{BASE_URL}/{endpoint}",
            data=body,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=30,
        )
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"  API erro em {endpoint}: {e}")
        return None


def parse_dados(resp):
    if resp is None:
        return {}
    if isinstance(resp, str):
        try:
            return json.loads(resp)
        except json.JSONDecodeError:
            return {}
    return resp


# ══════════════════════════════════════════════════════════════
# 1. COLETAR TODOS OS PRODUTOS
# ══════════════════════════════════════════════════════════════
print("[1/3] Buscando todos os produtos do Tiny...")
todos_produtos = []
pagina = 1
tem_mais = True

while tem_mais:
    dados = parse_dados(tiny_post("produtos.pesquisa.php", {"pagina": str(pagina)}))
    retorno = dados.get("retorno") or dados or {}
    status = retorno.get("status")
    produtos = retorno.get("produtos") or []

    if status != "OK" or not produtos:
        tem_mais = False
    else:
        for item in produtos:
            p = item.get("produto") or item
            if p.get("id") and p.get("codigo"):
                todos_produtos.append({
                    "id": str(p["id"]),
                    "sku": p["codigo"].strip(),
                    "nome": p.get("nome", ""),
                })
        tem_mais = len(produtos) >= 100
        pagina += 1
        print(f"  Página {pagina - 1}: {len(produtos)} produtos  (total acumulado: {len(todos_produtos)})")

print(f"\n[OK] {len(todos_produtos)} produtos encontrados.")


# ══════════════════════════════════════════════════════════════
# 2. BUSCAR DETALHES E FILTRAR TIPO "K" (KIT)
# ══════════════════════════════════════════════════════════════
print("\n[2/3] Buscando detalhes para identificar kits (tipo=K)...")


def buscar_kit(produto):
    try:
        dados = parse_dados(tiny_post("produto.obter.php", {"id": produto["id"]}))
        retorno = dados.get("retorno") or dados or {}
        if retorno.get("status") != "OK":
            return None

        p = retorno.get("produto") or {}
        if p.get("tipo") != "K":
            return None

        sku_kit = p.get("codigo", produto["sku"]).strip()
        nome_kit = p.get("nome", produto["nome"])

        raw_kit = p.get("kit") or []
        if not isinstance(raw_kit, list):
            return None

        componentes = []
        for entry in raw_kit:
            item = entry.get("item") or entry
            sku_comp = (item.get("codigo") or "").strip()
            nome_comp = item.get("descricao") or item.get("nome") or ""
            qtd = float(item.get("quantidade", 1) or 1)
            if sku_comp:
                componentes.append({"sku": sku_comp, "nome": nome_comp, "qtd": qtd})

        if not componentes:
            return None

        return {
            "sku_kit": sku_kit,
            "nome_kit": nome_kit,
            "componentes": componentes,
        }
    except Exception as e:
        return None


kits = []
for i in range(0, len(todos_produtos), CONCORRENCIA):
    lote = todos_produtos[i : i + CONCORRENCIA]
    resultados = list(ThreadPoolExecutor(max_workers=CONCORRENCIA).map(buscar_kit, lote))
    for r in resultados:
        if r:
            kits.append(r)
    if (i // CONCORRENCIA) % 20 == 0:
        print(f"  {i}/{len(todos_produtos)} verificados... {len(kits)} kits encontrados")
    time.sleep(0.1)  # rate-limit friendly

print(f"\n[OK] {len(kits)} kits encontrados com componentes.")

# ══════════════════════════════════════════════════════════════
# 3. GRAVAR NO BANCO (sku_kits)
# ══════════════════════════════════════════════════════════════
print("\n[3/3] Gravando de-para em sku_kits...")

if not kits:
    print("Nenhum kit encontrado. Nada a gravar.")
    sys.exit(0)

gravados = 0
with engine.begin() as conn:
    for k in kits:
        for c in k["componentes"]:
            conn.execute(
                text(
                    """
                    INSERT INTO sku_kits (empresa, sku_kit, sku_componente, quantidade, ativo)
                    VALUES (:e, :sk, :sc, :q, TRUE)
                    ON CONFLICT (empresa, sku_kit, sku_componente) DO UPDATE SET
                        quantidade = EXCLUDED.quantidade,
                        ativo      = TRUE
                """
                ),
                {"e": EMPRESA, "sk": k["sku_kit"], "sc": c["sku"], "q": c["qtd"]},
            )
            gravados += 1

print(f"\n[SUCESSO] {gravados} registros gravados/atualizados em sku_kits.")
print("\nKits sincronizados:")
for k in kits:
    comps = ", ".join([f"{c['sku']} x{c['qtd']}" for c in k["componentes"]])
    print(f"  {k['sku_kit']} → {comps}")
