"""
Sync Kits Tiny v2 — versão leve (evita rate limit)
Busca páginas com delay e grava kits encontrados.
"""
import requests, time, json
from sqlalchemy import create_engine, text

TOKEN = "adad5861e5d6cc4e25f4e0d6e2d17eafd87e7c90a2a535d3690a885761fd644e"
BASE = "https://api.tiny.com.br/api2"
engine = create_engine("postgresql+psycopg2://postgres:131105Gv@37.60.236.200:5432/Marcon")

def tiny_post(endpoint, params):
    r = requests.post(f"{BASE}/{endpoint}", data={"token": TOKEN, "formato": "JSON", **params}, timeout=30)
    return r.json()

print("Buscando produtos...")
produtos = []
pagina = 1
while True:
    d = tiny_post("produtos.pesquisa.php", {"pagina": str(pagina)})
    ret = d.get("retorno") or {}
    prods = ret.get("produtos") or []
    if ret.get("status") != "OK" or not prods:
        break
    for item in prods:
        p = item.get("produto") or item
        if p.get("id") and p.get("codigo"):
            produtos.append({"id": str(p["id"]), "sku": p["codigo"].strip(), "nome": p.get("nome","")})
    print(f"  Pag {pagina}: +{len(prods)} (total {len(produtos)})")
    if len(prods) < 100:
        break
    pagina += 1
    time.sleep(0.5)  # evita rate limit

print(f"\nTotal: {len(produtos)} produtos")

# Verificar detalhes com delay
print("\nVerificando detalhes (1 a cada 0.3s)...")
kits = []
for i, p in enumerate(produtos):
    if i % 50 == 0:
        print(f"  {i}/{len(produtos)}... kits: {len(kits)}")
    try:
        d = tiny_post("produto.obter.php", {"id": p["id"]})
        ret = d.get("retorno") or {}
        if ret.get("status") != "OK":
            continue
        prod = ret.get("produto") or {}
        raw = prod.get("kit") or []
        if not raw:
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
    except Exception as e:
        pass
    time.sleep(0.3)

print(f"\n=== {len(kits)} KITS ENCONTRADOS ===")
for k in kits:
    comps = ", ".join([f"{c['sku']} x{int(c['qtd'])}" for c in k["componentes"]])
    print(f"  {k['sku_kit']} → {comps}")

if kits:
    print(f"\nGravando {sum(len(k['componentes']) for k in kits)} registros em sku_kits...")
    with engine.begin() as conn:
        for k in kits:
            for c in k["componentes"]:
                conn.execute(text("""
                    INSERT INTO sku_kits (empresa, sku_kit, sku_componente, quantidade, ativo)
                    VALUES ('marcon', :sk, :sc, :q, TRUE)
                    ON CONFLICT (empresa, sku_kit, sku_componente) DO UPDATE SET
                        quantidade = EXCLUDED.quantidade, ativo = TRUE
                """), {"sk": k["sku_kit"], "sc": c["sku"], "q": c["qtd"]})
    print("✅ Gravado com sucesso!")
else:
    print("Nenhum kit encontrado.")
