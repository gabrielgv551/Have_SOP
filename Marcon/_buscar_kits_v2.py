import requests, json, time
from concurrent.futures import ThreadPoolExecutor

TOKEN = "adad5861e5d6cc4e25f4e0d6e2d17eafd87e7c90a2a535d3690a885761fd644e"
BASE = "https://api.tiny.com.br/api2"

def tiny_post(endpoint, params):
    r = requests.post(f"{BASE}/{endpoint}", data={"token": TOKEN, "formato": "JSON", **params}, timeout=30)
    return r.json()

# Buscar todos os produtos
print("Buscando produtos...")
todos = []
pagina = 1
while True:
    d = tiny_post("produtos.pesquisa.php", {"pagina": str(pagina)})
    ret = d.get("retorno") or {}
    prods = ret.get("produtos") or []
    if not prods or ret.get("status") != "OK":
        break
    for item in prods:
        p = item.get("produto") or item
        todos.append({"id": p["id"], "sku": p.get("codigo",""), "nome": p.get("nome","")})
    print(f"  Pag {pagina}: +{len(prods)} (total {len(todos)})")
    if len(prods) < 100:
        break
    pagina += 1

print(f"\nTotal produtos: {len(todos)}")

# Buscar detalhes em lotes de 10 concorrentes
kits = []

def check(p):
    try:
        d = tiny_post("produto.obter.php", {"id": p["id"]})
        ret = d.get("retorno") or {}
        if ret.get("status") != "OK":
            return None
        prod = ret.get("produto") or {}
        if prod.get("tipo") == "K":
            raw = prod.get("kit") or []
            comps = []
            for entry in raw:
                item = entry.get("item") or entry
                sku = (item.get("codigo") or "").strip()
                qtd = float(item.get("quantidade", 1) or 1)
                if sku:
                    comps.append({"sku": sku, "qtd": qtd})
            if comps:
                return {"sku_kit": prod.get("codigo", p["sku"]).strip(), "nome": prod.get("nome", p["nome"]), "componentes": comps}
    except Exception as e:
        pass
    return None

print("\nVerificando detalhes...")
for i in range(0, len(todos), 10):
    lote = todos[i:i+10]
    with ThreadPoolExecutor(10) as pool:
        res = list(pool.map(check, lote))
    for r in res:
        if r:
            kits.append(r)
    if i % 100 == 0:
        print(f"  {i}/{len(todos)}... kits: {len(kits)}")
    time.sleep(0.1)

print(f"\n=== {len(kits)} KITS ENCONTRADOS ===")
for k in kits:
    comps = ", ".join([f"{c['sku']} x{c['qtd']}" for c in k["componentes"]])
    print(f"  {k['sku_kit']} -> {comps}")
