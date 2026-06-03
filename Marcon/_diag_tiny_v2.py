import requests, json, os

TOKEN = os.getenv("TINY_TOKEN", "adad5861e5d6cc4e25f4e0d6e2d17eafd87e7c90a2a535d3690a885761fd644e")
BASE = "https://api.tiny.com.br/api2"

def tiny_post(endpoint, params):
    r = requests.post(f"{BASE}/{endpoint}", data={"token": TOKEN, "formato": "JSON", **params}, timeout=30)
    return r.json()

# 1. Pesquisar produtos
print("=== produtos.pesquisa.php (página 1) ===")
d = tiny_post("produtos.pesquisa.php", {"pagina": "1"})
print(json.dumps(d, indent=2, ensure_ascii=False)[:1500])

# 2. Pegar primeiro produto e ver detalhes
produtos = (d.get("retorno") or {}).get("produtos") or []
if produtos:
    primeiro = produtos[0].get("produto") or produtos[0]
    pid = primeiro.get("id")
    print(f"\n=== produto.obter.php (id={pid}) ===")
    det = tiny_post("produto.obter.php", {"id": pid})
    print(json.dumps(det, indent=2, ensure_ascii=False)[:1500])
    
    # Verificar se tem campo tipo
    p = (det.get("retorno") or {}).get("produto") or {}
    print(f"\ntipo={p.get('tipo')}")
    if p.get("kit"):
        print(f"kit={json.dumps(p['kit'], indent=2, ensure_ascii=False)[:800]}")
