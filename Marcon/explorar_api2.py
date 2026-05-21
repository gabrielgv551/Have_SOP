"""
Explora todos os endpoints da API e testa o de pedidos
"""
import requests
import json

BASE = "https://sys.precocerto.co"
EMAIL = "comercial@casaeletromarcon.com.br"
SENHA = "eletro123"

session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "application/json",
    "Origin": "https://sys.precocerto.co",
    "Referer": "https://sys.precocerto.co/",
})

# Login
r = session.post(BASE + "/api/token/", json={"username": EMAIL, "password": SENHA}, timeout=10)
access = r.json()["access"]
session.headers["Authorization"] = f"Bearer {access}"
print(f"✅ Autenticado\n")

# Buscar raiz completa da API
r = session.get(BASE + "/api/", timeout=10)
api_root = r.json()
print("=== TODOS OS ENDPOINTS DISPONÍVEIS ===")
for key, url in sorted(api_root.items()):
    print(f"  {key}: {url}")

# Testar endpoints relacionados a pedidos/orders
print("\n=== TESTANDO ENDPOINTS DE PEDIDOS ===")
order_keys = [k for k in api_root if any(x in k.lower() for x in ["order", "sale", "pedido", "venda", "channel"])]
for key in order_keys:
    url = api_root[key]
    r = session.get(url, params={"limit": 2, "offset": 0, "date_after": "2026-05-01", "date_before": "2026-05-19"}, timeout=15)
    print(f"\n[{key}] → {r.status_code}")
    if r.status_code == 200:
        try:
            data = r.json()
            print(f"  Tipo: {type(data).__name__}")
            if isinstance(data, dict):
                print(f"  Keys: {list(data.keys())[:10]}")
                if "count" in data:
                    print(f"  Total registros: {data['count']}")
                if "results" in data and data["results"]:
                    print(f"  Primeiro resultado keys: {list(data['results'][0].keys())[:15]}")
            elif isinstance(data, list) and data:
                print(f"  Lista com {len(data)} items, primeiro: {list(data[0].keys())[:15]}")
        except:
            print(f"  {r.text[:300]}")
    elif r.status_code != 404:
        print(f"  {r.text[:150]}")

# Testar order/facts especificamente com filtros de data
print("\n=== TESTANDO /api/order/facts ===")
params_list = [
    {"limit": 5},
    {"limit": 5, "source_created": "04/05/2026 - 19/05/2026"},
    {"limit": 5, "date_after": "2026-05-04", "date_before": "2026-05-19"},
    {"limit": 5, "ordering": "-source_created"},
]
for params in params_list:
    r = session.get(BASE + "/api/order/facts", params=params, timeout=15)
    print(f"\n  Params: {params}")
    print(f"  Status: {r.status_code}")
    if r.status_code == 200:
        data = r.json()
        if isinstance(data, dict):
            print(f"  Keys: {list(data.keys())}")
            print(f"  Count: {data.get('count')}")
            if "results" in data and data["results"]:
                print(f"  Primeiro item keys: {list(data['results'][0].keys())}")
                print(f"  Primeiro item: {json.dumps(data['results'][0], indent=2, ensure_ascii=False)[:600]}")
        elif isinstance(data, list):
            print(f"  Lista: {len(data)} items")
            if data:
                print(f"  Primeiro: {json.dumps(data[0], indent=2, ensure_ascii=False)[:400]}")
        break
    else:
        print(f"  {r.text[:150]}")
