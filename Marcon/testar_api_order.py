"""
Testa /api/order com os parâmetros exatos usados pela tabela de pedidos
"""
import requests
import json

BASE = "https://sys.precocerto.co"
EMAIL = "comercial@casaeletromarcon.com.br"
SENHA = "eletro123"

r_jwt = requests.post(BASE + "/api/token/", json={"username": EMAIL, "password": SENHA}, timeout=10)
access = r_jwt.json()["access"]

api = requests.Session()
api.headers.update({
    "Authorization": f"Bearer {access}",
    "Accept": "application/json, text/plain, */*",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Referer": "https://sys.precocerto.co/gerenciar/pedidos-de-venda/",
    "Origin": "https://sys.precocerto.co",
})
print(f"✅ Autenticado\n")

# Testar /api/order com todos os parâmetros que a tabela usa
print("=== Testando /api/order ===")
param_sets = [
    # Parâmetros básicos da URL
    {"ordering": "-source_created"},
    {"source_created": "04/05/2026 - 19/05/2026", "ordering": "-source_created"},
    {"source_created": "04/05/2026 - 19/05/2026"},
    # Sem nenhum parâmetro
    {},
    # Com limit/offset (padrão de tabela)
    {"ordering": "-source_created", "limit": 10, "offset": 0},
    {"source_created": "04/05/2026 - 19/05/2026", "ordering": "-source_created", "limit": 10, "offset": 0},
    # Exatamente os parâmetros da URL original
    {"source_created": "04/05/2026 - 19/05/2026", "date_before": "2026-05-19", "date_after": "2026-05-04", "ordering": "-source_created"},
]

for params in param_sets:
    # URL com trailing ? igual ao frontend
    url = f"{BASE}/api/order"
    r = api.get(url, params=params, timeout=15)
    ct = r.headers.get("Content-Type", "")
    
    if r.status_code == 200 and "json" in ct:
        d = r.json()
        print(f"✅ {params}")
        print(f"   Keys: {list(d.keys()) if isinstance(d, dict) else 'lista'}")
        if isinstance(d, dict):
            print(f"   total={d.get('total', d.get('count', '?'))}, rows={len(d.get('rows', d.get('results', [])))} items")
            rows = d.get('rows', d.get('results', []))
            if rows:
                print(f"   Primeiro row keys: {list(rows[0].keys())}")
        print()
        break
    else:
        print(f"  ❌ {params} → {r.status_code}")

# Verificar também se /api/order precisa de um content-type especial
print("\n=== Testando com diferentes Content-Type ===")
for ct_header in ["application/json", "text/html", None]:
    extra = {"Content-Type": ct_header} if ct_header else {}
    r = api.get(BASE + "/api/order", 
                params={"ordering": "-source_created", "limit": 10},
                headers=extra,
                timeout=15)
    ct = r.headers.get("Content-Type", "")
    print(f"  Content-Type={ct_header}: {r.status_code} | {ct[:50]}")

# Verificar o timing (500 real vs 500 erro de app)
print("\n=== Timing do erro 500 ===")
import time
for ep in ["/api/order", "/api/order/facts", "/api/product-warehouse"]:
    start = time.time()
    r = api.get(BASE + ep, timeout=15)
    elapsed = time.time() - start
    print(f"  {ep}: {r.status_code} em {elapsed:.3f}s")

# Verificar se a URL exata do frontend funciona diferente
print("\n=== URL exata com ? no final ===")
r = api.get(BASE + "/api/order?", params={"ordering": "-source_created"}, timeout=15)
print(f"  /api/order? → {r.status_code}")

# Verificar se order com ID específico funciona
print("\n=== Tentando acessar /api/order/1 e similares ===")
for order_id in [1, 100, 1000]:
    r = api.get(BASE + f"/api/order/{order_id}", timeout=10)
    ct = r.headers.get("Content-Type", "")
    print(f"  /api/order/{order_id} → {r.status_code} | {ct[:40]}")
    if r.status_code == 200:
        print(f"    ✅ {r.text[:200]}")

# Tentar o endpoint de companies para entender o problema
print("\n=== Diagnóstico: tentando identificar a empresa do usuário ===")
# O user detail não tem company_id - vamos tentar outros endpoints
for ep in [
    "/api/basic-config/",
    "/api/basic-config",
    "/api/basic-config/tax",
    "/api/integration/",
    "/api/integration",
]:
    r = api.get(BASE + ep, timeout=10)
    ct = r.headers.get("Content-Type", "")
    if r.status_code == 200 and "json" in ct:
        d = r.json()
        print(f"✅ {ep}: {json.dumps(d, ensure_ascii=False)[:300]}")
    elif r.status_code not in (404, 405, 500):
        print(f"  {ep} → {r.status_code}")

print("\n=== Diagnóstico final: lista de todos os 200 OK da API ===")
r_root = api.get(BASE + "/api/", timeout=10)
for key, url in r_root.json().items():
    r = api.get(url, timeout=8)
    if r.status_code == 200:
        ct = r.headers.get("Content-Type", "")
        if "json" in ct:
            d = r.json()
            print(f"✅ {key}: {json.dumps(d, ensure_ascii=False)[:200]}")
