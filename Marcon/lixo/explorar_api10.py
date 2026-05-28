"""
1. Inspeciona respostas dos endpoints que funcionam (product-warehouse, etc.)
2. Tenta encontrar company_id na resposta do user/detail mais detalhada
3. Verifica se há endpoints alternos para orders no bundle
"""
import requests
import json
import base64

BASE = "https://sys.precocerto.co"
EMAIL = "comercial@casaeletromarcon.com.br"
SENHA = "eletro123"

r_jwt = requests.post(BASE + "/api/token/", json={"username": EMAIL, "password": SENHA}, timeout=10)
access = r_jwt.json()["access"]
api = requests.Session()
api.headers.update({
    "Authorization": f"Bearer {access}",
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0",
})

# 1. Ver resposta completa de product-warehouse
print("=== GET /api/product-warehouse ===")
r = api.get(BASE + "/api/product-warehouse", timeout=10)
print(json.dumps(r.json(), indent=2, ensure_ascii=False)[:800])

# 2. payment-method
print("\n=== GET /api/payment-method ===")
r = api.get(BASE + "/api/payment-method", timeout=10)
print(json.dumps(r.json(), indent=2, ensure_ascii=False)[:800])

# 3. integration/main
print("\n=== GET /api/integration/main ===")
r = api.get(BASE + "/api/integration/main", timeout=10)
print(json.dumps(r.json(), indent=2, ensure_ascii=False)[:800])

# 4. basic-config/tax
print("\n=== GET /api/basic-config/tax ===")
r = api.get(BASE + "/api/basic-config/tax", timeout=10)
print(json.dumps(r.json(), indent=2, ensure_ascii=False)[:800])

# 5. Tentar /api/user/detail com query params para mais info
print("\n=== GET /api/user/detail (completo) ===")
r = api.get(BASE + "/api/user/detail", timeout=10)
print(json.dumps(r.json(), indent=2, ensure_ascii=False))

# 6. Tentar endpoints de "company" que não são "company/facts"
print("\n=== Testando sub-rotas de company ===")
for ep in [
    "/api/company/detail",
    "/api/company/info",
    "/api/company/current",
    "/api/company/list",
    "/api/company/me",
    "/api/active-company",
    "/api/my-company",
]:
    r = api.get(BASE + ep, timeout=10)
    ct = r.headers.get("Content-Type", "")
    if r.status_code == 200 and "json" in ct:
        print(f"✅ {ep}: {r.text[:300]}")
    elif r.status_code not in (404, 405, 500):
        print(f"⚠️  {ep} → {r.status_code}")

# 7. Ver integration-task (retornou 200 html)
print("\n=== GET /api/integration-task ===")
r = api.get(BASE + "/api/integration-task", params={"limit": 5}, timeout=10)
ct = r.headers.get("Content-Type", "")
print(f"Status: {r.status_code}, CT: {ct}")
if "json" in ct:
    print(r.text[:500])

# 8. Tentar o endpoint de orders pelo padrão do DRF (com ?format=json)
print("\n=== Testando variações do endpoint de orders ===")
variants = [
    "/api/order/facts",
    "/api/orders/facts",
    "/api/sale-order/facts",
    "/api/sale-orders/",
    "/api/orders/",
    "/api/order/",
    "/api/order/list",
    "/api/order/summary",
    "/api/order/report",
]
params_to_try = [
    {},
    {"ordering": "-source_created"},
    {"date_after": "2026-05-04", "date_before": "2026-05-19"},
    {"source_created": "04/05/2026 - 19/05/2026"},
    {"company_id": "1"},
]
for ep in variants:
    for params in params_to_try:
        r = api.get(BASE + ep, params=params, timeout=10)
        ct = r.headers.get("Content-Type", "")
        if r.status_code == 200 and "json" in ct:
            print(f"✅ {ep} {params} → 200!")
            d = r.json()
            print(f"  Keys: {list(d.keys()) if isinstance(d, dict) else 'lista'}")
            break
        elif r.status_code not in (404, 405, 500):
            print(f"⚠️  {ep} {params} → {r.status_code}")
