"""
Testa endpoints encontrados no bundle JS:
- /api/user/detail
- /api/order/
- /api/orderline/
- /api/order/facts com company header
"""
import requests
import json

BASE = "https://sys.precocerto.co"
EMAIL = "comercial@casaeletromarcon.com.br"
SENHA = "eletro123"

session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "pt-BR,pt;q=0.9",
    "Origin": "https://sys.precocerto.co",
    "Referer": "https://sys.precocerto.co/gerenciar/pedidos-de-venda/",
})

# Login
r = session.post(BASE + "/api/token/", json={"username": EMAIL, "password": SENHA}, timeout=10)
access = r.json()["access"]
session.headers["Authorization"] = f"Bearer {access}"
print(f"✅ Autenticado\n")

# 1. Buscar detalhes do usuário
print("=== GET /api/user/detail ===")
r = session.get(BASE + "/api/user/detail", timeout=10)
print(f"Status: {r.status_code}")
if r.status_code == 200:
    data = r.json()
    print(json.dumps(data, indent=2, ensure_ascii=False)[:1000])
    company_id = None
    if isinstance(data, dict):
        company_id = data.get("company_id") or data.get("company") or data.get("organization")
        print(f"\nCompany ID: {company_id}")
else:
    print(r.text[:300])

# 2. Testar /api/organization-users (está no API root)
print("\n=== GET /api/organization-users ===")
r = session.get(BASE + "/api/organization-users", timeout=10)
print(f"Status: {r.status_code}")
if r.status_code == 200:
    print(r.text[:600])

# 3. Tentar /api/order/ (singular, sem "facts")
print("\n=== Testando /api/order/ e variantes ===")
endpoints = [
    "/api/order/",
    "/api/order",
    "/api/orderline/",
    "/api/orderline",
    "/api/order/list",
    "/api/orders/",
]
for ep in endpoints:
    r = session.get(BASE + ep, params={"limit": 5}, timeout=10)
    ct = r.headers.get("Content-Type", "")
    flag = "✅" if r.status_code == 200 else ("⚠️" if r.status_code not in (404, 405) else "  ")
    print(f"{flag} {ep} → {r.status_code}")
    if r.status_code == 200 and "json" in ct:
        d = r.json()
        print(f"   Keys: {list(d.keys()) if isinstance(d, dict) else 'lista'}")
        if isinstance(d, dict) and "count" in d:
            print(f"   count={d['count']}")
        break

# 4. Tentar /api/order/facts sem autenticação (ver se é 401)
print("\n=== /api/order/facts sem auth (teste) ===")
r2 = requests.get(BASE + "/api/order/facts", timeout=10)
print(f"Sem auth: {r2.status_code}")

# 5. Testar /api/order/facts com company_id no header (se soubermos o ID)
print("\n=== /api/order/facts com headers extras ===")
for company_header in [None, "1", "16082"]:  # 16082 é o user_id do token
    extra = {}
    if company_header:
        extra["X-Company"] = company_header
        extra["X-Organization"] = company_header
        extra["Company-Id"] = company_header
    r = session.get(BASE + "/api/order/facts", headers=extra, 
                    params={"source_created": "04/05/2026 - 19/05/2026"}, timeout=10)
    print(f"Company header={company_header}: {r.status_code}")
    if r.status_code == 200:
        print(r.text[:400])

# 6. Tentar o endpoint de exportação de pedidos (vi referência no bundle)
print("\n=== Endpoints de exportação/sheet ===")
for ep in ["/api/order/export", "/api/order/download", "/api/export/orders", 
           "/api/sheet/order", "/api/sheets/order"]:
    r = session.get(BASE + ep, timeout=10)
    if r.status_code not in (404, 405):
        print(f"⚠️ {ep} → {r.status_code}: {r.text[:100]}")

# 7. Tentar endpoint de filtros do dashboard
print("\n=== GET /api/dashboard/filters ===")
r = session.get(BASE + "/api/dashboard/filters", params={"company_id": "1"}, timeout=10)
print(f"Status: {r.status_code}")
if r.status_code == 200:
    print(r.text[:400])

print("\n=== GET /api/integration/main ===")
r = session.get(BASE + "/api/integration/main", timeout=10)
print(f"Status: {r.status_code}")
if r.status_code == 200:
    print(r.text[:600])
elif r.status_code != 404:
    print(r.text[:200])
