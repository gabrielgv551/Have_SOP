"""
Explorador de endpoints autenticados do precocerto.co
"""
import requests

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
tokens = r.json()
access = tokens["access"]
print(f"✅ Token obtido: {access[:40]}...")
session.headers["Authorization"] = f"Bearer {access}"

# Explorar raiz da API
print("\n--- GET /api/ ---")
r = session.get(BASE + "/api/", timeout=10)
print(f"Status: {r.status_code}")
if r.status_code == 200:
    print(r.text[:2000])

# Tentar endpoints prováveis com autenticação
print("\n--- Testando endpoints ---")
candidatos = [
    "/api/sales/orders/",
    "/api/sale/orders/",
    "/api/comercial/pedidos/",
    "/api/gerenciar/pedidos-de-venda/",
    "/api/pedido/",
    "/api/pedidos/",
    "/api/order/",
    "/api/orders/sale/",
    "/api/erp/orders/",
    "/api/channel-sale-orders/",
    "/api/channel-orders/",
    "/api/marketplace/orders/",
    "/api/erp/sale-orders/",
    "/api/v1/",  # explorar versão
]

for ep in candidatos:
    r = session.get(BASE + ep, params={"limit": 1}, timeout=10)
    ct = r.headers.get("Content-Type", "")
    flag = "✅" if r.status_code == 200 else ("⚠️ " if r.status_code not in (404,405) else "  ")
    print(f"{flag} {ep} → {r.status_code}", end="")
    if r.status_code == 200 and "json" in ct:
        preview = r.text[:200]
        print(f"  {preview}")
    elif r.status_code not in (404, 405) and r.text:
        print(f"  {r.text[:100]}")
    else:
        print()

# Tentar descobrir pelos paths no HTML da página principal
print("\n--- Analisando paths da URL original ---")
# O frontend acessa /gerenciar/pedidos-de-venda/
# O backend provavelmente tem path similar como /api/
# Tentar com o path da URL do front sem /gerenciar/
for ep in ["/api/sale-order/", "/api/saleorder/", "/api/channel-sale-order/"]:
    r = session.get(BASE + ep, params={"limit": 1}, timeout=10)
    print(f"  {ep} → {r.status_code} | {r.text[:100]}")
