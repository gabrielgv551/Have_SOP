import requests, json, os
from datetime import datetime

BASE_URL = "https://gateway-web.ge.finance/api"
APP_ORIGIN = "https://app.ge.finance"

def login(email, password):
    url = f"{BASE_URL}/Auth/login"
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/plain, */*",
        "Origin": APP_ORIGIN,
        "Referer": f"{APP_ORIGIN}/",
        "language": "pt-BR",
        "User-Agent": "Mozilla/5.0",
    }
    resp = requests.post(url, json={"username": email, "password": password}, headers=headers, timeout=20)
    resp.raise_for_status()
    data = resp.json()
    token = data.get("token") or data.get("access_token") or (data.get("data") or {}).get("token")
    customer_id = data.get("customerId") or (data.get("data") or {}).get("customerId") or ""
    plan_id = data.get("customerPlanId") or (data.get("data") or {}).get("customerPlanId") or "507"
    return {"token": token, "customerId": str(customer_id), "planId": str(plan_id)}

def get_page(auth, page=1, page_size=500):
    url = f"{BASE_URL}/SpreadSheet"
    params = {
        "pageSize": str(page_size),
        "firstDate": "2026-05-01",
        "endDate": "2026-05-29",
        "refreshDate": datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000"),
        "sortColumn": "date",
        "sortType": "2",
        "currentPage": str(page),
        "customerId": auth["customerId"],
        "customerPlanId": auth["planId"],
        "isTrial": "false",
    }
    headers = {
        "Authorization": f"Bearer {auth['token']}",
        "customerid": auth["customerId"],
        "customerplanid": auth["planId"],
        "language": "pt-BR",
        "Accept": "application/json",
        "Origin": APP_ORIGIN,
        "Referer": f"{APP_ORIGIN}/",
    }
    resp = requests.get(url, headers=headers, params=params, timeout=60)
    return resp.json()

# Login
email = os.getenv("GEFINANCE_EMAIL")
password = os.getenv("GEFINANCE_PASSWORD")
if not email or not password:
    import sqlalchemy as sa
    eng = sa.create_engine("postgresql+psycopg2://postgres:131105Gv@37.60.236.200:5432/Lanzi")
    with eng.begin() as c:
        rows = c.execute(sa.text("SELECT chave, valor FROM configuracoes WHERE empresa='lanzi' AND chave IN ('gefinance_email', 'gefinance_password')")).fetchall()
        cfg = {r[0]: r[1] for r in rows}
        email = cfg.get("gefinance_email")
        password = cfg.get("gefinance_password")

auth = login(email, password)
data = get_page(auth, 1, 500)

# Achar pedido com multiplos produtos e frete
for pedido in data.get("result", []):
    prods = pedido.get("products", [])
    if len(prods) > 1:
        frete = pedido.get("receivedFreight", 0)
        if frete and frete > 0:
            print(f"\n=== Pedido {pedido.get('id')} ===")
            print(f"Pedido totalSale: {pedido.get('totalSale')}")
            print(f"Pedido receivedFreight: {frete}")
            print(f"Pedido paidFreight: {pedido.get('paidFreight')}")
            print(f"Pedido shippingCost: {pedido.get('shippingCost')}")
            print(f"N produtos: {len(prods)}")
            for i, prod in enumerate(prods):
                print(f"\n  Produto {i+1}: {prod.get('productName')}")
                print(f"    sku: {prod.get('sku')}")
                print(f"    totalSale: {prod.get('totalSale')}")
                print(f"    totalSaleWithShippingAndTax: {prod.get('totalSaleWithShippingAndTax')}")
                print(f"    shippingValuePaidClient: {prod.get('shippingValuePaidClient')}")
                print(f"    valueWithDiscount: {prod.get('valueWithDiscount')}")
                print(f"    valueWithoutDiscount: {prod.get('valueWithoutDiscount')}")
                print(f"    discountValue: {prod.get('discountValue')}")
            break
