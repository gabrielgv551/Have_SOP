import requests, os
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

def get_page(auth, page, page_size=500):
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
meta = get_page(auth, 1, 500)
total_pages = meta.get("totalPages", 1)
print(f"Total pages: {total_pages}")

# Accumulators
sum_prod_totalSale = 0
sum_prod_totalSaleWithShipping = 0
sum_prod_valueWithDiscount = 0
sum_prod_valueWithoutDiscount = 0
sum_prod_shippingPaidClient = 0
sum_prod_liquidValue = 0
sum_prod_financialTransfer = 0
sum_pedido_totalSale = 0
sum_pedido_totalSale_unique = 0
sum_pedido_receivedFreight = 0
sum_pedido_liquidValue = 0
sum_pedido_margin = 0
pedidos_vistos = set()

for p in range(1, total_pages + 1):
    data = get_page(auth, p, 500)
    for pedido in data.get("result", []):
        pid = pedido.get("id")
        sum_pedido_totalSale += pedido.get("totalSale", 0) or 0
        sum_pedido_receivedFreight += pedido.get("receivedFreight", 0) or 0
        sum_pedido_liquidValue += pedido.get("liquidValue", 0) or 0
        sum_pedido_margin += pedido.get("margin", 0) or 0
        if pid not in pedidos_vistos:
            sum_pedido_totalSale_unique += pedido.get("totalSale", 0) or 0
            pedidos_vistos.add(pid)
        for prod in pedido.get("products", []):
            sum_prod_totalSale += prod.get("totalSale", 0) or 0
            sum_prod_totalSaleWithShipping += prod.get("totalSaleWithShippingAndTax", 0) or 0
            sum_prod_valueWithDiscount += prod.get("valueWithDiscount", 0) or 0
            sum_prod_valueWithoutDiscount += prod.get("valueWithoutDiscount", 0) or 0
            sum_prod_shippingPaidClient += prod.get("shippingValuePaidClient", 0) or 0
            sum_prod_liquidValue += prod.get("liquidValue", 0) or 0
            sum_prod_financialTransfer += prod.get("financialTransfer", 0) or 0
    if p % 5 == 0:
        print(f"  Page {p}/{total_pages} done")

print("\n=== RESULTS ===")
print(f"prod.totalSale: {sum_prod_totalSale:,.2f}")
print(f"prod.totalSaleWithShippingAndTax: {sum_prod_totalSaleWithShipping:,.2f}")
print(f"prod.valueWithDiscount: {sum_prod_valueWithDiscount:,.2f}")
print(f"prod.valueWithoutDiscount: {sum_prod_valueWithoutDiscount:,.2f}")
print(f"prod.shippingValuePaidClient: {sum_prod_shippingPaidClient:,.2f}")
print(f"prod.valueWithoutDiscount + shipping: {sum_prod_valueWithoutDiscount + sum_prod_shippingPaidClient:,.2f}")
print(f"prod.liquidValue: {sum_prod_liquidValue:,.2f}")
print(f"prod.financialTransfer: {sum_prod_financialTransfer:,.2f}")
print(f"pedido.totalSale (all lines): {sum_pedido_totalSale:,.2f}")
print(f"pedido.totalSale (unique): {sum_pedido_totalSale_unique:,.2f}")
print(f"pedido.receivedFreight: {sum_pedido_receivedFreight:,.2f}")
print(f"pedido.liquidValue: {sum_pedido_liquidValue:,.2f}")
print(f"pedido.margin: {sum_pedido_margin:,.2f}")
print(f"\nAlvo planilha: 1,392,531.86")
