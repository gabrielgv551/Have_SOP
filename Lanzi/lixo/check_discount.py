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

# Calcular somas
sum_total_sale = 0
sum_total_sale_with_shipping = 0
sum_value_with_discount = 0
sum_value_without_discount = 0
sum_discount_value = 0
sum_shipping_paid_client = 0
sum_pedido_total_sale = 0
pedidos_vistos = set()

for pedido in data.get("result", []):
    pedido_id = pedido.get("id")
    pedido_total = pedido.get("totalSale", 0) or 0
    if pedido_id not in pedidos_vistos:
        sum_pedido_total_sale += pedido_total
        pedidos_vistos.add(pedido_id)
    
    for prod in pedido.get("products", []):
        sum_total_sale += prod.get("totalSale", 0) or 0
        tss = prod.get("totalSaleWithShippingAndTax", 0)
        if tss:
            sum_total_sale_with_shipping += tss
        sum_value_with_discount += prod.get("valueWithDiscount", 0) or 0
        sum_value_without_discount += prod.get("valueWithoutDiscount", 0) or 0
        sum_discount_value += prod.get("discountValue", 0) or 0
        sum_shipping_paid_client += prod.get("shippingValuePaidClient", 0) or 0

print(f"Soma produto.totalSale: {sum_total_sale:,.2f}")
print(f"Soma produto.totalSaleWithShippingAndTax: {sum_total_sale_with_shipping:,.2f}")
print(f"Soma produto.valueWithDiscount: {sum_value_with_discount:,.2f}")
print(f"Soma produto.valueWithoutDiscount: {sum_value_without_discount:,.2f}")
print(f"Soma produto.discountValue: {sum_discount_value:,.2f}")
print(f"Soma produto.shippingValuePaidClient: {sum_shipping_paid_client:,.2f}")
print(f"Soma pedido.totalSale (unico): {sum_pedido_total_sale:,.2f}")
print(f"\nAlvo planilha: 1,392,531.86")
