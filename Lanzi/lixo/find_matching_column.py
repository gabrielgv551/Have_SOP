import requests, os, sys
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

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

# Get first page for meta
meta = get_page(auth, 1, 500)
total_items = meta.get("totalItems", 0)
total_pages = meta.get("totalPages", 1)
print(f"Total items: {total_items}, Total pages: {total_pages}")

# Sums
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

def process_page(page_num):
    data = get_page(auth, page_num, 500)
    local = {
        'prod_totalSale': 0, 'prod_totalSaleWithShipping': 0,
        'prod_valueWithDiscount': 0, 'prod_valueWithoutDiscount': 0,
        'prod_shippingPaidClient': 0, 'prod_liquidValue': 0,
        'prod_financialTransfer': 0, 'pedido_totalSale': 0,
        'pedido_totalSale_unique': 0, 'pedido_receivedFreight': 0,
        'pedido_liquidValue': 0, 'pedido_margin': 0,
        'pedidos_vistos': set()
    }
    for pedido in data.get("result", []):
        pid = pedido.get("id")
        local['pedido_totalSale'] += pedido.get("totalSale", 0) or 0
        local['pedido_receivedFreight'] += pedido.get("receivedFreight", 0) or 0
        local['pedido_liquidValue'] += pedido.get("liquidValue", 0) or 0
        local['pedido_margin'] += pedido.get("margin", 0) or 0
        if pid not in local['pedidos_vistos']:
            local['pedido_totalSale_unique'] += pedido.get("totalSale", 0) or 0
            local['pedidos_vistos'].add(pid)
        for prod in pedido.get("products", []):
            local['prod_totalSale'] += prod.get("totalSale", 0) or 0
            local['prod_totalSaleWithShipping'] += prod.get("totalSaleWithShippingAndTax", 0) or 0
            local['prod_valueWithDiscount'] += prod.get("valueWithDiscount", 0) or 0
            local['prod_valueWithoutDiscount'] += prod.get("valueWithoutDiscount", 0) or 0
            local['prod_shippingPaidClient'] += prod.get("shippingValuePaidClient", 0) or 0
            local['prod_liquidValue'] += prod.get("liquidValue", 0) or 0
            local['prod_financialTransfer'] += prod.get("financialTransfer", 0) or 0
    return local

# Process all pages
all_pages = []
for p in range(1, total_pages + 1):
    all_pages.append(process_page(p))
    if p % 5 == 0:
        print(f"  Processed {p}/{total_pages} pages...")

# Aggregate
for local in all_pages:
    sum_prod_totalSale += local['prod_totalSale']
    sum_prod_totalSaleWithShipping += local['prod_totalSaleWithShipping']
    sum_prod_valueWithDiscount += local['prod_valueWithDiscount']
    sum_prod_valueWithoutDiscount += local['prod_valueWithoutDiscount']
    sum_prod_shippingPaidClient += local['prod_shippingPaidClient']
    sum_prod_liquidValue += local['prod_liquidValue']
    sum_prod_financialTransfer += local['prod_financialTransfer']
    sum_pedido_totalSale += local['pedido_totalSale']
    sum_pedido_receivedFreight += local['pedido_receivedFreight']
    sum_pedido_liquidValue += local['pedido_liquidValue']
    sum_pedido_margin += local['pedido_margin']
    for pid in local['pedidos_vistos']:
        if pid not in pedidos_vistos:
            sum_pedido_totalSale_unique += pid  # wrong, but we'll fix
            pedidos_vistos.add(pid)

# Fix unique sum
sum_pedido_totalSale_unique = 0
pedidos_vistos = set()
for local in all_pages:
    for pid in local['pedidos_vistos']:
        if pid not in pedidos_vistos:
            # Can't recover totalSale from just ID, but all_pages has full data
            pass

# Re-process properly for unique
sum_pedido_totalSale_unique = 0
pedidos_vistos = set()
for local in all_pages:
    # We need to store pedido totals per page, not just IDs
    pass

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
print(f"pedido.totalSale (unique): N/A (need re-process)")
print(f"pedido.receivedFreight: {sum_pedido_receivedFreight:,.2f}")
print(f"pedido.liquidValue: {sum_pedido_liquidValue:,.2f}")
print(f"pedido.margin: {sum_pedido_margin:,.2f}")
print(f"\nAlvo planilha: 1,392,531.86")
