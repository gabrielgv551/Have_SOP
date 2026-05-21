"""
Inspeciona os campos disponíveis no /api/orderline do Preco Certo.
Pega apenas 1 pedido e imprime todas as chaves e valores.
"""
import os, json, requests
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

load_dotenv()

PC_URL   = "https://sys.precocerto.co"
PC_EMAIL = os.getenv("PRECOCERTO_EMAIL",    "comercial@casaeletromarcon.com.br")
PC_SENHA = os.getenv("PRECOCERTO_PASSWORD", "eletro123")

# Auth
with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    ctx = browser.new_context(locale="pt-BR")
    page = ctx.new_page()
    page.goto(f"{PC_URL}/login/", wait_until="networkidle", timeout=40_000)
    page.wait_for_timeout(1500)
    page.fill("input[name='username_login']", PC_EMAIL)
    page.fill("input[name='password_login']", PC_SENHA)
    page.click("button[type='submit']")
    try: page.wait_for_url("**/dashboard**", timeout=25_000)
    except: pass
    page.wait_for_timeout(2000)
    cookies = ctx.cookies(urls=[PC_URL])
    browser.close()

cookie_dict = {c["name"]: c["value"] for c in cookies}
headers = {
    "Accept": "application/json",
    "x-csrftoken": cookie_dict.get("csrftoken", ""),
    "Cookie": "; ".join(f"{k}={v}" for k, v in cookie_dict.items()),
}

# 1. Buscar 1 order recente
print("\n=== Buscando 1 order recente (Mai/2026) ===")
r0 = requests.get(f"{PC_URL}/api/order", headers=headers, params={
    "source_created": "01/05/2026 - 21/05/2026",
    "limit": 1, "offset": 0,
}, timeout=60)
rows0 = r0.json().get("rows", [])
if not rows0:
    print("Nenhum order encontrado")
    exit()
order = rows0[0]
order_id = order.get("id")
print(f"  order id={order_id}, number={order.get('number')}, total={order.get('total')}")

# 2. Detalhe individual do order
print(f"\n=== GET /api/order/{order_id}/ (detalhe) ===")
r1 = requests.get(f"{PC_URL}/api/order/{order_id}/", headers=headers, timeout=60)
print(f"Status: {r1.status_code}")
if r1.status_code == 200:
    det = r1.json()
    for k, v in sorted(det.items()):
        print(f"  {k:50s} = {repr(v)[:120]}")
else:
    print(r1.text[:500])

# 3. Buscar channel info
channel = order.get("channel", {})
ch_id = channel.get("id") if isinstance(channel, dict) else None
if ch_id:
    print(f"\n=== GET /api/channel/{ch_id}/ ===")
    r2 = requests.get(f"{PC_URL}/api/channel/{ch_id}/", headers=headers, timeout=60)
    print(f"Status: {r2.status_code}")
    if r2.status_code == 200:
        ch = r2.json()
        for k, v in sorted(ch.items()):
            print(f"  {k:50s} = {repr(v)[:120]}")
    else:
        print(r2.text[:300])

# Busca as orderlines do pedido 72346
print("\n=== /api/orderline → number=72346 ===")
r2 = requests.get(f"{PC_URL}/api/orderline", headers=headers, params={
    "number": "72346", "limit": 20, "offset": 0,
}, timeout=60)
print(f"Status: {r2.status_code}")
data2 = r2.json()
rows2 = data2.get("rows", [])
if rows2:
    for i, row in enumerate(rows2):
        print(f"\n  -- Linha {i+1} --")
        for k, v in sorted(row.items()):
            print(f"  {k:45s} = {v}")
else:
    print("Nenhum resultado em /api/orderline")
