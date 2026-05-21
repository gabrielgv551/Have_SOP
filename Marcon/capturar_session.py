"""
Captura a sessão autenticada do Preco Certo via Playwright
e testa GET /api/order com os cookies de sessão
"""
import json
import time
import urllib.parse
import requests as req_lib
from playwright.sync_api import sync_playwright

PC_URL = "https://sys.precocerto.co"
EMAIL  = "comercial@casaeletromarcon.com.br"
SENHA  = "eletro123"

requests_capturados = []

def on_request(request):
    if "/api/" in request.url:
        # Capturar cookie header também
        requests_capturados.append({
            "url": request.url,
            "method": request.method,
            "headers": dict(request.headers),
        })

with sync_playwright() as p:
    print("Abrindo browser...")
    browser = p.chromium.launch(headless=False, slow_mo=300)
    context = browser.new_context(
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124",
    )
    page = context.new_page()
    page.on("request", on_request)

    # Login
    print(f"Navegando para {PC_URL}/login/ ...")
    page.goto(f"{PC_URL}/login/", wait_until="networkidle", timeout=30_000)
    page.wait_for_timeout(2000)

    print("Preenchendo formulário...")
    try:
        page.fill("input[name='username_login']", EMAIL, timeout=8_000)
        page.fill("input[name='password_login']", SENHA)
        page.click("button[type='submit']")
        print("  Botão clicado!")
    except Exception as e:
        print(f"  Erro: {e}")

    # Aguardar o redirect para o dashboard (login bem-sucedido)
    print("Aguardando redirect para dashboard...")
    try:
        page.wait_for_url("**/dashboard**", timeout=25_000)
        print(f"  ✅ Redirecionado para: {page.url}")
    except Exception:
        print(f"  URL atual: {page.url}")
    page.wait_for_timeout(3000)

    # Navegar para pedidos explicitamente
    print("\nNavegando para pedidos-de-venda...")
    try:
        page.goto(
            f"{PC_URL}/gerenciar/pedidos-de-venda/"
            f"?source_created=04/05/2026 - 19/05/2026"
            f"&date_after=2026-05-04&date_before=2026-05-19&ordering=-source_created",
            wait_until="domcontentloaded",
            timeout=40_000,
        )
    except Exception as e:
        print(f"  goto parcial: {e}")
    print("Aguardando API calls (15s)...")
    time.sleep(15)

    print(f"\nURL final: {page.url}")

    # Capturar TODOS os cookies do contexto
    all_cookies = context.cookies()
    cookie_dict = {c["name"]: c["value"] for c in all_cookies}

    print(f"\nTodos os cookies ({len(all_cookies)}):")
    for c in all_cookies:
        print(f"  {c['name']}: {str(c['value'])[:80]} (domain={c['domain']}, httpOnly={c.get('httpOnly')})")

    # Pegar token do localStorage
    try:
        token_ls = page.evaluate("() => localStorage.getItem('auth._token.local')")
        print(f"\nLocalStorage auth._token.local: {str(token_ls)[:80]}")
    except Exception as e:
        print(f"localStorage erro: {e}")
        token_ls = None

    # Capturar o cookie header exato de uma request para /api/order
    order_req = next((r for r in requests_capturados if "/api/order" in r["url"] and "token" not in r["url"]), None)
    if order_req:
        print(f"\nRequest /api/order capturado:")
        print(f"  URL: {order_req['url']}")
        cookie_header = order_req["headers"].get("cookie", "")
        auth_header = order_req["headers"].get("authorization", "")
        csrf_header = order_req["headers"].get("x-csrftoken", "")
        print(f"  Cookie header: {cookie_header[:200]}")
        print(f"  Authorization: {auth_header[:80]}")
        print(f"  X-CSRF: {csrf_header[:40]}")

    browser.close()

print("\n" + "="*60)
print("TESTANDO COM COOKIES CAPTURADOS")
print("="*60)

# Construir cookie string com todos os cookies
cookie_str = "; ".join(f"{c['name']}={c['value']}" for c in all_cookies)

# Extrair token do cookie auth._token.local
raw_token_cookie = cookie_dict.get("auth._token.local", "")
if raw_token_cookie:
    token_from_cookie = urllib.parse.unquote(raw_token_cookie).replace("Bearer ", "").strip()
    print(f"\nToken do cookie auth._token.local: {token_from_cookie[:60]}...")
else:
    token_from_cookie = None
    print("\nCookie auth._token.local não encontrado")

# Token do localStorage
if token_ls:
    token_from_ls = token_ls.replace("Bearer ", "").strip()
    print(f"Token do localStorage: {token_from_ls[:60]}...")
else:
    token_from_ls = None

# CSRF token
csrf = cookie_dict.get("csrftoken", "")
print(f"CSRF token: {csrf[:40]}")

# Tentar GET /api/order com cookies de sessão
print("\n--- Teste 1: Apenas cookies (sem Authorization) ---")
r = req_lib.get(
    f"{PC_URL}/api/order",
    headers={
        "Accept": "application/json",
        "Referer": f"{PC_URL}/gerenciar/pedidos-de-venda/",
        "x-csrftoken": csrf,
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124",
        "Cookie": cookie_str,
    },
    params={"ordering": "-source_created", "limit": 5, "source_created": "04/05/2026 - 19/05/2026"},
    timeout=30,
)
print(f"Status: {r.status_code} | Content-Type: {r.headers.get('Content-Type', '')[:40]}")
if r.status_code == 200 and "json" in r.headers.get("Content-Type", ""):
    data = r.json()
    print(f"✅ Sucesso! total={data.get('total')}, rows={len(data.get('rows', []))}")
    if data.get("rows"):
        print(f"Campos: {list(data['rows'][0].keys())}")
else:
    print(f"Erro: {r.text[:200]}")

print("\n--- Teste 2: Cookies + Authorization header ---")
for token in [t for t in [token_from_cookie, token_from_ls] if t and len(t) > 40]:
    r = req_lib.get(
        f"{PC_URL}/api/order",
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Referer": f"{PC_URL}/gerenciar/pedidos-de-venda/",
            "x-csrftoken": csrf,
            "User-Agent": "Mozilla/5.0 Chrome/124",
            "Cookie": cookie_str,
        },
        params={"ordering": "-source_created", "limit": 5},
        timeout=30,
    )
    print(f"Token[:{60}]: {token[:60]}...")
    print(f"Status: {r.status_code}")
    if r.status_code == 200 and "json" in r.headers.get("Content-Type", ""):
        print(f"✅ Sucesso! {r.text[:300]}")
        break
    else:
        print(f"Erro: {r.text[:100]}")

# Mostrar requests capturados para /api/order
print("\n--- Requests /api/order capturados pelo browser ---")
order_reqs = [r for r in requests_capturados if "/api/order" in r["url"] and "token" not in r["url"]]
for req in order_reqs[:5]:
    print(f"\n  {req['method']} {req['url'][:100]}")
    for k, v in req["headers"].items():
        if k.lower() not in ("sec-ch-ua", "sec-ch-ua-mobile", "sec-ch-ua-platform"):
            print(f"    {k}: {str(v)[:80]}")
