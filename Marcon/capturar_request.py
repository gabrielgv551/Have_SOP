"""
Abre o browser VISÍVEL, loga no Preco Certo e captura o request exato
que a página /gerenciar/pedidos-de-venda/ faz ao /api/order
(headers, cookies, params, etc.) para reproduzir via requests
"""
import json
import time
import requests as req_lib
from playwright.sync_api import sync_playwright

PC_URL = "https://sys.precocerto.co"
EMAIL  = "comercial@casaeletromarcon.com.br"
SENHA  = "eletro123"

api_request_capturado = {}

def on_request(request):
    if "/api/order" in request.url and "token" not in request.url:
        api_request_capturado["url"]     = request.url
        api_request_capturado["headers"] = dict(request.headers)
        api_request_capturado["method"]  = request.method
        print(f"\n  [REQUEST CAPTURADO] {request.method} {request.url}")
        print(f"  Headers: {json.dumps(dict(request.headers), indent=2)}")

def on_response(response):
    if "/api/order" in response.url and "token" not in response.url:
        ct = response.headers.get("content-type", "")
        print(f"\n  [RESPONSE] {response.status} {response.url[:80]}")
        print(f"  Content-Type: {ct}")
        if "json" in ct:
            try:
                data = response.json()
                print(f"  Data: total={data.get('total')}, rows={len(data.get('rows', []))}")
                if data.get("rows"):
                    print(f"  Campos: {list(data['rows'][0].keys())}")
                api_request_capturado["response_ok"] = True
                api_request_capturado["response_data"] = data
            except Exception as e:
                print(f"  Erro ao parsear JSON: {e}")

with sync_playwright() as p:
    print("Abrindo Chrome visível...")
    browser = p.chromium.launch(headless=False, slow_mo=500)
    context = browser.new_context(
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124",
    )
    page = context.new_page()
    page.on("request", on_request)
    page.on("response", on_response)

    # 1. Ir para login
    print(f"\n[1] Navegando para {PC_URL}/login/ ...")
    page.goto(f"{PC_URL}/login/", wait_until="networkidle", timeout=30_000)
    page.wait_for_timeout(2000)

    # 2. Preencher credenciais
    print("[2] Preenchendo formulário...")
    try:
        page.fill("input[name='username_login']", EMAIL, timeout=10_000)
        page.fill("input[name='password_login']", SENHA)
        print("   Campos preenchidos!")
        
        # Tirar screenshot para ver o estado
        page.screenshot(path="login_preenchido.png")
        print("   Screenshot: login_preenchido.png")
        
        # Clicar no botão de submit
        page.click("button[type='submit']")
        print("   Botão clicado!")
    except Exception as e:
        print(f"   Erro: {e}")
        # Mostrar todos os inputs disponíveis
        inputs = page.query_selector_all("input")
        print(f"   Inputs disponíveis: {len(inputs)}")
        for inp in inputs:
            name = inp.get_attribute("name")
            type_ = inp.get_attribute("type")
            id_ = inp.get_attribute("id")
            print(f"     input name={name}, type={type_}, id={id_}")

    # 3. Aguardar login
    print("\n[3] Aguardando login (15s)...")
    time.sleep(15)
    
    current_url = page.url
    print(f"   URL atual: {current_url}")
    page.screenshot(path="pos_login.png")
    print("   Screenshot: pos_login.png")
    
    # Token do localStorage
    try:
        token_ls = page.evaluate("() => localStorage.getItem('auth._token.local')")
        print(f"   Token localStorage: {str(token_ls)[:60]}")
    except Exception as e:
        print(f"   localStorage erro: {e}")

    if "/login" not in current_url:
        # 4. Navegar para pedidos
        print("\n[4] Navegando para pedidos-de-venda...")
        page.goto(
            f"{PC_URL}/gerenciar/pedidos-de-venda/"
            f"?source_created=04/05/2026 - 19/05/2026"
            f"&date_after=2026-05-04&date_before=2026-05-19&ordering=-source_created",
            wait_until="domcontentloaded",
            timeout=30_000,
        )
        print("   Aguardando carregamento das requests (20s)...")
        time.sleep(20)
        page.screenshot(path="pedidos_page.png")
        print("   Screenshot: pedidos_page.png")
    else:
        print("\n   ❌ Login não funcionou - ainda em /login/")
        # Tentar clicar em elementos visíveis
        print("   Procurando elementos de login...")
        all_btns = page.query_selector_all("button")
        for btn in all_btns:
            txt = btn.inner_text()
            print(f"     Button: '{txt}'")

    browser.close()

print("\n" + "="*60)
if api_request_capturado:
    print("REQUEST CAPTURADO:")
    print(json.dumps({
        "url": api_request_capturado.get("url"),
        "headers": api_request_capturado.get("headers"),
    }, indent=2))
    
    if api_request_capturado.get("response_ok"):
        print("\n✅ API respondeu com sucesso!")
        
    # Testar os mesmos headers via requests
    if api_request_capturado.get("headers"):
        print("\nTestando headers capturados via requests...")
        headers = api_request_capturado["headers"]
        # Remover headers problemáticos para requests
        for h in [":method", ":path", ":scheme", ":authority", "content-length"]:
            headers.pop(h, None)
        r = req_lib.get(
            f"{PC_URL}/api/order",
            headers=headers,
            params={"ordering": "-source_created", "limit": 5},
            timeout=30,
        )
        print(f"Status com headers do browser: {r.status_code}")
        ct = r.headers.get("Content-Type", "")
        if "json" in ct:
            print(r.text[:400])
else:
    print("Nenhum request para /api/order foi capturado")
