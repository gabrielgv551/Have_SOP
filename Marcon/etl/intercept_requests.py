from playwright.sync_api import sync_playwright
import time
import json

PC_URL = "https://sys.precocerto.co"
PC_EMAIL = "comercial@casaeletromarcon.com.br"
PC_SENHA = "eletro123"

def run():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124",
            locale="pt-BR",
            viewport={"width": 1280, "height": 720}
        )
        page = context.new_page()

        # Listen for all requests
        def handle_request(request):
            if "api" in request.url or "order" in request.url or "export" in request.url:
                print(f"[REQ] {request.method} {request.url}")
        
        def handle_response(response):
            if "api" in response.url or "order" in response.url or "export" in response.url:
                print(f"[RES] {response.status} {response.url}")

        page.on("request", handle_request)
        page.on("response", handle_response)

        print("Fazendo login...")
        page.goto(f"{PC_URL}/login/", wait_until="domcontentloaded")
        page.wait_for_timeout(2000)
        page.fill("input[name='username_login']", PC_EMAIL)
        page.fill("input[name='password_login']", PC_SENHA)
        page.click("button[type='submit']")
        
        print("Aguardando carregamento da dashboard...")
        page.wait_for_timeout(5000)
        print("URL atual:", page.url)

        print("Navegando para a página de pedidos...")
        page.goto(f"{PC_URL}/v2/orders", wait_until="networkidle")
        page.wait_for_timeout(5000)
        
        # Tentar clicar em algum botão de exportar se existir
        print("Procurando botão de exportar...")
        try:
            page.click("button:has-text('Exportar')", timeout=3000)
            page.wait_for_timeout(3000)
        except:
            print("Botão Exportar não encontrado")

        browser.close()

if __name__ == "__main__":
    run()
