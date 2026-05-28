"""
Abre o Preço Certo, navega até a tela de pedidos e intercepta
todas as requisições de rede para descobrir o endpoint de export.
"""
import json, time
from playwright.sync_api import sync_playwright

PC_URL   = "https://sys.precocerto.co"
PC_EMAIL = "comercial@casaeletromarcon.com.br"
PC_SENHA = "eletro123"

captured = []

def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        ctx     = browser.new_context()
        page    = ctx.new_page()

        # Interceptar TODAS as requisições da API do Preço Certo
        def on_request(req):
            if "precocerto.co" in req.url and req.url != page.url:
                post_data = ""
                try:
                    post_data = req.post_data or ""
                except:
                    pass
                info = {"method": req.method, "url": req.url}
                if post_data:
                    info["body"] = post_data
                print(f"[REQ] {req.method} {req.url}")
                if post_data:
                    print(f"  body: {post_data[:200]}")
                captured.append(info)

        def on_response(resp):
            if "precocerto.co" in resp.url:
                ct = resp.headers.get("content-type", "")
                print(f"[RES] {resp.status} {resp.url[:100]} | {ct[:50]}")
                captured.append({"type": "response", "status": resp.status, "url": resp.url, "content_type": ct})

        page.on("request",  on_request)
        page.on("response", on_response)

        # Login
        print("Fazendo login...")
        page.goto(f"{PC_URL}/login/")
        page.wait_for_timeout(2000)
        page.fill("input[name='username_login']", PC_EMAIL)
        page.fill("input[name='password_login']", PC_SENHA)
        page.click("button[type='submit']")
        page.wait_for_url("**/dashboard**", timeout=30_000)
        print(f"Logado! URL: {page.url}")

        # Navegar para a página de pedidos
        print("\nAbrindo página de pedidos...")
        page.goto(f"{PC_URL}/v2/orders/")
        page.wait_for_timeout(3000)
        print(f"URL: {page.url}")

        print("\n=== INSTRUÇÕES ===")
        print("1. No browser, filtre por fevereiro 2025")
        print("2. Clique no botão de EXPORTAR/DOWNLOAD (ícone de planilha)")
        print("3. Aguarde — a requisição será capturada aqui")
        print("4. Feche o browser quando terminar")
        print("==================\n")

        # Aguardar o usuário interagir
        try:
            page.wait_for_event("close", timeout=120_000)
        except Exception:
            pass

        browser.close()

    if captured:
        print("\n=== REQUISIÇÕES CAPTURADAS ===")
        for c in captured:
            print(json.dumps(c, indent=2, ensure_ascii=False))
        with open("export_discovery.json", "w", encoding="utf-8") as f:
            json.dump(captured, f, indent=2, ensure_ascii=False)
        print("\nSalvo em export_discovery.json")
    else:
        print("\nNenhuma requisição de export capturada.")

if __name__ == "__main__":
    main()
