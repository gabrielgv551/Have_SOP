"""
Navega até a página de pedidos com filtro de fevereiro e clica automaticamente
no botão de exportar para capturar o endpoint de API.
"""
import json, time
from playwright.sync_api import sync_playwright

PC_URL   = "https://sys.precocerto.co"
PC_EMAIL = "comercial@casaeletromarcon.com.br"
PC_SENHA = "eletro123"

def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, slow_mo=500)
        ctx     = browser.new_context()
        page    = ctx.new_page()

        captured = []
        def on_request(req):
            if "precocerto.co" in req.url and req.method in ("POST", "GET"):
                if any(x in req.url for x in ["/api/", "/gerenciar/"]):
                    body = ""
                    try: body = req.post_data or ""
                    except: pass
                    captured.append({"method": req.method, "url": req.url, "body": body[:200]})
                    print(f"[{req.method}] {req.url[:100]}")
                    if body:
                        print(f"  body: {body[:150]}")

        page.on("request", on_request)

        # Login
        page.goto(f"{PC_URL}/login/")
        page.wait_for_timeout(1500)
        page.fill("input[name='username_login']", PC_EMAIL)
        page.fill("input[name='password_login']", PC_SENHA)
        page.click("button[type='submit']")
        page.wait_for_url("**/dashboard**", timeout=30_000)
        print("Logado!")

        # Navegar direto para pedidos com filtro de fev 2025
        url = (f"{PC_URL}/gerenciar/pedidos-de-venda/"
               "?source_created=01%2F02%2F2025%20-%2028%2F02%2F2025"
               "&date_before=2025-02-28&date_after=2025-02-01&ordering=-source_created")
        page.goto(url)
        page.wait_for_timeout(4000)
        print(f"Na página: {page.url}")

        # Procurar o botão de exportar por texto ou ícone
        print("\nProcurando botão de exportar...")
        export_btn = None
        selectors = [
            "button:has-text('Exportar')",
            "button:has-text('EXPORTAR')",
            "button:has-text('Export')",
            "a:has-text('Exportar Planilha')",
            "a:has-text('EXPORTAR PLANILHA')",
            "[title*='xportar']",
            "[aria-label*='xportar']",
            ".cloud-download",
            "[class*='export']",
        ]
        for sel in selectors:
            try:
                btn = page.locator(sel).first
                if btn.count() > 0:
                    print(f"Botão encontrado: {sel}")
                    export_btn = btn
                    break
            except:
                pass

        if export_btn:
            print("Clicando no botão de exportar...")
            export_btn.click()
            page.wait_for_timeout(3000)
        else:
            print("Botão não encontrado automaticamente. Feche o browser manualmente após clicar.")
            print("Aguardando 60s...")
            page.wait_for_timeout(60000)

        browser.close()

        # Mostrar apenas requests relevantes
        print("\n=== REQUESTS CAPTURADOS (POST ou com export) ===")
        for c in captured:
            url = c["url"]
            if c["method"] == "POST" or any(x in url.lower() for x in ["export", "sheet", "task"]):
                print(json.dumps(c, ensure_ascii=False, indent=2))

        with open("export_requests.json", "w", encoding="utf-8") as f:
            json.dump(captured, f, ensure_ascii=False, indent=2)
        print("\nSalvo em export_requests.json")

if __name__ == "__main__":
    main()
