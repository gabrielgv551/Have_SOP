"""
Roda NO WINDOWS — abre o Chrome, você faz login no Upseller,
e salva o token de autenticação em upseller_token.json

Execute:
  pip install playwright && playwright install chromium
  python get_upseller_token.py
"""
import json, time
from pathlib import Path
from playwright.sync_api import sync_playwright

LOGIN_URL   = "https://app.upseller.com/pt/login"
TOKEN_FILE  = Path(__file__).parent.parent / "upseller_token.json"

def run():
    captured = {}

    def intercept(response):
        if "/api/auth/login" in response.url and response.status in (200, 201):
            try:
                body = response.json()
                if body.get("code") != 2001:
                    captured["auth"] = body
                    captured["url"]  = response.url
            except Exception:
                pass

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=False, args=["--start-maximized"])
        ctx     = browser.new_context(viewport={"width": 1280, "height": 800})
        page    = ctx.new_page()
        page.on("response", intercept)

        print("\n[!] Abrindo Chrome...")
        print("    Faça login no Upseller normalmente.")
        print("    O script detectará o token automaticamente.\n")
        page.goto(LOGIN_URL)

        # Aguarda até 3 minutos
        for _ in range(180):
            time.sleep(1)
            if "login" not in page.url.lower() and captured.get("auth"):
                break
            if "login" not in page.url.lower() and not captured.get("auth"):
                # Tenta capturar cookies da sessão
                cookies = ctx.cookies()
                captured["cookies"] = {c["name"]: c["value"] for c in cookies}
                break

        if not captured:
            print("[!] Timeout — tente novamente.")
            browser.close()
            return

        TOKEN_FILE.write_text(json.dumps(captured, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"[OK] Token salvo em: {TOKEN_FILE}")

        browser.close()

if __name__ == "__main__":
    run()
