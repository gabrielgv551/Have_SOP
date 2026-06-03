"""
Intercepta a requisicao executeRule.do quando o usuario clica em EXPORTAR
no navegador Playwright, captura o payload EXATO, e depois replica.
"""

import os
import json
import csv
from datetime import datetime
from playwright.sync_api import sync_playwright

BASE_URL = "https://amj.gcomweb.com.br/gcomweb"
SYS = "PGC"
USUARIO = "CONSULTORIA1"
SENHA = "1234"
SALVAR_EM = os.getcwd()

captured_request = None

def main():
    global captured_request

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context()
        page = context.new_page()

        # Intercepta executeRule.do
        def handle_route(route, request):
            global captured_request
            if "executeRule.do" in request.url and request.method == "POST":
                post_data = request.post_data
                print(f"\n🎯 executeRule.do INTERCEPTADO!")
                print(f"   URL: {request.url}")
                print(f"   Headers: {json.dumps(dict(request.headers), indent=2)}")
                print(f"   POST data: {post_data}")
                captured_request = {
                    "url": request.url,
                    "headers": dict(request.headers),
                    "post_data": post_data
                }
            route.continue_()

        page.route("**/executeRule.do", handle_route)

        # 1. Login
        print("[1/3] Login...")
        print("      Navegador aberto. Fazendo login automatico...")
        page.goto(f"{BASE_URL}/open.do?sys={SYS}", timeout=60000)
        page.wait_for_timeout(2000)

        login_frame = None
        for frame in page.frames:
            try:
                if frame.is_detached():
                    continue
                if frame.locator("input[type='text']").count() > 0 and frame.locator("input[type='password']").count() > 0:
                    login_frame = frame
                    break
            except Exception:
                continue
        if not login_frame:
            login_frame = page

        login_frame.locator("input[type='text']").first.fill(USUARIO)
        login_frame.locator("input[type='password']").first.fill(SENHA)

        for sel in ["button:has-text('Logar')", "button:has-text('Entrar')", "input[type='submit']"]:
            try:
                btn = login_frame.locator(sel)
                if btn.count() > 0:
                    btn.first.click()
                    break
            except Exception:
                continue

        page.wait_for_timeout(3000)

        # 2. Navega para Agenda Financeira
        print("[2/3] Navegando para Agenda Financeira...")
        for frame in page.frames:
            try:
                if frame.is_detached():
                    continue
                link = frame.locator("text=Agenda Financeira")
                if link.count() > 0:
                    link.first.click()
                    break
            except Exception:
                continue

        print("[3/3] Agora CLIQUE no botao EXPORTAR no navegador!")
        print("      A requisicao sera interceptada automaticamente.")
        print("      Aguardando 60 segundos...")

        # Aguarda o usuario clicar em exportar
        for i in range(60):
            if captured_request:
                print("\n✅ Requisicao capturada!")
                break
            page.wait_for_timeout(1000)

        if captured_request:
            # Salva o payload capturado
            with open(os.path.join(SALVAR_EM, "executeRule_payload.json"), "w") as f:
                json.dump(captured_request, f, indent=2)
            print(f"   Payload salvo em: executeRule_payload.json")

            # Agora replica a requisicao usando os cookies atuais
            import requests
            import urllib3
            urllib3.disable_warnings()

            cookies = context.cookies()
            cookie_dict = {c["name"]: c["value"] for c in cookies}
            jsessionid = cookie_dict.get("JSESSIONID", "")

            session = requests.Session()
            session.verify = False
            session.headers.update(captured_request["headers"])
            session.cookies.set("JSESSIONID", jsessionid, domain="amj.gcomweb.com.br", path="/gcomweb")

            # Converte post_data string para dict
            post_data_dict = {}
            for pair in captured_request["post_data"].split("&"):
                if "=" in pair:
                    k, v = pair.split("=", 1)
                    post_data_dict[k] = v

            r = session.post(captured_request["url"], data=post_data_dict, timeout=60, verify=False)
            print(f"\n   Replicando executeRule...")
            print(f"   status={r.status_code}, len={len(r.text)}")

            # Tenta baixar o CSV
            download_file = r"tmp\CONSULTORIA1\Agenda Financeira - Listagem de Registros Financeiros.csv"
            r2 = session.get(
                f"{BASE_URL}/download",
                params={"download_file": download_file},
                timeout=60, stream=True, verify=False,
            )

            filepath = os.path.join(SALVAR_EM, "contas_a_pagar.csv")
            with open(filepath, "wb") as f:
                for chunk in r2.iter_content(chunk_size=8192):
                    f.write(chunk)

            size_kb = os.path.getsize(filepath) / 1024
            print(f"\n✅ CSV salvo: {filepath} ({size_kb:.1f} KB)")

            if size_kb > 0:
                registros = []
                with open(filepath, "r", encoding="latin-1", errors="replace") as f:
                    reader = csv.DictReader(f, delimiter=";")
                    for row in reader:
                        registros.append(row)
                print(f"   Registros: {len(registros)}")
        else:
            print("\n❌ Nenhuma requisicao capturada")

        browser.close()


if __name__ == "__main__":
    main()
