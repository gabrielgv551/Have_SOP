"""
Script HÍBRIDO v2 — Playwright faz login e carrega COMPLETAMENTE a Agenda Financeira
(incluindo todos os iframes e regras de inicialização), depois extrai JSESSIONID
e passa para requests que chama executeRule + download.
"""

import requests
import urllib3
from playwright.sync_api import sync_playwright
from datetime import datetime
import os
import paramiko

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BASE_URL   = "https://amj.gcomweb.com.br/gcomweb"
LOGIN      = "CONSULTORIA1"
SENHA      = "1234"
SALVAR_EM  = os.getcwd()
HEADLESS   = False

# Servidor
SERVER_HOST = "37.60.236.200"
SERVER_USER = "root"
SERVER_PASS = "131105Gv"
SERVER_PATH = "/opt/scripts/contas_a_pagar.csv"


def main():
    filepath = None
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()

        # ── 1. Login ──
        print("[1/5] Login...")
        page.goto(f"{BASE_URL}/open.do?sys=PGC", timeout=60000)
        page.wait_for_timeout(2000)

        # Procura frame de login
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

        login_frame.locator("input[type='text']").first.fill(LOGIN)
        login_frame.locator("input[type='password']").first.fill(SENHA)

        for sel in ["button:has-text('Logar')", "button:has-text('Entrar')", "input[type='submit']"]:
            try:
                btn = login_frame.locator(sel)
                if btn.count() > 0:
                    btn.first.click()
                    print(f"      Botão clicado: {sel}")
                    break
            except Exception:
                continue

        page.wait_for_timeout(3000)

        # ── 2. Navega para Agenda Financeira ──
        print("[2/5] Navegando para Agenda Financeira...")
        for frame in page.frames:
            try:
                if frame.is_detached():
                    continue
                link = frame.locator("text=Agenda Financeira")
                if link.count() > 0:
                    link.first.click()
                    print("      Clicado em 'Agenda Financeira'")
                    break
            except Exception:
                continue

        # ── 3. Aguarda TODOS os iframes carregarem completamente ──
        print("[3/5] Aguardando carregamento completo da Agenda...")
        page.wait_for_timeout(5000)

        # Espera mais um pouco para garantir que todas as regras de inicialização rodaram
        print("      Aguardando regras de inicialização...")
        for _ in range(10):
            page.wait_for_timeout(1000)
            # Verifica se ainda há requisições pendentes (aproximadamente)
            # Não há API direta, então só esperamos

        # ── 4. Extrai cookies ──
        print("[4/5] Extraindo cookies...")
        cookies = context.cookies()
        cookie_dict = {c["name"]: c["value"] for c in cookies}
        jsessionid = cookie_dict.get("JSESSIONID", "")
        webrun = cookie_dict.get("WebrunSelectedSystem", "PGC")
        print(f"      JSESSIONID: {jsessionid[:20]}..." if jsessionid else "      ❌ JSESSIONID não encontrado")

        # ── 5. Chama executeRule via requests ──
        print("[5/5] executeRule.do via requests...")
        session = requests.Session()
        session.verify = False
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36",
            "Accept": "application/javascript,*/*;q=0.9",
            "Accept-Language": "pt-BR,pt;q=0.9",
            "Content-Type": "application/x-www-form-urlencoded",
            "Origin": "https://amj.gcomweb.com.br",
            "Referer": f"{BASE_URL}/openform.do?sys=PGC&action=openform&formID=8278&align=0&mode=-1&goto=-1&filter=&scrolling=False&firstLoad=true",
        })
        # Define os cookies manualmente
        session.cookies.set("JSESSIONID", jsessionid, domain="amj.gcomweb.com.br", path="/gcomweb")
        session.cookies.set("WebrunSelectedSystem", webrun, domain="amj.gcomweb.com.br", path="/gcomweb")

        payload = {
            "sys": "PGC",
            "ruleClassName": "br.com.gsa.pgc.agenda.AgendaFinanceiraBean",
            "method": "exportarExcel",
        }

        r = session.post(
            f"{BASE_URL}/executeRule.do",
            data=payload,
            timeout=60,
            verify=False,
        )
        print(f"      status={r.status_code}, len={len(r.text)}")
        if "WFRException" in r.text or "não pode ser acessado" in r.text:
            print("      ❌ executeRule bloqueado")
            browser.close()
            return
        print("      ✅ executeRule aceito")

        # Download
        print("      Baixando CSV...")
        download_file = r"tmp\CONSULTORIA1\Agenda Financeira - Listagem de Registros Financeiros.csv"
        r2 = session.get(
            f"{BASE_URL}/download",
            params={"download_file": download_file},
            timeout=60,
            stream=True,
            verify=False,
        )
        r2.raise_for_status()

        filepath = os.path.join(SALVAR_EM, "contas_a_pagar.csv")
        with open(filepath, "wb") as f:
            for chunk in r2.iter_content(chunk_size=8192):
                f.write(chunk)

        size_kb = os.path.getsize(filepath) / 1024
        print(f"\n✅ Arquivo salvo: {filepath}")
        print(f"   Tamanho: {size_kb:.1f} KB")

        browser.close()

    # ── 6. Envia para servidor ──
    if filepath and os.path.exists(filepath) and os.path.getsize(filepath) > 0:
        print(f"\n[6/6] Enviando para {SERVER_HOST}...")
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(SERVER_HOST, username=SERVER_USER, password=SERVER_PASS, timeout=30)

        sftp = client.open_sftp()
        sftp.put(filepath, SERVER_PATH)
        sftp.close()

        stdin, stdout, stderr = client.exec_command(f"ls -la {SERVER_PATH}")
        print(stdout.read().decode().strip())
        client.close()
        print("\n🚀 PRONTO! CSV esta no servidor.")
    else:
        print("\n❌ CSV vazio, nao enviado.")


if __name__ == "__main__":
    main()
