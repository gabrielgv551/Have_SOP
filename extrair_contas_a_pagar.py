"""
Extrator de Contas a Pagar — GCOM Web
=====================================
Script FINAL e 100% AUTOMÁTICO.

Fluxo:
1. Playwright faz login e navega até a Agenda Financeira
2. Aguarda carregamento completo (iframes + regras de inicialização)
3. Extrai JSESSIONID do navegador
4. Requests chama executeRule.do para gerar o CSV
5. Requests baixa o CSV

Requisitos: playwright, requests
    pip install playwright requests
    playwright install chromium

Autor: Have Sistemas
"""

import os
from datetime import datetime
import requests
import urllib3
from playwright.sync_api import sync_playwright

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ─── CONFIGURAÇÕES ─────────────────────────────────────────────────────────
BASE_URL = "https://amj.gcomweb.com.br/gcomweb"
SYS = "PGC"
USUARIO = "CONSULTORIA1"
SENHA = "1234"
SALVAR_EM = os.getcwd()
HEADLESS = True  # True = roda em background | False = mostra navegador


def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS)
        context = browser.new_context()
        page = context.new_page()

        # ── 1. Login ──
        print("[1/5] Login...")
        page.goto(f"{BASE_URL}/open.do?sys={SYS}", timeout=60000)
        page.wait_for_timeout(2000)

        # Localiza o frame de login
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

        # ── 2. Navega para Agenda Financeira ──
        print("[2/5] Navegando para Agenda Financeira...")
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

        # ── 3. Aguarda carregamento completo ──
        print("[3/5] Aguardando carregamento completo...")
        page.wait_for_timeout(5000)
        for _ in range(10):
            page.wait_for_timeout(1000)

        # ── 4. Extrai cookies ──
        print("[4/5] Extraindo sessão...")
        cookies = context.cookies()
        cookie_dict = {c["name"]: c["value"] for c in cookies}
        jsessionid = cookie_dict.get("JSESSIONID", "")
        webrun = cookie_dict.get("WebrunSelectedSystem", "PGC")

        if not jsessionid:
            print("❌ Falhou: JSESSIONID não encontrado")
            browser.close()
            return

        # ── 5. executeRule + Download via requests ──
        print("[5/5] Gerando e baixando CSV...")
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
        session.cookies.set("JSESSIONID", jsessionid, domain="amj.gcomweb.com.br", path="/gcomweb")
        session.cookies.set("WebrunSelectedSystem", webrun, domain="amj.gcomweb.com.br", path="/gcomweb")

        # Dispara o executeRule para gerar o CSV
        r = session.post(
            f"{BASE_URL}/executeRule.do",
            data={
                "sys": "PGC",
                "ruleClassName": "br.com.gsa.pgc.agenda.AgendaFinanceiraBean",
                "method": "exportarExcel",
            },
            timeout=60,
            verify=False,
        )
        if "WFRException" in r.text or "não pode ser acessado" in r.text:
            print("❌ executeRule bloqueado")
            browser.close()
            return

        # Baixa o CSV
        download_file = r"tmp\CONSULTORIA1\Agenda Financeira - Listagem de Registros Financeiros.csv"
        r2 = session.get(
            f"{BASE_URL}/download",
            params={"download_file": download_file},
            timeout=60,
            stream=True,
            verify=False,
        )
        r2.raise_for_status()

        # Salva com nome fixo para facilitar leitura no N8N
        filepath = os.path.join(SALVAR_EM, "contas_a_pagar.csv")
        with open(filepath, "wb") as f:
            for chunk in r2.iter_content(chunk_size=8192):
                f.write(chunk)

        size_kb = os.path.getsize(filepath) / 1024
        print(f"\n✅ Arquivo salvo: {filepath}")
        print(f"   Tamanho: {size_kb:.1f} KB")

        # Le o CSV e imprime como JSON para o N8N capturar
        import csv
        import json
        registros = []
        with open(filepath, "r", encoding="latin-1", errors="replace") as f:
            reader = csv.DictReader(f, delimiter=";")
            for row in reader:
                registros.append(row)

        print("\n---JSON_START---")
        print(json.dumps(registros, ensure_ascii=False))
        print("---JSON_END---")

        browser.close()


if __name__ == "__main__":
    main()
