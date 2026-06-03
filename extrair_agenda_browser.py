"""
Extrator de Contas a Pagar — via JavaScript no navegador
========================================================
Faz login com Playwright, navega ate a Agenda Financeira,
depois chama executeRule.do DENTRO do navegador (fetch API)
para manter todo o contexto de cookies/sessao.
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
HEADLESS = False


def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()

        # 1. Login
        print("[1/4] Login...")
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
        print("[2/4] Navegando para Agenda Financeira...")
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

        # 3. Aguarda carregamento
        print("[3/4] Aguardando carregamento completo...")
        page.wait_for_timeout(10000)
        for _ in range(15):
            page.wait_for_timeout(1000)

        # 4. Chama executeRule via JavaScript dentro do navegador
        print("[4/4] Chamando executeRule via JavaScript no navegador...")

        js_code = """
        async () => {
            const formData = new URLSearchParams();
            formData.append('sys', 'PGC');
            formData.append('ruleClassName', 'br.com.gsa.pgc.agenda.AgendaFinanceiraBean');
            formData.append('method', 'exportarExcel');

            const response = await fetch('/gcomweb/executeRule.do', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/x-www-form-urlencoded',
                    'Accept': 'application/javascript,*/*;q=0.9',
                },
                body: formData.toString(),
                credentials: 'include'
            });

            const text = await response.text();
            return { status: response.status, body: text.substring(0, 500) };
        }
        """

        result = page.evaluate(js_code)
        print(f"      status={result['status']}, body={result['body'][:200]}")

        if "WFRException" in result['body'] or "não pode ser acessado" in result['body']:
            print("      executeRule bloqueado")
            browser.close()
            return

        print("      executeRule aceito")

        # 5. Baixa o CSV via JavaScript
        print("      Baixando CSV...")
        js_download = """
        async () => {
            const downloadFile = 'tmp/CONSULTORIA1/Agenda Financeira - Listagem de Registros Financeiros.csv';
            const response = await fetch('/gcomweb/download?download_file=' + encodeURIComponent(downloadFile), {
                method: 'GET',
                credentials: 'include'
            });

            if (!response.ok) {
                return { ok: false, status: response.status, size: 0 };
            }

            const blob = await response.blob();
            return { ok: true, status: response.status, size: blob.size };
        }
        """

        dl_result = page.evaluate(js_download)
        print(f"      download status={dl_result['status']}, size={dl_result['size']}")

        if dl_result['ok'] and dl_result['size'] > 0:
            # Baixa o conteudo real como array de bytes
            js_get_bytes = """
            async () => {
                const downloadFile = 'tmp/CONSULTORIA1/Agenda Financeira - Listagem de Registros Financeiros.csv';
                const response = await fetch('/gcomweb/download?download_file=' + encodeURIComponent(downloadFile), {
                    method: 'GET',
                    credentials: 'include'
                });
                const buffer = await response.arrayBuffer();
                const bytes = new Uint8Array(buffer);
                return Array.from(bytes);
            }
            """

            bytes_list = page.evaluate(js_get_bytes)
            filepath = os.path.join(SALVAR_EM, "contas_a_pagar.csv")
            with open(filepath, "wb") as f:
                f.write(bytes(bytes_list))

            size_kb = os.path.getsize(filepath) / 1024
            print(f"\n✅ Arquivo salvo: {filepath}")
            print(f"   Tamanho: {size_kb:.1f} KB")

            # Le como JSON
            registros = []
            with open(filepath, "r", encoding="latin-1", errors="replace") as f:
                reader = csv.DictReader(f, delimiter=";")
                for row in reader:
                    registros.append(row)

            print(f"   Registros: {len(registros)}")
        else:
            print("\n❌ Download falhou ou veio vazio")

        browser.close()


if __name__ == "__main__":
    main()
