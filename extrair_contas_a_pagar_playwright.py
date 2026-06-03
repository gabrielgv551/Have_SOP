"""
Extrator de Contas a Pagar — 100% Playwright
=============================================
Faz login, navega ate a Agenda Financeira, clica no botao EXPORTAR
automaticamente e baixa o CSV pelo navegador (sem requests).
"""

import os
import sys
import csv
import json
from datetime import datetime
from playwright.sync_api import sync_playwright

BASE_URL = "https://amj.gcomweb.com.br/gcomweb"
SYS = "PGC"
USUARIO = "CONSULTORIA1"
SENHA = "1234"
SALVAR_EM = os.getcwd()
HEADLESS = False  # False = navegador visivel (mais confiavel)


def main():
    download_path = None

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()

        # Intercepta download
        def handle_download(download):
            nonlocal download_path
            fn = download.suggested_filename
            print(f"      Download detectado: {fn}")
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            download_path = os.path.join(SALVAR_EM, f"contas_a_pagar_{ts}.csv")
            download.save_as(download_path)
            print(f"      Salvo em: {download_path}")

        page.on("download", handle_download)

        # 1. Login
        print("[1/5] Login...")
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

        # 3. Aguarda carregamento completo
        print("[3/5] Aguardando carregamento completo...")
        page.wait_for_timeout(8000)

        # 3.5. Clica em PESQUISAR para carregar os dados
        print("[3.5/5] Clicando em PESQUISAR...")
        pesquisou = False
        for frame in page.frames:
            try:
                if frame.is_detached():
                    continue
                seletores_pesquisar = [
                    "button:has-text('PESQUISAR')",
                    "button:has-text('Pesquisar')",
                    "button:has-text('pesquisar')",
                    "input[value*='PESQUISAR']",
                    "input[value*='Pesquisar']",
                    "a:has-text('PESQUISAR')",
                    "img[title*='Pesquisar']",
                    "img[title*='pesquisar']",
                    "[onclick*='pesquisar']",
                    "[onclick*='Pesquisar']",
                ]
                for sel in seletores_pesquisar:
                    el = frame.locator(sel)
                    if el.count() > 0:
                        for i in range(el.count()):
                            try:
                                if el.nth(i).is_visible():
                                    print(f"      Pesquisar encontrado: {sel}")
                                    el.nth(i).click()
                                    pesquisou = True
                                    break
                            except Exception:
                                continue
                        if pesquisou:
                            break
                if pesquisou:
                    break
            except Exception:
                continue

        if pesquisou:
            print("      Aguardando resultados...")
            page.wait_for_timeout(5000)

        # 4. Clica no botao EXPORTAR
        print("[4/5] Procurando botao EXPORTAR...")
        exportado = False

        # Primeiro, encontra o frame da agenda (onde ha dados/tabela)
        agenda_frame = None
        for frame in page.frames:
            try:
                if frame.is_detached():
                    continue
                # Frame da agenda geralmente tem uma tabela ou grid
                if frame.locator("table, .grid, [class*='grid'], [class*='tabela']").count() > 0:
                    agenda_frame = frame
                    break
            except Exception:
                continue

        # Se nao encontrou frame especifico, usa todos
        frames_para_buscar = [agenda_frame] if agenda_frame else page.frames

        for frame in frames_para_buscar:
            if frame is None or frame.is_detached():
                continue
            try:
                # Prioriza imagens de exportar (botoes de toolbar)
                seletores = [
                    "img[title='Exportar']",
                    "img[title='exportar']",
                    "img[alt='Exportar']",
                    "img[alt='exportar']",
                    "img[title*='Exportar']",
                    "img[alt*='Exportar']",
                    "img[src*='export']",
                    "img[src*='csv']",
                    "img[src*='excel']",
                    "button:has-text('Exportar')",
                    "button:has-text('EXPORTAR')",
                    "[onclick*='exportar']",
                    "[onclick*='Exportar']",
                    "[onclick*='CSV']",
                ]

                for sel in seletores:
                    el = frame.locator(sel)
                    count = el.count()
                    if count > 0:
                        # Verifica se eh visivel
                        for i in range(count):
                            try:
                                if el.nth(i).is_visible():
                                    print(f"      Encontrado visivel: {sel} (indice {i})")
                                    el.nth(i).click()
                                    exportado = True
                                    break
                            except Exception:
                                continue
                        if exportado:
                            break

                if exportado:
                    break

            except Exception:
                continue

        if not exportado:
            print("      Botao EXPORTAR nao encontrado")
            browser.close()
            sys.exit(1)

        # 5. Aguarda download
        print("[5/5] Aguardando download...")
        for _ in range(30):
            if download_path and os.path.exists(download_path) and os.path.getsize(download_path) > 0:
                break
            page.wait_for_timeout(1000)

        if download_path and os.path.exists(download_path):
            size_kb = os.path.getsize(download_path) / 1024
            print(f"\n✅ Arquivo salvo: {download_path}")
            print(f"   Tamanho: {size_kb:.1f} KB")

            # Le o CSV e imprime como JSON
            registros = []
            with open(download_path, "r", encoding="latin-1", errors="replace") as f:
                reader = csv.DictReader(f, delimiter=";")
                for row in reader:
                    registros.append(row)

            print("\n---JSON_START---")
            print(json.dumps(registros, ensure_ascii=False))
            print("---JSON_END---")
        else:
            print("\n❌ Download nao concluido")
            browser.close()
            sys.exit(1)

        browser.close()


if __name__ == "__main__":
    main()
