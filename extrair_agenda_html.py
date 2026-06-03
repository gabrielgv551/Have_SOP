"""
Extrator de Contas a Pagar — via HTML da tabela
===============================================
Faz login com Playwright, navega ate a Agenda Financeira,
clica em PESQUISAR, espera a tabela carregar e extrai os dados
diretamente do HTML (sem executeRule, sem download).
"""

import os
import csv
import json
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
        context = browser.new_context()
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

        # 3. Aguarda carregamento e clica em PESQUISAR
        print("[3/4] Aguardando carregamento e clicando PESQUISAR...")
        page.wait_for_timeout(10000)

        pesquisou = False
        for frame in page.frames:
            try:
                if frame.is_detached():
                    continue
                seletores = [
                    "button:has-text('PESQUISAR')",
                    "button:has-text('Pesquisar')",
                    "input[value*='PESQUISAR']",
                    "input[value*='Pesquisar']",
                    "img[title*='Pesquisar']",
                    "[onclick*='pesquisar']",
                    "[onclick*='Pesquisar']",
                ]
                for sel in seletores:
                    el = frame.locator(sel)
                    if el.count() > 0:
                        for i in range(el.count()):
                            try:
                                if el.nth(i).is_visible():
                                    print(f"      Clicando: {sel}")
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

        # 4. Extrai dados da tabela
        print("[4/4] Extraindo dados da tabela...")
        registros = []

        for frame in page.frames:
            try:
                if frame.is_detached():
                    continue

                # Procura tabelas com dados
                tabelas = frame.locator("table")
                count = tabelas.count()
                if count == 0:
                    continue

                for t in range(count):
                    try:
                        tabela = tabelas.nth(t)
                        # Verifica se a tabela tem mais de 2 linhas (header + dados)
                        linhas = tabela.locator("tr")
                        if linhas.count() < 2:
                            continue

                        # Extrai header
                        header_cells = linhas.first.locator("th, td")
                        headers = []
                        for h in range(header_cells.count()):
                            headers.append(header_cells.nth(h).inner_text().strip())

                        # Extrai dados
                        dados_tabela = []
                        for r in range(1, linhas.count()):
                            cells = linhas.nth(r).locator("td, th")
                            if cells.count() == 0:
                                continue
                            row_data = {}
                            for c in range(min(cells.count(), len(headers))):
                                row_data[headers[c]] = cells.nth(c).inner_text().strip()
                            if row_data:
                                dados_tabela.append(row_data)

                        if len(dados_tabela) > 0:
                            print(f"      Tabela {t}: {len(dados_tabela)} registros")
                            registros.extend(dados_tabela)

                    except Exception:
                        continue

            except Exception:
                continue

        print(f"\n✅ Total de registros: {len(registros)}")

        if registros:
            # Salva como CSV
            filepath = os.path.join(SALVAR_EM, "contas_a_pagar.csv")
            with open(filepath, "w", newline="", encoding="utf-8-sig") as f:
                writer = csv.DictWriter(f, fieldnames=registros[0].keys(), delimiter=";")
                writer.writeheader()
                writer.writerows(registros)

            size_kb = os.path.getsize(filepath) / 1024
            print(f"   CSV salvo: {filepath} ({size_kb:.1f} KB)")

            # Imprime como JSON
            print("\n---JSON_START---")
            print(json.dumps(registros, ensure_ascii=False))
            print("---JSON_END---")
        else:
            print("\n❌ Nenhum dado encontrado na tabela")

        browser.close()


if __name__ == "__main__":
    main()
