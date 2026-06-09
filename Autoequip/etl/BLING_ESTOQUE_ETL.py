"""
Extrator de Estoque do Bling — 100% Playwright
=============================================
Faz login no Bling, navega até o estoque e baixa o relatório.
Criado para a empresa Autoequip.
"""

import os
import sys
import csv
import json
import time
from datetime import datetime
from playwright.sync_api import sync_playwright

BASE_URL = "https://www.bling.com.br"
LOGIN_URL = "https://www.bling.com.br/login"
USUARIO = "gabriel.viana@rafaelbueno"
SENHA = "seFze0-tesgah-fuxpag"
SALVAR_EM = os.getcwd()
HEADLESS = False  # False = navegador visivel (mais confiavel para evitar bloqueios)

def main():
    download_path = None

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS)
        context = browser.new_context(accept_downloads=True, user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        page = context.new_page()

        # Intercepta download
        def handle_download(download):
            nonlocal download_path
            fn = download.suggested_filename
            print(f"      Download detectado: {fn}")
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            download_path = os.path.join(SALVAR_EM, f"bling_estoque_{ts}.csv")
            download.save_as(download_path)
            print(f"      Salvo em: {download_path}")

        page.on("download", handle_download)

        print("[1/5] Acessando página de login do Bling...")
        page.goto(LOGIN_URL, timeout=60000)
        page.wait_for_timeout(3000)

        print("[2/5] Preenchendo credenciais...")
        # Lidar com possiveis variações de seletores de login
        usuario_input = page.locator("input[name='usuario'], input[name='login'], input[type='text'], input[name='email']")
        senha_input = page.locator("input[name='senha'], input[type='password']")
        
        if usuario_input.count() > 0 and senha_input.count() > 0:
            usuario_input.first.fill(USUARIO)
            senha_input.first.fill(SENHA)
            print("      Credenciais preenchidas.")
        else:
            print("      [ERRO] Campos de login não encontrados.")
            browser.close()
            sys.exit(1)

        print("[3/5] Efetuando login...")
        btn_login = page.locator("button:has-text('Entrar'), button:has-text('Login'), input[type='submit']")
        if btn_login.count() > 0:
            btn_login.first.click()
        else:
            page.keyboard.press("Enter")
            
        # Aguarda a página inicial carregar após o login
        try:
            page.wait_for_load_state("networkidle", timeout=15000)
        except Exception:
            pass
        page.wait_for_timeout(5000)
        
        # Verificar se login teve sucesso
        if "login" in page.url.lower():
            print("      [ERRO] Falha no login ou captcha necessário. Verifique a interface visual.")
            # time.sleep(30) # Descomente para preencher captcha manualmente
            # browser.close()
            # sys.exit(1)
            
        print("[4/5] Navegando para a página de Estoque...")
        # URLs de estoque conhecidas no Bling
        page.goto(f"{BASE_URL}/estoque.php", timeout=60000)
        page.wait_for_timeout(5000)

        print("[5/5] Exportando Estoque...")
        # Tenta achar o botão de exportar
        exportado = False
        seletores_exportar = [
            "button:has-text('Exportar')",
            "a:has-text('Exportar Planilha')",
            "img[alt*='Exportar']",
            "img[title*='Exportar']",
            "[onclick*='exportar']",
            "text='Exportar relatório'"
        ]
        
        for sel in seletores_exportar:
            el = page.locator(sel)
            if el.count() > 0:
                for i in range(el.count()):
                    try:
                        if el.nth(i).is_visible():
                            print(f"      Clicando no botão de exportar: {sel}")
                            el.nth(i).click()
                            exportado = True
                            break
                    except Exception:
                        continue
                if exportado:
                    break
                    
        if not exportado:
            print("      [AVISO] Botão de exportar direto não encontrado. Você pode precisar ajustar os seletores para a página atual do Bling.")
            # browser.close()
            # sys.exit(1)

        # Aguarda download (se houver)
        for _ in range(30):
            if download_path and os.path.exists(download_path) and os.path.getsize(download_path) > 0:
                break
            page.wait_for_timeout(1000)

        if download_path and os.path.exists(download_path):
            size_kb = os.path.getsize(download_path) / 1024
            print(f"\n✅ Arquivo de estoque salvo: {download_path}")
            print(f"   Tamanho: {size_kb:.1f} KB")
        else:
            print("\n❌ Download não concluído automaticamente ou precisa ser feito manualmente. O navegador será mantido aberto por 30s.")
            page.wait_for_timeout(30000)

        browser.close()

if __name__ == "__main__":
    main()
