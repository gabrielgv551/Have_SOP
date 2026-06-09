"""
Extrator de Contas a Pagar — GCOM Web (HIBRIDO que FUNCIONAVA)
===============================================================
Playwright faz login e carrega a Agenda Financeira completamente,
depois requests chama executeRule + download.
"""

import os
import re
import requests
import urllib3
from datetime import datetime
from playwright.sync_api import sync_playwright

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BASE_URL   = "https://amj.gcomweb.com.br/gcomweb"
LOGIN      = "CONSULTORIA1"
SENHA      = "1234"
SALVAR_EM  = os.getcwd()
HEADLESS   = False  # False + xvfb-run no servidor (GCOM detecta headless)


def _bloqueado(text: str) -> bool:
    t = text or ""
    return (
        "WFRException" in t
        or "não pode ser acessado" in t
        or "nao pode ser acessado" in t
        or "Acesso negado" in t
        or "interactionError" in t
        or "ClassNotFoundException" in t
        or "Exceção Gerada" in t
        or "Excecao Gerada" in t
    )


def _fechar_dialogo_erro(page):
    for frame in page.frames:
        try:
            if frame.is_detached():
                continue
            if frame.locator("img[src*='int_erro']").count() == 0:
                continue
            for sel in ["button:has-text('Ok')", "button:has-text('OK')", "text=Ok"]:
                btn = frame.locator(sel)
                if btn.count() > 0:
                    btn.first.click()
                    print("      Dialogo de erro fechado (Ok)")
                    page.wait_for_timeout(1500)
                    return True
        except Exception:
            continue
    return False


def _abrir_agenda_financeira(page):
    """Clica no icone da Agenda na area de trabalho (form 8278)."""
    for frame in page.frames:
        url = frame.url or ""
        if "formID=8278" not in url and "form.jsp" not in url:
            continue
        try:
            if frame.is_detached():
                continue
            alvo = frame.get_by_text("Agenda Financeira", exact=False)
            if alvo.count() > 0:
                alvo.first.click()
                print("      Agenda Financeira aberta (desktop)")
                page.wait_for_timeout(3000)
                return True
        except Exception:
            continue
    return _clicar_em_frames(page, ["text=Agenda Financeira"], "Agenda Financeira")


DOWNLOAD_PATHS = [
    "tmp/CONSULTORIA1/Agenda Financeira - Listagem de Registros Financeiros.csv",
    r"tmp\CONSULTORIA1\Agenda Financeira - Listagem de Registros Financeiros.csv",
]


def _sync_cookies(session, cookies_list):
    for c in cookies_list:
        domain = (c.get("domain") or "amj.gcomweb.com.br").lstrip(".")
        path = c.get("path") or "/gcomweb"
        session.cookies.set(c["name"], c["value"], domain=domain, path=path)


_JS_FETCH_DOWNLOAD = """
async (downloadFile) => {
    const url = '/gcomweb/download?download_file=' + encodeURIComponent(downloadFile);
    const response = await fetch(url, { method: 'GET', credentials: 'include' });
    if (!response.ok) {
        return { ok: false, status: response.status, size: 0, bytes: [] };
    }
    const buffer = await response.arrayBuffer();
    const bytes = Array.from(new Uint8Array(buffer));
    return { ok: true, status: response.status, size: bytes.length, bytes };
}
"""


def _download_via_browser(page, paths):
    targets = [page] + [f for f in page.frames if not f.is_detached()]
    for target in targets:
        for path in paths:
            try:
                result = target.evaluate(_JS_FETCH_DOWNLOAD, path)
            except Exception as ex:
                print(f"      fetch falhou no frame: {ex}")
                continue
            if not result:
                continue
            if result.get("ok") and result.get("size", 0) > 200:
                print(f"      download via navegador OK ({result['size']} bytes)")
                return bytes(result["bytes"])
            print(f"      fetch: status={result.get('status')}, size={result.get('size')}")
    return None


def _clicar_em_frames(page, seletores, rotulo):
    for frame in page.frames:
        try:
            if frame.is_detached():
                continue
            for sel in seletores:
                el = frame.locator(sel)
                for i in range(el.count()):
                    try:
                        if el.nth(i).is_visible():
                            el.nth(i).click()
                            print(f"      {rotulo}: {sel}")
                            return True
                    except Exception:
                        continue
        except Exception:
            continue
    return False


def _clicar_pesquisar(page):
    return _clicar_em_frames(
        page,
        [
            "button:has-text('PESQUISAR')",
            "button:has-text('Pesquisar')",
            "input[value*='PESQUISAR']",
            "input[value*='Pesquisar']",
            "a:has-text('PESQUISAR')",
            "img[title*='Pesquisar']",
            "[onclick*='pesquisar']",
            "[onclick*='Pesquisar']",
        ],
        "Pesquisar",
    )


def _clicar_exportar(page):
    return _clicar_em_frames(
        page,
        [
            "img[title='Exportar']",
            "img[title*='Exportar']",
            "img[alt*='Exportar']",
            "img[src*='export']",
            "img[src*='csv']",
            "button:has-text('Exportar')",
            "button:has-text('EXPORTAR')",
            "[onclick*='exportar']",
            "[onclick*='Exportar']",
        ],
        "Exportar",
    )


def _csv_valido(path: str) -> bool:
    if os.path.getsize(path) < 200:
        return False
    with open(path, "rb") as f:
        head = f.read(800)
    if head.lstrip().startswith(b"<"):
        return False
    return b";" in head or b"," in head


def main():
    download_via_ui = {}

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()

        def _on_download(dl):
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            path = os.path.join(SALVAR_EM, f"contas_a_pagar_{ts}.csv")
            dl.save_as(path)
            download_via_ui["path"] = path
            print(f"      download UI: {dl.suggested_filename}")

        page.on("download", _on_download)

        # ── 1. Login ──
        print("[1/6] Login...")
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
                    print(f"      Botao clicado: {sel}")
                    break
            except Exception:
                continue

        page.wait_for_timeout(3000)

        # ── 2. Navega para Agenda Financeira ──
        print("[2/6] Navegando para Agenda Financeira...")
        if not _abrir_agenda_financeira(page):
            print("      AVISO: nao abriu Agenda Financeira")
        page.wait_for_timeout(3000)
        _fechar_dialogo_erro(page)

        # ── 3. Aguarda TODOS os iframes carregarem completamente ──
        print("[3/6] Aguardando carregamento completo da Agenda...")
        page.wait_for_timeout(5000)

        print("      Aguardando regras de inicializacao...")
        for _ in range(10):
            page.wait_for_timeout(1000)
        _fechar_dialogo_erro(page)

        print("[4/6] Pesquisando registros na agenda...")
        if _clicar_pesquisar(page):
            page.wait_for_timeout(5000)
        else:
            print("      AVISO: botao Pesquisar nao encontrado")

        # ── 5. Extrai cookies ──
        print("[5/6] Extraindo cookies...")
        cookies = context.cookies()
        cookie_dict = {c["name"]: c["value"] for c in cookies}
        jsessionid = cookie_dict.get("JSESSIONID", "")
        webrun = cookie_dict.get("WebrunSelectedSystem", "PGC")
        print(f"      JSESSIONID: {jsessionid[:20]}..." if jsessionid else "      JSESSIONID nao encontrado")
        if not jsessionid:
            print("      FALHA: login sem sessao")
            browser.close()
            return 1

        agenda_form = "464569453"
        for fr in page.frames:
            u = fr.url or ""
            if "formID=" in u and "8278" not in u:
                m = re.search(r"formID=(\d+)", u)
                if m:
                    agenda_form = m.group(1)
                    break

        # ── 6. executeRule + download ──
        print("[6/6] executeRule.do via requests...")
        session = requests.Session()
        session.verify = False
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36",
            "Accept": "application/javascript,*/*;q=0.9",
            "Accept-Language": "pt-BR,pt;q=0.9",
            "Content-Type": "application/x-www-form-urlencoded",
            "Origin": "https://amj.gcomweb.com.br",
            "Referer": f"{BASE_URL}/openform.do?sys=PGC&action=openform&formID={agenda_form}&align=0&mode=-1&goto=-1&filter=&scrolling=False&firstLoad=true",
        })
        _sync_cookies(session, cookies)

        payload = {
            "action": "executeRule",
            "sys": "PGC",
            "formID": agenda_form,
            "pType": "2",
            "ruleClassName": "br.com.gsa.pgc.agenda.AgendaFinanceiraBean",
            "method": "exportarExcel",
        }
        print(f"      formID agenda: {agenda_form}")

        r = session.post(
            f"{BASE_URL}/executeRule.do",
            data=payload,
            timeout=60,
            verify=False,
        )
        print(f"      status={r.status_code}, len={len(r.text)}")
        if _bloqueado(r.text):
            print("      executeRule bloqueado")
            print(f"      resposta: {r.text[:400]}")
            browser.close()
            return 2
        print("      executeRule aceito")
        page.wait_for_timeout(3000)

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        filepath = os.path.join(SALVAR_EM, f"contas_a_pagar_{ts}.csv")
        data = None

        print("      Baixando CSV (requests)...")
        for download_file in DOWNLOAD_PATHS:
            r2 = session.get(
                f"{BASE_URL}/download",
                params={"download_file": download_file},
                timeout=60,
                verify=False,
            )
            clen = r2.headers.get("Content-Length", "?")
            print(f"      requests status={r2.status_code}, len={len(r2.content)}, Content-Length={clen}")
            if r2.status_code == 200 and len(r2.content) > 200:
                data = r2.content
                break

        if not data:
            print("      requests vazio — tentando download no navegador...")
            data = _download_via_browser(page, DOWNLOAD_PATHS)

        if not data:
            print("      tentando clique em Exportar no navegador...")
            if _clicar_exportar(page):
                for _ in range(30):
                    pth = download_via_ui.get("path")
                    if pth and os.path.exists(pth) and os.path.getsize(pth) > 200:
                        filepath = pth
                        break
                    page.wait_for_timeout(1000)

        if not os.path.exists(filepath) or os.path.getsize(filepath) < 200:
            if data:
                with open(filepath, "wb") as f:
                    f.write(data)
            else:
                browser.close()
                return 3

        if not _csv_valido(filepath):
            print("      FALHA: arquivo baixado nao parece CSV (HTML ou vazio?)")
            with open(filepath, "rb") as f:
                print(f"      inicio: {f.read(200)!r}")
            browser.close()
            return 3

        size_kb = os.path.getsize(filepath) / 1024
        print(f"\nOK Arquivo salvo: {filepath}")
        print(f"   Tamanho: {size_kb:.1f} KB")

        browser.close()
        return 0


if __name__ == "__main__":
    raise SystemExit(main() or 0)
