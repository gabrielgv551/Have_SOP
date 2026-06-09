"""
╔══════════════════════════════════════════════════════════════╗
║      S&OP Intelligence · Preco Certo ETL                     ║
║  Playwright para auth → requests para paginação              ║
║  Carga incremental automática + upsert PostgreSQL            ║
╚══════════════════════════════════════════════════════════════╝

DEPENDÊNCIAS:
  pip install playwright requests pandas sqlalchemy psycopg2-binary python-dotenv
  playwright install chromium

COMO FUNCIONA:
  1. Playwright abre browser headless, loga no Preco Certo
  2. Captura token JWT e cookies da sessão autenticada
  3. requests usa esse token para paginar GET /api/order
  4. Upsert no PostgreSQL (tabela bd_vendas_precocerto)

USO:
  python PRECOCERTO_ETL.py                  # incremental
  FULL_RELOAD=1 python PRECOCERTO_ETL.py    # desde FIRST_DATE
  HEADED=1 python PRECOCERTO_ETL.py         # ver o browser
"""

import io
import os
import sys
import time
import threading
import json
import requests
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from sqlalchemy.types import BigInteger, Numeric, Text, TIMESTAMP, Integer
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────────────────────────────
# CONFIGURAÇÃO
# ─────────────────────────────────────────────────────────────────
DB_CONFIG = {
    "host"    : os.getenv("MARCON_HOST",     "37.60.236.200"),
    "port"    : os.getenv("MARCON_PORT",     5432),
    "database": os.getenv("MARCON_DB",       "Marcon"),
    "user"    : os.getenv("MARCON_USER",     "postgres"),
    "password": os.getenv("MARCON_PASSWORD", ""),
}

PC_URL       = "https://sys.precocerto.co"
PC_EMAIL     = "comercial@casaeletromarcon.com.br"
PC_SENHA     = "eletro123"
FULL_RELOAD     = bool(os.getenv("FULL_RELOAD"))
REPROCESS       = bool(os.getenv("REPROCESS"))       # REPROCESS=1: reprocessa do cache, sem API
FORCE_DOWNLOAD  = os.getenv("FORCE_DOWNLOAD", "0") not in ("", "0", "false", "False")  # FORCE_DOWNLOAD=1: ignora cache
OVERLAP_DAYS    = int(os.getenv("OVERLAP_DAYS", "2"))
FIRST_DATE      = os.getenv("PRECOCERTO_FIRST_DATE", "2025-01-01")
HEADED          = bool(os.getenv("HEADED"))    # HEADED=1 para ver o browser
DATE_BEFORE     = os.getenv("DATE_BEFORE")     # DATE_BEFORE=2025-01-31 para limitar período
PAGE_LIMIT      = 500     # 500 itens/req — 5x menos requests que 100
TABLE           = "bd_vendas"
CACHE_DIR       = os.getenv("CACHE_DIR", os.path.join(os.path.dirname(__file__), "cache"))


# ─────────────────────────────────────────────────────────────────
# SPINNER
# ─────────────────────────────────────────────────────────────────
class Spinner:
    _frames = ["⠋","⠙","⠹","⠸","⠼","⠴","⠦","⠧","⠇","⠏"]
    def __init__(self, msg):
        self.msg = msg
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._spin, daemon=True)
        self._start = time.time()
    def _spin(self):
        i = 0
        while not self._stop.is_set():
            elapsed = time.time() - self._start
            sys.stdout.write(f"\r  {self._frames[i%len(self._frames)]}  {self.msg}  ({elapsed:.1f}s)   ")
            sys.stdout.flush()
            time.sleep(0.1)
            i += 1
    def __enter__(self): self._thread.start(); return self
    def __exit__(self, *_):
        self._stop.set(); self._thread.join()
        sys.stdout.write(f"\r  ✔  {self.msg}  ({time.time()-self._start:.1f}s)        \n")
        sys.stdout.flush()


# ─────────────────────────────────────────────────────────────────
# CACHE DE DADOS BRUTOS
# ─────────────────────────────────────────────────────────────────
# Session global com keep-alive — reutiliza conexões TCP/SSL
_session = requests.Session()
_session.headers.update({
    "Accept":        "application/json, text/plain, */*",
    "Origin":        PC_URL,
    "Referer":       f"{PC_URL}/gerenciar/pedidos-de-venda/",
    "User-Agent":    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124",
    "Accept-Language": "pt-BR,pt;q=0.9",
})


def _cache_path(tipo: str, date_after: str, date_before: str) -> str:
    os.makedirs(CACHE_DIR, exist_ok=True)
    return os.path.join(CACHE_DIR, f"{tipo}_{date_after}_{date_before}.json")

def _cache_load(tipo: str, date_after: str, date_before: str):
    path = _cache_path(tipo, date_after, date_before)
    if os.path.exists(path):
        print(f"    [cache] Lendo {os.path.basename(path)}")
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return None

def _cache_save(tipo: str, date_after: str, date_before: str, data):
    path = _cache_path(tipo, date_after, date_before)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    print(f"    [cache] Salvo {os.path.basename(path)}")


def _sep(c="─", n=60): print(c * n)
def _step(n, total, msg):
    print(f"\n[{datetime.now():%H:%M:%S}] ── Etapa {n}/{total}: {msg}")


# ─────────────────────────────────────────────────────────────────
# AUTENTICAÇÃO VIA PLAYWRIGHT
# ─────────────────────────────────────────────────────────────────
def autenticar_playwright() -> dict:
    """
    Loga no Preco Certo via Playwright e captura:
    - sessionid  (Django session — chave principal de auth)
    - csrftoken  (CSRF token para headers)
    - todos os outros cookies

    O backend usa SessionAuthentication via Django session.
    O Bearer token em localStorage pode ser 'fake' — o sessionid é o que funciona.
    """
    from playwright.sync_api import sync_playwright

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=not HEADED)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124",
            locale="pt-BR",
        )
        page = context.new_page()

        print(f"\n  Abrindo {PC_URL}/login/ ...")
        page.goto(f"{PC_URL}/login/", wait_until="domcontentloaded", timeout=40_000)
        page.wait_for_timeout(2000)

        # Se já redirecionou para dashboard, sessão ainda ativa
        if "/dashboard" in page.url or "/v2/" in page.url:
            print(f"  Sessão ainda ativa — dashboard detectado")
        else:
            print(f"  Preenchendo credenciais ...")
            # Salvar screenshot para diagnóstico
            _scr = os.path.join(os.path.dirname(__file__), "login_debug.png")
            page.screenshot(path=_scr)
            print(f"  [debug] Screenshot salvo: {_scr}")
            print(f"  [debug] Título: {page.title()} | URL: {page.url}")
            # Tentar múltiplos seletores para compatibilidade
            _user_sel = None
            for _sel in ["input[name='username_login']", "input[type='email']",
                         "input[type='text']", "#id_username", "input[name='username']"]:
                try:
                    page.wait_for_selector(_sel, timeout=3_000)
                    _user_sel = _sel
                    break
                except Exception:
                    continue
            if not _user_sel:
                raise RuntimeError("Formulário de login não encontrado. Ver login_debug.png")
            print(f"  [debug] Selector usado: {_user_sel}")
            # Detectar selector da senha (substitui 'username' por 'password')
            _pass_sel = _user_sel.replace("username_login", "password_login") \
                                  .replace("type='email'", "type='password'") \
                                  .replace("type='text'", "type='password'") \
                                  .replace("id_username", "id_password") \
                                  .replace("name='username'", "name='password'")
            page.fill(_user_sel, PC_EMAIL)
            try:
                page.fill(_pass_sel, PC_SENHA)
            except Exception:
                page.fill("input[type='password']", PC_SENHA)
            page.click("button[type='submit']")

            print(f"  Aguardando redirect para dashboard ...")
            try:
                page.wait_for_url("**/dashboard**", timeout=30_000)
            except Exception:
                pass
            page.wait_for_timeout(2000)

        print(f"  URL após login: {page.url}")

        # Capturar todos os cookies do domínio precocerto.co
        all_cookies = context.cookies(urls=[PC_URL])
        cookie_dict = {c["name"]: c["value"] for c in all_cookies}
        browser.close()

    sessionid = cookie_dict.get("sessionid", "")
    csrftoken  = cookie_dict.get("csrftoken", "")

    if not sessionid:
        raise RuntimeError(
            f"Login falhou — sessionid não encontrado.\n"
            f"Cookies obtidos: {list(cookie_dict.keys())}\n"
            f"URL atual pode ainda ser /login/ (credenciais inválidas?)"
        )

    print(f"\n  ✔  Autenticado | sessionid: {sessionid[:20]}... | csrf: {csrftoken[:20]}...")
    return {
        "sessionid":  sessionid,
        "csrftoken":  csrftoken,
        "cookies":    cookie_dict,
        "cookie_str": "; ".join(f"{k}={v}" for k, v in cookie_dict.items()),
    }


# ─────────────────────────────────────────────────────────────────
# HEADERS DA API  (session auth, não Bearer)
# ─────────────────────────────────────────────────────────────────
def _api_headers(auth: dict) -> dict:
    return {
        "x-csrftoken": auth.get("csrftoken", ""),
        "Cookie":      auth.get("cookie_str", ""),
    }


# ─────────────────────────────────────────────────────────────────
# BUSCAR PEDIDOS (paginação por offset)
# ─────────────────────────────────────────────────────────────────
def _fmt_date_range(date_after: str, date_before: str) -> str:
    """'yyyy-MM-dd' → 'dd/MM/yyyy - dd/MM/yyyy'"""
    def iso_to_br(s):
        d = datetime.strptime(s, "%Y-%m-%d")
        return d.strftime("%d/%m/%Y")
    return f"{iso_to_br(date_after)} - {iso_to_br(date_before)}"


def _buscar_pagina(auth: dict, url: str, params_extra: dict, offset: int) -> dict:
    """Helper genérico de paginação para qualquer endpoint."""
    params = {**params_extra, "limit": PAGE_LIMIT, "offset": offset}
    for tentativa in range(5):
        try:
            r = _session.get(url, headers=_api_headers(auth), params=params, timeout=90)
            if r.status_code == 200:
                try:
                    return r.json()
                except ValueError as e:
                    with open("erro_api_precocerto.html", "w", encoding="utf-8") as f:
                        f.write(r.text)
                    raise RuntimeError(f"JSON inválido (API retornou HTML/Texto). Salvo em erro_api_precocerto.html. Erro: {e}")
            if r.status_code in (401, 403):
                raise RuntimeError(f"Sessão expirada (HTTP {r.status_code}).")
            wait = 10 * (tentativa + 1)
            print(f"\n  [{r.status_code}] Aguardando {wait}s (tentativa {tentativa+1}/5)...")
            time.sleep(wait)
        except requests.Timeout:
            print(f"\n  [Timeout] Tentativa {tentativa+1}/5 — aguardando 15s...")
            time.sleep(15)
    raise RuntimeError(f"Falha após 5 tentativas (offset={offset})")


def buscar_pagina(auth: dict, date_after: str, date_before: str, offset: int) -> dict:
    params = {
        "source_created": _fmt_date_range(date_after, date_before),
        "date_after":     date_after,
        "date_before":    date_before,
        "ordering":       "-source_created",
    }
    return _buscar_pagina(auth, f"{PC_URL}/api/orderline", params, offset)


WORKERS = 5   # 5 paralelos — reduz timeouts por sobrecarga no servidor


def baixar_todos(auth: dict, date_after: str, date_before: str) -> list[dict]:
    from concurrent.futures import ThreadPoolExecutor, as_completed

    if not FORCE_DOWNLOAD:
        cached = _cache_load("orderlines", date_after, date_before)
        if cached is not None:
            print(f"    ✔  {len(cached):,} orderlines do cache")
            return cached

    # Página 0 para descobrir o total
    dados = buscar_pagina(auth, date_after, date_before, 0)
    total = dados.get("total", 0)

    if total == 0:
        print(f"    → 0 pedidos no período {date_after} → {date_before}")
        return []

    paginas_total = (total + PAGE_LIMIT - 1) // PAGE_LIMIT
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"    [{ts}] {total:,} pedidos | {paginas_total} pág | {WORKERS} workers paralelos")

    # Offsets de todas as páginas restantes
    offsets = [i * PAGE_LIMIT for i in range(1, paginas_total)]

    resultados = {0: dados.get("rows", [])}
    concluidas = 1

    def fetch(offset):
        d = buscar_pagina(auth, date_after, date_before, offset)
        return offset, d.get("rows", [])

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {pool.submit(fetch, off): off for off in offsets}
        for fut in as_completed(futures):
            off, rows = fut.result()
            resultados[off] = rows
            concluidas += 1
            pct = int(concluidas / paginas_total * 20)
            bar = "█" * pct + "░" * (20 - pct)
            ts  = datetime.now().strftime("%H:%M:%S")
            sys.stdout.write(
                f"\r    [{ts}] [{bar}] {concluidas}/{paginas_total} pág concluídas   "
            )
            sys.stdout.flush()

    sys.stdout.write("\n")

    # Ordenar por offset e achatar
    todos = []
    for off in sorted(resultados):
        todos.extend(resultados[off])

    print(f"    ✔  {len(todos):,} pedidos baixados")
    _cache_save("orderlines", date_after, date_before, todos)
    return todos


def baixar_orders(auth: dict, date_after: str, date_before: str) -> dict:
    """Busca /api/order → dict {id: row, number: row} para enriquecer Total Venda, Status e Frete."""
    from concurrent.futures import ThreadPoolExecutor, as_completed

    if not FORCE_DOWNLOAD:
        cached = _cache_load("orders", date_after, date_before)
        if cached is not None:
            print(f"    ✔  {len(cached):,} entradas /api/order do cache")
            return cached

    # Extender ±7 dias para capturar pedidos com source_created fora da janela das orderlines
    d_after_ext  = (datetime.strptime(date_after,  "%Y-%m-%d") - timedelta(days=7)).strftime("%Y-%m-%d")
    d_before_ext = (datetime.strptime(date_before, "%Y-%m-%d") + timedelta(days=7)).strftime("%Y-%m-%d")
    params = {
        "source_created": _fmt_date_range(d_after_ext, d_before_ext),
        "date_after":     d_after_ext,
        "date_before":    d_before_ext,
        "ordering":       "-source_created",
    }
    dados = _buscar_pagina(auth, f"{PC_URL}/api/order", params, 0)
    total = dados.get("total", 0)
    if total == 0:
        return {}

    paginas_total = (total + PAGE_LIMIT - 1) // PAGE_LIMIT
    resultados    = {0: dados.get("rows", [])}
    offsets       = [i * PAGE_LIMIT for i in range(1, paginas_total)]

    def fetch(offset):
        d = _buscar_pagina(auth, f"{PC_URL}/api/order", params, offset)
        return offset, d.get("rows", [])

    concluidas_o = 1
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures_o = {pool.submit(fetch, off): off for off in offsets}
        for fut in as_completed(futures_o):
            off, rows = fut.result()
            resultados[off] = rows
            concluidas_o += 1
            pct = int(concluidas_o / paginas_total * 20)
            bar = "█" * pct + "░" * (20 - pct)
            ts  = datetime.now().strftime("%H:%M:%S")
            sys.stdout.write(
                f"\r    [{ts}] /order [{bar}] {concluidas_o}/{paginas_total} pág   "
            )
            sys.stdout.flush()
    sys.stdout.write("\n")

    order_map = {}
    for off in sorted(resultados):
        for o in resultados[off]:
            oid  = str(o.get("id",     ""))
            onum = str(o.get("number", ""))
            if oid:  order_map[oid]  = o
            if onum: order_map[onum] = o
    print(f"    ✔  {len(order_map):,} entradas /api/order carregadas (id + number)")
    _cache_save("orders", date_after, date_before, order_map)
    return order_map


# ─────────────────────────────────────────────────────────────────
# TRANSFORMAÇÃO → DATAFRAME  (mesmo schema de bd_vendas)
# ─────────────────────────────────────────────────────────────────
def _s(v, default=""):
    return str(v).strip() if v is not None else default

def _canal(v, default=""):
    """Extrai label/name do canal — suporta string ou dict {'label': ..., 'name': ...}"""
    if v is None: return default
    if isinstance(v, dict): return _s(v.get("label") or v.get("name"), default)
    return _s(v, default)

def _n(v, default=0.0):
    try: return float(v) if v is not None else default
    except: return default

def _i(v, default=0):
    try: return int(v) if v is not None else default
    except: return default


def processar(linhas_api: list, order_map: dict = None) -> pd.DataFrame:
    """
    Converte as linhas de /api/orderline para o schema de bd_vendas.
    Se order_map fornecido, enriquece Total Venda, Status e Frete via /api/order.
    """
    print(f"  Processando {len(linhas_api):,} linhas...")

    # Pré-passo: somar custo e contar linhas reais por order_id
    custo_por_pedido:         dict[str, float]   = {}
    custo_por_prod_no_pedido: dict[tuple, float]  = {}  # (oid_key, pid) → custo combinado (fix dedup)
    linhas_por_pedido:        dict[str, int]      = {}
    for L in linhas_api:
        oid_key  = str(L.get("order_id", "") or L.get("number", ""))
        pid_str  = str(_i(L.get("product_id")))
        qtd      = _n(L.get("quantity"))
        uc       = _n(L.get("unit_cost") or L.get("product_cost"))
        ct       = _n(L.get("product_cost") or uc * qtd)
        custo_por_pedido[oid_key]  = custo_por_pedido.get(oid_key, 0.0) + ct
        linhas_por_pedido[oid_key] = linhas_por_pedido.get(oid_key, 0)  + 1
        custo_por_prod_no_pedido[(oid_key, pid_str)] = custo_por_prod_no_pedido.get((oid_key, pid_str), 0.0) + ct

    linhas = []

    for L in linhas_api:
        number = L.get("number")  # número interno do orderline (não mostrado no UI)
        try:
            order_id = int(number)  # será sobrescrito pelo order.number após enrichment
        except (TypeError, ValueError):
            order_id = abs(hash(str(number))) % 2_000_000_000

        qtd        = _n(L.get("quantity"))
        unit_cost  = _n(L.get("unit_cost") or L.get("product_cost"))
        custo_total = _n(L.get("product_cost") or unit_cost * qtd)
        comissao   = _n(L.get("unit_commission")) * max(qtd, 1)
        icms_difal        = _n(L.get("icms_difal"))
        icms_pct          = _n(L.get("used_icms_percent") or L.get("icms_percent"))
        # Percentuais confirmados da API — valores absolutos calculados após total_venda
        pct_credito_icms  = _n(L.get("final_icms_credit_percent"))
        pct_icms_venda    = _n(L.get("used_icms_percent") or L.get("icms_percent"))
        pct_ipi           = _n(L.get("ipi_percent"))
        pct_pis           = _n(L.get("replaced_pis_percent"))   # sobrescrito pelo tributary_config
        pct_cofins        = _n(L.get("replaced_cofins_percent")) # sobrescrito pelo tributary_config
        pct_simples       = 0.0

        # Enriquecer com /api/order
        total_venda       = 0.0
        status_venda      = ""
        frete_prod        = 0.0
        perc_margem       = 0.0
        valor_desconto    = 0.0
        total_venda_ped   = 0.0
        margem_produto    = 0.0
        margem_contrib    = 0.0
        numero_ecommerce  = _s(L.get("number"))
        if order_map:
            numero = str(L.get("order_id", "") or L.get("number", ""))
            ordem  = order_map.get(numero, {})
            # Fallback: tentar order_id como inteiro (evita miss por "12345.0" vs "12345")
            if not ordem and L.get("order_id"):
                try:
                    ordem = order_map.get(str(int(float(str(L.get("order_id"))))), {})
                except (ValueError, TypeError):
                    pass
            status_venda     = _s(ordem.get("status"))
            frete_prod       = _n(ordem.get("shipping_cost"))
            perc_margem      = _n(ordem.get("percentage_margin"))
            valor_desconto   = _n(ordem.get("discount_subsidy"))
            total_venda_ped  = _n(ordem.get("total"))
            margem_contrib   = _n(ordem.get("profit"))
            numero_ecommerce = _s(ordem.get("channel_order_id") or L.get("number"))
            if ordem.get("number"):
                order_id = _i(ordem.get("number"))  # número visível no Preco Certo UI
            # Usar contagem real do batch (não lines_count da API — pode estar errado)
            lines_count = linhas_por_pedido.get(str(L.get("order_id", "") or number), 1)
            if lines_count <= 1:
                total_venda    = total_venda_ped
                margem_produto = margem_contrib
            else:
                # Multi-produto: distribuir total e margem proporcionalmente ao custo da linha
                # Usa custo COMBINADO por (order, produto) para evitar subcontagem após drop_duplicates
                # Fallback para quantidade quando custo_total=0 (evita total_venda=0)
                oid_key_r = str(L.get("order_id", "") or number)
                pid_key_r = str(_i(L.get("product_id")))
                custo_combinado = custo_por_prod_no_pedido.get((oid_key_r, pid_key_r), custo_total)
                total_custo_ped = custo_por_pedido.get(oid_key_r, 0.0)
                if total_custo_ped > 0:
                    ratio = custo_combinado / total_custo_ped
                else:
                    qtd_total_ped = sum(
                        _n(x.get("quantity")) for x in linhas_api
                        if str(x.get("order_id", "") or x.get("number", "")) == oid_key_r
                    )
                    ratio = (qtd / qtd_total_ped) if qtd_total_ped > 0 else 0.0
                total_venda    = total_venda_ped * ratio
                margem_produto = margem_contrib  * ratio
            # Alíquotas do tributary_config (Simples Nacional)
            tcfg = ordem.get("tributary_config") or {}
            if isinstance(tcfg, dict):
                pct_pis    = _n(tcfg.get("pis",    pct_pis))
                pct_cofins = _n(tcfg.get("cofins", pct_cofins))
                pct_simples = _n(tcfg.get("simple", 0))

        # Calcular impostos absolutos: crédito sobre custo, débitos sobre total_venda
        credito_icms = (pct_credito_icms / 100.0) * custo_total
        icms_venda   = (pct_icms_venda   / 100.0) * total_venda
        ipi          = (pct_ipi          / 100.0) * total_venda
        pis          = (pct_pis          / 100.0) * total_venda
        cofins       = (pct_cofins        / 100.0) * total_venda
        simples      = (pct_simples       / 100.0) * total_venda
        carga_trib   = icms_venda + ipi + pis + cofins - credito_icms

        linhas.append({
            "Order ID"            : order_id,
            "Produto ID"          : _i(L.get("product_id")),
            "Sku"                 : _s(L.get("product_code")),
            "Sku Anterior"        : "",
            "Nome Produto"        : _s(L.get("product_name")),
            "Categoria"           : "",
            "NCM"                 : "",
            "Canal de venda"      : _canal(L.get("channel")),
            "Canal Apelido"       : _canal(L.get("channel")),
            "Data"                : L.get("source_created"),
            "Status"              : status_venda,
            "Numero Ecommerce"    : numero_ecommerce,
            "Cliente"             : "",
            "Estado"              : _s(L.get("state")),
            "Cidade"              : "",
            "Metodo Envio"        : "",
            "App Integracao"      : _s(L.get("integration")),
            "Quantidade Vendida"  : qtd,
            "Quantidade Pedido"   : qtd,
            "Estoque Total"       : 0.0,
            "Custo Total"         : custo_total,
            "Total Venda"         : total_venda,
            "Valor Desconto"      : valor_desconto,
            "Margem Produto"      : margem_produto,
            "Valor Liquido Prod"  : 0.0,
            "Perc Custo"          : 0.0,
            "Perc Margem"         : perc_margem,
            "Credito ICMS"        : credito_icms,
            "ICMS Venda"          : icms_venda,
            "IPI"                 : ipi,
            "PIS"                 : pis,
            "COFINS"              : cofins,
            "Simples"             : simples,
            "Carga Tributaria"    : carga_trib,
            "Imposto Produto"     : -(carga_trib if carga_trib != 0.0 else simples),
            "Frete Pago Prod"     : -frete_prod,
            "Comissao Produto"    : -comissao,
            "Frete Recebido"      : 0.0,
            "Frete Pago"          : 0.0,
            "Comissao Pedido"     : 0.0,
            "Diferenca Frete"     : 0.0,
            "Taxas"               : _n(icms_pct),
            "Embalagem"           : 0.0,
            "Repasse Financeiro"  : total_venda - comissao - frete_prod,
            "Margem Contribuicao" : margem_contrib,
            "Valor Liquido"       : 0.0,
            "Total Custo Pedido"  : 0.0,
            "Total Venda Pedido"  : total_venda_ped,
        })

    if not linhas:
        return pd.DataFrame()

    df = pd.DataFrame(linhas)

    # Data
    df["Data"] = pd.to_datetime(df["Data"], format="mixed", dayfirst=True, errors="coerce", utc=True)
    df["Data"] = df["Data"].dt.tz_convert("America/Sao_Paulo").dt.tz_localize(None)
    df["Ano"]  = df["Data"].dt.year
    df["Mes"]  = df["Data"].dt.month

    # DRE (mesmas colunas calculadas do GE Finance)
    df["Quantidade"]               = df["Quantidade Vendida"]
    df["Receita Bruta"]            = df["Total Venda"]
    df["Imposto"]                  = df["Imposto Produto"]  # = Carga Tributária (soma dos impostos)
    df["Receita Liquida"]          = df["Total Venda"] + df["Imposto Produto"]
    df["CMV"]                      = -df["Custo Total"]
    df["Margem Bruta"]             = df["Receita Liquida"] + df["CMV"]
    _rl                            = df["Receita Liquida"].replace(0, pd.NA)
    df["MB_pct"]                   = (df["Margem Bruta"] / _rl).fillna(0).round(4)
    df["Comissoes"]                = df["Comissao Produto"]
    df["Frete"]                    = df["Frete Pago Prod"]
    df["Margem Contribuicao Calc"] = df["Margem Bruta"] + df["Comissoes"] + df["Frete"]
    df["MC_pct"]                   = (df["Margem Contribuicao Calc"] / _rl).fillna(0).round(4)

    # Numéricas
    num_cols = [
        "Quantidade Vendida","Quantidade Pedido","Estoque Total","Custo Total",
        "Total Venda","Valor Desconto","Margem Produto","Valor Liquido Prod",
        "Perc Custo","Perc Margem","Imposto Produto","Frete Pago Prod","Comissao Produto",
        "Credito ICMS","ICMS Venda","IPI","PIS","COFINS","Simples","Carga Tributaria",
        "Frete Recebido","Frete Pago","Comissao Pedido","Diferenca Frete","Taxas",
        "Embalagem","Repasse Financeiro","Margem Contribuicao","Valor Liquido",
        "Total Custo Pedido","Total Venda Pedido",
        "Quantidade","Receita Bruta","Imposto","Receita Liquida","CMV",
        "Margem Bruta","MB_pct","Comissoes","Frete","Margem Contribuicao Calc","MC_pct",
    ]
    for col in num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    df["Order ID"]   = pd.to_numeric(df["Order ID"],   errors="coerce").fillna(0).astype(int)
    df["Produto ID"] = pd.to_numeric(df["Produto ID"], errors="coerce").fillna(0).astype(int)

    df = df.drop_duplicates(subset=["Order ID", "Produto ID"])
    matched = sum(1 for r in linhas if r.get("Order ID") and r.get("Total Venda", 0) != 0)
    print(f"  ✔  {len(linhas):,} linhas processadas | {matched:,} com order match ({matched*100//max(len(linhas),1)}%)")
    if linhas:
        sample = linhas[0]
        print(f"  [debug] 1ª linha: Order ID={sample.get('Order ID')}, Total Venda={sample.get('Total Venda')}, Simples={sample.get('Simples')}")
    return df


# ─────────────────────────────────────────────────────────────────
# PEDIDOS ÓRFÃOS (sem orderlines — linhas sintéticas via resume_lines)
# ─────────────────────────────────────────────────────────────────
def processar_orfaos(order_map: dict, order_ids_cobertos: set,
                     date_after: str = None, date_before: str = None) -> pd.DataFrame:
    """
    Para pedidos em /api/order sem nenhuma orderline correspondente,
    cria linhas sintéticas usando resume_lines (contém product_code e product_name).
    Total é distribuído igualmente entre as linhas do resume.
    date_after / date_before: filtro ISO para limitar órfãos ao período correto,
    evitando inserir pedidos das bordas da janela estendida.
    """
    from datetime import date as _date
    d_min = datetime.strptime(date_after,  "%Y-%m-%d").date() if date_after  else None
    d_max = datetime.strptime(date_before, "%Y-%m-%d").date() if date_before else None

    seen = set()
    orfaos = []
    for ordem in order_map.values():
        oid  = str(ordem.get("id",     "") or "")
        onum = str(ordem.get("number", "") or "")
        uniq = oid or onum
        if not uniq or uniq in seen:
            continue
        seen.add(uniq)
        # Pula se alguma orderline já cobre este pedido
        if oid in order_ids_cobertos or onum in order_ids_cobertos:
            continue
        # Filtrar pelo período original (evita órfãos das bordas da janela estendida)
        if d_min or d_max:
            sc = str(ordem.get("source_created") or "")
            try:
                # source_created pode vir como "dd/MM/yyyy" ou "yyyy-MM-dd"
                if "/" in sc:
                    d_ord = datetime.strptime(sc[:10], "%d/%m/%Y").date()
                else:
                    d_ord = datetime.strptime(sc[:10], "%Y-%m-%d").date()
                if d_min and d_ord < d_min: continue
                if d_max and d_ord > d_max: continue
            except (ValueError, TypeError):
                pass
        orfaos.append(ordem)

    if not orfaos:
        print(f"  [órfãos] Nenhum pedido órfão encontrado")
        return pd.DataFrame()

    print(f"  [órfãos] {len(orfaos)} pedidos sem orderlines → criando linhas sintéticas")

    linhas = []
    for ordem in orfaos:
        total_ped   = _n(ordem.get("total"))
        margem_ped  = _n(ordem.get("profit"))
        frete_ped   = _n(ordem.get("shipping_cost"))
        desconto    = _n(ordem.get("discount_subsidy"))
        perc_margem = _n(ordem.get("percentage_margin"))
        resume      = ordem.get("resume_lines") or []
        if not isinstance(resume, list) or not resume:
            resume = [{}]
        n = len(resume)

        try:
            order_id = _i(ordem.get("number"))
        except (TypeError, ValueError):
            order_id = abs(hash(str(ordem.get("number", "")))) % 2_000_000_000

        for idx, rl in enumerate(resume):
            sku  = _s(rl.get("product_code"))
            nome = _s(rl.get("product_name"))
            # Produto ID sintético em faixa > 1B para não colidir com IDs reais
            pid  = abs(hash(f"syn_{sku or idx}_{order_id}")) % 1_000_000_000 + 1_000_000_000

            linhas.append({
                "Order ID"            : order_id,
                "Produto ID"          : pid,
                "Sku"                 : sku,
                "Sku Anterior"        : "",
                "Nome Produto"        : nome,
                "Categoria"           : "",
                "NCM"                 : "",
                "Canal de venda"      : _canal(ordem.get("channel")),
                "Canal Apelido"       : _canal(ordem.get("channel")),
                "Data"                : ordem.get("source_created"),
                "Status"              : _s(ordem.get("status")),
                "Numero Ecommerce"    : _s(ordem.get("channel_order_id")),
                "Cliente"             : "",
                "Estado"              : _s(ordem.get("state")),
                "Cidade"              : "",
                "Metodo Envio"        : "",
                "App Integracao"      : _s(ordem.get("integration")),
                "Quantidade Vendida"  : 1.0,
                "Quantidade Pedido"   : 1.0,
                "Estoque Total"       : 0.0,
                "Custo Total"         : 0.0,
                "Total Venda"         : total_ped / n,
                "Valor Desconto"      : desconto / n,
                "Margem Produto"      : margem_ped / n,
                "Valor Liquido Prod"  : 0.0,
                "Perc Custo"          : 0.0,
                "Perc Margem"         : perc_margem,
                "Credito ICMS"        : 0.0,
                "ICMS Venda"          : 0.0,
                "IPI"                 : 0.0,
                "PIS"                 : 0.0,
                "COFINS"              : 0.0,
                "Simples"             : 0.0,
                "Carga Tributaria"    : 0.0,
                "Imposto Produto"     : 0.0,
                "Frete Pago Prod"     : -(frete_ped / n),
                "Comissao Produto"    : 0.0,
                "Frete Recebido"      : 0.0,
                "Frete Pago"          : 0.0,
                "Comissao Pedido"     : 0.0,
                "Diferenca Frete"     : 0.0,
                "Taxas"               : 0.0,
                "Embalagem"           : 0.0,
                "Repasse Financeiro"  : (total_ped - frete_ped) / n,
                "Margem Contribuicao" : margem_ped / n,
                "Valor Liquido"       : 0.0,
                "Total Custo Pedido"  : 0.0,
                "Total Venda Pedido"  : total_ped,
            })

    if not linhas:
        return pd.DataFrame()

    df = pd.DataFrame(linhas)
    df["Data"] = pd.to_datetime(df["Data"], format="mixed", dayfirst=True, errors="coerce", utc=True)
    df["Data"] = df["Data"].dt.tz_convert("America/Sao_Paulo").dt.tz_localize(None)
    df["Ano"] = df["Data"].dt.year
    df["Mes"] = df["Data"].dt.month

    df["Quantidade"]               = df["Quantidade Vendida"]
    df["Receita Bruta"]            = df["Total Venda"]
    df["Imposto"]                  = df["Imposto Produto"]
    df["Receita Liquida"]          = df["Total Venda"] + df["Imposto Produto"]
    df["CMV"]                      = -df["Custo Total"]
    df["Margem Bruta"]             = df["Receita Liquida"] + df["CMV"]
    _rl                            = df["Receita Liquida"].replace(0, pd.NA)
    df["MB_pct"]                   = (df["Margem Bruta"] / _rl).fillna(0).round(4)
    df["Comissoes"]                = df["Comissao Produto"]
    df["Frete"]                    = df["Frete Pago Prod"]
    df["Margem Contribuicao Calc"] = df["Margem Bruta"] + df["Comissoes"] + df["Frete"]
    df["MC_pct"]                   = (df["Margem Contribuicao Calc"] / _rl).fillna(0).round(4)

    num_cols = [
        "Quantidade Vendida","Quantidade Pedido","Estoque Total","Custo Total",
        "Total Venda","Valor Desconto","Margem Produto","Valor Liquido Prod",
        "Perc Custo","Perc Margem","Imposto Produto","Frete Pago Prod","Comissao Produto",
        "Credito ICMS","ICMS Venda","IPI","PIS","COFINS","Simples","Carga Tributaria",
        "Frete Recebido","Frete Pago","Comissao Pedido","Diferenca Frete","Taxas",
        "Embalagem","Repasse Financeiro","Margem Contribuicao","Valor Liquido",
        "Total Custo Pedido","Total Venda Pedido",
        "Quantidade","Receita Bruta","Imposto","Receita Liquida","CMV",
        "Margem Bruta","MB_pct","Comissoes","Frete","Margem Contribuicao Calc","MC_pct",
    ]
    for col in num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    df["Order ID"]   = pd.to_numeric(df["Order ID"],   errors="coerce").fillna(0).astype(int)
    df["Produto ID"] = pd.to_numeric(df["Produto ID"], errors="coerce").fillna(0).astype(int)
    df = df.drop_duplicates(subset=["Order ID", "Produto ID"])
    print(f"  [órfãos] ✔ {len(df)} linhas sintéticas prontas")
    return df


# ─────────────────────────────────────────────────────────────────
# BANCO DE DADOS
# ─────────────────────────────────────────────────────────────────
def conectar():
    url = (
        f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )
    return create_engine(url, connect_args={"options": "-c client_encoding=utf8"}, pool_pre_ping=True)


def obter_data_ultimo_sync(engine) -> str:
    hoje = datetime.now()
    primeiro_deste_mes = hoje.replace(day=1)
    ultimo_do_mes_anterior = primeiro_deste_mes - timedelta(days=1)
    primeiro_mes_anterior = ultimo_do_mes_anterior.replace(day=1)
    d = primeiro_mes_anterior.strftime("%Y-%m-%d")
    print(f"  [i] Buscando desde: {d} (início do mês anterior)")
    return d


def upsert_banco(engine, df: pd.DataFrame):
    if df.empty:
        print("  [i] Nenhum dado para salvar.")
        return

    print(f"  Salvando {len(df):,} registros no PostgreSQL...")

    # Criar tabela com schema idêntico ao bd_vendas
    with engine.connect() as conn:
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {TABLE} (
                "Order ID"               BIGINT,
                "Produto ID"             BIGINT,
                "Sku"                    TEXT,
                "Sku Anterior"           TEXT,
                "Nome Produto"           TEXT,
                "Categoria"              TEXT,
                "NCM"                    TEXT,
                "Canal de venda"         TEXT,
                "Canal Apelido"          TEXT,
                "Data"                   TIMESTAMP,
                "Status"                 TEXT,
                "Numero Ecommerce"       TEXT,
                "Cliente"                TEXT,
                "Estado"                 TEXT,
                "Cidade"                 TEXT,
                "Metodo Envio"           TEXT,
                "App Integracao"         TEXT,
                "Quantidade Vendida"     NUMERIC,
                "Quantidade Pedido"      NUMERIC,
                "Estoque Total"          NUMERIC,
                "Custo Total"            NUMERIC,
                "Total Venda"            NUMERIC,
                "Valor Desconto"         NUMERIC,
                "Margem Produto"         NUMERIC,
                "Valor Liquido Prod"     NUMERIC,
                "Perc Custo"             NUMERIC,
                "Perc Margem"            NUMERIC,
                "Credito ICMS"           NUMERIC,
                "ICMS Venda"             NUMERIC,
                "IPI"                    NUMERIC,
                "PIS"                    NUMERIC,
                "COFINS"                 NUMERIC,
                "Simples"                NUMERIC,
                "Carga Tributaria"       NUMERIC,
                "Imposto Produto"        NUMERIC,
                "Frete Pago Prod"        NUMERIC,
                "Comissao Produto"       NUMERIC,
                "Frete Recebido"         NUMERIC,
                "Frete Pago"             NUMERIC,
                "Comissao Pedido"        NUMERIC,
                "Diferenca Frete"        NUMERIC,
                "Taxas"                  NUMERIC,
                "Embalagem"              NUMERIC,
                "Repasse Financeiro"     NUMERIC,
                "Margem Contribuicao"    NUMERIC,
                "Valor Liquido"          NUMERIC,
                "Total Custo Pedido"     NUMERIC,
                "Total Venda Pedido"     NUMERIC,
                "Ano"                    INTEGER,
                "Mes"                    INTEGER,
                "Quantidade"             NUMERIC,
                "Receita Bruta"          NUMERIC,
                "Imposto"                NUMERIC,
                "Receita Liquida"        NUMERIC,
                "CMV"                    NUMERIC,
                "Margem Bruta"           NUMERIC,
                "MB_pct"                 NUMERIC,
                "Comissoes"              NUMERIC,
                "Frete"                  NUMERIC,
                "Margem Contribuicao Calc" NUMERIC,
                "MC_pct"                 NUMERIC,
                PRIMARY KEY ("Order ID", "Produto ID")
            )
        """))
        conn.commit()

    # Garante colunas novas em tabela já existente
    new_cols = [
        ("Credito ICMS",    "NUMERIC DEFAULT 0"),
        ("ICMS Venda",      "NUMERIC DEFAULT 0"),
        ("IPI",             "NUMERIC DEFAULT 0"),
        ("PIS",             "NUMERIC DEFAULT 0"),
        ("COFINS",          "NUMERIC DEFAULT 0"),
        ("Simples",         "NUMERIC DEFAULT 0"),
        ("Carga Tributaria","NUMERIC DEFAULT 0"),
    ]
    with engine.begin() as conn:
        for col, typ in new_cols:
            conn.execute(text(
                f'ALTER TABLE {TABLE} ADD COLUMN IF NOT EXISTS "{col}" {typ}'
            ))

    # COPY → temp → upsert
    cols        = list(df.columns)
    cols_quoted = ", ".join(f'"{c}"' for c in cols)
    raw = engine.raw_connection()
    try:
        with raw.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS _tmp_vendas")
            cur.execute(f"CREATE TEMP TABLE _tmp_vendas AS SELECT * FROM {TABLE} LIMIT 0")
            buf = io.StringIO()
            df.to_csv(buf, index=False, header=False, sep="\t", na_rep="\\N",
                      date_format="%Y-%m-%d %H:%M:%S")
            buf.seek(0)
            cur.copy_expert(
                f'COPY _tmp_vendas ({cols_quoted}) FROM STDIN '
                f"WITH (FORMAT CSV, DELIMITER E'\\t', NULL '\\N')",
                buf,
            )
            update_set = ",\n                    ".join(
                f'"{c}" = EXCLUDED."{c}"'
                for c in cols if c not in ("Order ID", "Produto ID")
            )
            cur.execute(f"""
                INSERT INTO {TABLE} ({cols_quoted})
                SELECT {cols_quoted} FROM _tmp_vendas
                ON CONFLICT (\"Order ID\", \"Produto ID\") DO UPDATE SET
                    {update_set}
            """)
        raw.commit()
    finally:
        raw.close()

    # Log de sync
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS sync_log (
                id SERIAL PRIMARY KEY, tabela VARCHAR(100), registros INTEGER,
                data_sync TIMESTAMP DEFAULT NOW(), status VARCHAR(50), origem VARCHAR(100)
            )
        """))
        conn.execute(text(
            "INSERT INTO sync_log (tabela, registros, status, origem) VALUES (:t, :n, 'OK', 'precocerto')"
        ), {"t": TABLE, "n": len(df)})
        conn.commit()

    print(f"  ✔  Upsert concluído  |  sync registrado")


# ─────────────────────────────────────────────────────────────────
# AUTH VIA REQUESTS (sem Playwright)
# ─────────────────────────────────────────────────────────────────
def autenticar_requests() -> requests.Session:
    """Login direto via /authenticate_user_ajax/ — sem abrir browser."""
    s = requests.Session()
    s.get(f"{PC_URL}/login/", timeout=15)
    csrf = s.cookies.get("csrftoken", "")
    r = s.post(
        f"{PC_URL}/authenticate_user_ajax/",
        data={"username_login": PC_EMAIL, "password_login": PC_SENHA,
              "csrfmiddlewaretoken": csrf},
        headers={"Referer": f"{PC_URL}/login/", "X-Requested-With": "XMLHttpRequest"},
        timeout=15,
    )
    r.raise_for_status()
    d = r.json()
    if not d.get("auth"):
        raise RuntimeError(f"Login via requests falhou: {d}")
    sessionid = s.cookies.get("sessionid", "")
    csrf2     = s.cookies.get("csrftoken", csrf)
    print(f"  ✔  Autenticado | sessionid: {sessionid[:20]}...")
    return s


# ─────────────────────────────────────────────────────────────────
# DOWNLOAD EXPORT XLSX
# ─────────────────────────────────────────────────────────────────
def _split_periodo(date_after: str, date_before: str) -> list[tuple[str, str]]:
    """Divide o período em quinzenas para contornar o limite de ~10k linhas do export."""
    from datetime import date as _date
    d0 = datetime.strptime(date_after,  "%Y-%m-%d").date()
    d1 = datetime.strptime(date_before, "%Y-%m-%d").date()
    sub = []
    cur = d0
    while cur <= d1:
        mid = _date(cur.year, cur.month, 15)
        if cur.month == 12:
            next_start = _date(cur.year + 1, 1, 1)
        else:
            next_start = _date(cur.year, cur.month + 1, 1)
        fim_mes = next_start - timedelta(days=1)
        if cur <= mid:
            sub.append((cur.isoformat(), min(mid, d1).isoformat()))
            if mid < d1 and mid < fim_mes:
                sub.append(((mid + timedelta(days=1)).isoformat(), min(fim_mes, d1).isoformat()))
            cur = next_start
        else:
            sub.append((cur.isoformat(), min(fim_mes, d1).isoformat()))
            cur = next_start
    return sub


def baixar_export_xlsx(session: requests.Session, date_after: str, date_before: str) -> pd.DataFrame:
    """
    GET /api/order/export-orders-by-line → aguarda task assíncrona → baixa XLSX do S3.
    Retorna DataFrame com todas as 85 colunas do export.
    Cache em cache/export_{date_after}_{date_before}.xlsx
    Se o export retornar 400 (limite ~10k linhas), divide em quinzenas automaticamente.
    """
    cache_path = os.path.join(CACHE_DIR, f"export_{date_after}_{date_before}.xlsx")

    if not FORCE_DOWNLOAD and os.path.exists(cache_path):
        print(f"    [cache] Lendo {os.path.basename(cache_path)}")
        df = pd.read_excel(cache_path)
        print(f"    ✔  {len(df):,} linhas do cache XLSX")
        return df

    os.makedirs(CACHE_DIR, exist_ok=True)
    csrf = session.cookies.get("csrftoken", "")
    h = {
        "x-csrftoken": csrf,
        "Accept": "application/json",
        "Referer": f"{PC_URL}/gerenciar/pedidos-de-venda/",
    }

    print(f"    Disparando export XLSX para {date_after} → {date_before}...")
    for tentativa in range(3):
        r = session.get(
            f"{PC_URL}/api/order/export-orders-by-line",
            params={
                "source_created": _fmt_date_range(date_after, date_before),
                "date_after":     date_after,
                "date_before":    date_before,
                "id__notin":      "",
            },
            headers=h, timeout=30,
        )
        if r.status_code in (400, 401, 403):
            if tentativa < 2:
                print(f"    [sessão/limite {r.status_code}] Re-autenticando (tentativa {tentativa+1})...")
                new_s = autenticar_requests()
                session.cookies.update(new_s.cookies)
                csrf = session.cookies.get("csrftoken", "")
                h["x-csrftoken"] = csrf
                continue
            else:
                # 3 tentativas falharam → dividir em quinzenas e combinar
                subs = _split_periodo(date_after, date_before)
                print(f"    [limite export] Dividindo em {len(subs)} quinzenas...")
                dfs = []
                for s_after, s_before in subs:
                    df_sub = baixar_export_xlsx(session, s_after, s_before)
                    if not df_sub.empty:
                        dfs.append(df_sub)
                if not dfs:
                    return pd.DataFrame()
                combined = pd.concat(dfs, ignore_index=True)
                combined.to_excel(cache_path, index=False)
                print(f"    ✔  {len(combined):,} linhas combinadas de {len(subs)} quinzenas")
                return combined
        r.raise_for_status()
        break
    task_id = r.json()["task_id"]
    print(f"    task_id: {task_id[:12]}... aguardando geração...")

    download_url = None
    for attempt in range(72):
        time.sleep(5)
        tr = session.get(f"{PC_URL}/api/task/result/{task_id}", headers=h, timeout=10)
        tr.raise_for_status()
        d  = tr.json()
        status = d.get("status", "?")
        if attempt % 6 == 0:
            print(f"    [{attempt * 5:>3}s] status={status}")
        if status == "SUCCESS":
            download_url = json.loads(d["result"])
            break
        if status in ("FAILURE", "REVOKED"):
            raise RuntimeError(f"Export task falhou: {d}")

    if not download_url:
        raise RuntimeError("Timeout (6 min) aguardando export XLSX")

    print(f"    Download XLSX do S3...")
    resp = requests.get(download_url, timeout=120)
    resp.raise_for_status()

    with open(cache_path, "wb") as f:
        f.write(resp.content)

    df = pd.read_excel(io.BytesIO(resp.content))
    print(f"    ✔  {len(df):,} linhas | {df['Número do pedido'].nunique():,} pedidos únicos")
    return df


# ─────────────────────────────────────────────────────────────────
# PROCESSAR XLSX → DataFrame bd_vendas
# ─────────────────────────────────────────────────────────────────
def processar_xlsx(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Mapeia as colunas do XLSX exportado diretamente para o schema de bd_vendas.
    Todos os impostos já vêm calculados — não precisa de /api/order.
    """
    if df_raw.empty:
        return pd.DataFrame()

    def _n(col):
        return pd.to_numeric(df_raw.get(col, 0), errors="coerce").fillna(0)

    def _s(col):
        return df_raw.get(col, pd.Series([""] * len(df_raw), dtype=str)).fillna("").astype(str)

    qty              = _n("Quantidade vendida")
    custo_total      = _n("Custo unitário") * qty
    preco_linha      = _n("Preço unitário") * qty
    comissao         = _n("Comissão unitária") * qty
    desc_bruto       = _n("Desconto unitário") * qty
    subsidio_desc    = _n("Subsídio de desconto unitário") * qty
    val_desconto     = desc_bruto - subsidio_desc

    # Frete: "Gastos de frete do pedido" é valor por pedido (igual em todas as linhas do mesmo pedido)
    # → distribuir proporcionalmente ao preço de cada linha dentro do pedido
    _frete_ped   = _n("Gastos de frete do pedido")
    _soma_preco  = preco_linha.groupby(df_raw["Número do pedido"]).transform("sum").replace(0, pd.NA)
    frete_prod   = ((preco_linha / _soma_preco).fillna(0) * _frete_ped)

    # Total Venda = Preço unitário × Quantidade (bruto, antes de descontos/frete)
    total_venda = preco_linha

    # Impostos já calculados pelo Preço Certo
    ipi          = _n("IPI")
    credito_icms = _n("Crédito de ICMS")
    icms_venda   = _n("ICMS de venda")
    icms_difal   = _n("ICMS DIFAL")
    icms_fcp     = _n("ICMS FCP")
    pis          = _n("PIS")
    cofins       = _n("COFINS")
    cred_pis     = _n("Crédito de PIS")
    cred_cofins  = _n("Crédito de COFINS")
    outros_cred  = _n("Outros créditos")
    carga_trib   = (ipi + icms_venda + pis + cofins + icms_difal + icms_fcp
                    - credito_icms - cred_pis - cred_cofins - outros_cred)
    imposto_prod = -carga_trib

    df = pd.DataFrame({
        "Order ID"           : pd.to_numeric(df_raw["Número do pedido"], errors="coerce").fillna(0).astype(int),
        "Produto ID"         : pd.to_numeric(df_raw.get("ID do produto no pedido", 0), errors="coerce").fillna(0).astype(int),
        "Sku"                : _s("Código do produto"),
        "Sku Anterior"       : "",
        "Nome Produto"       : _s("Nome do produto"),
        "Categoria"          : "",
        "NCM"                : "",
        "Canal de venda"     : _s("Canal de venda"),
        "Canal Apelido"      : _s("Canal de venda"),
        "Data"               : pd.to_datetime(df_raw["Data do pedido"], errors="coerce"),
        "Status"             : _s("Situação"),
        "Numero Ecommerce"   : _s("Número do pedido no Canal"),
        "Cliente"            : "",
        "Estado"             : _s("Estado de destino"),
        "Cidade"             : "",
        "Metodo Envio"       : _s("Tipo de envio"),
        "App Integracao"     : _s("Integração"),
        "Quantidade Vendida" : qty,
        "Quantidade Pedido"  : qty,
        "Estoque Total"      : 0.0,
        "Custo Total"        : custo_total,
        "Total Venda"        : total_venda,
        "Valor Desconto"     : val_desconto,
        "Margem Produto"     : _n("Lucro total do produto"),
        "Valor Liquido Prod" : 0.0,
        "Perc Custo"         : 0.0,
        "Perc Margem"        : _n("Margem do produto"),
        "Credito ICMS"       : credito_icms,
        "ICMS Venda"         : icms_venda,
        "IPI"                : ipi,
        "PIS"                : pis,
        "COFINS"             : cofins,
        "Simples"            : 0.0,
        "Carga Tributaria"   : carga_trib,
        "Imposto Produto"    : imposto_prod,
        "Frete Pago Prod"    : -frete_prod,
        "Comissao Produto"   : -comissao,
        "Frete Recebido"     : 0.0,
        "Frete Pago"         : 0.0,
        "Comissao Pedido"    : 0.0,
        "Diferenca Frete"    : 0.0,
        "Taxas"              : _n("Percentual de ICMS de venda"),
        "Embalagem"          : 0.0,
        "Repasse Financeiro" : total_venda - comissao - frete_prod,
        "Margem Contribuicao": _n("Lucro do pedido"),
        "Valor Liquido"      : 0.0,
        "Total Custo Pedido" : 0.0,
        "Total Venda Pedido" : _n("Total do pedido"),
    })

    df["Data"] = pd.to_datetime(df["Data"], errors="coerce")
    df["Ano"]  = df["Data"].dt.year
    df["Mes"]  = df["Data"].dt.month

    df["Quantidade"]               = df["Quantidade Vendida"]
    df["Receita Bruta"]            = df["Total Venda"]
    df["Imposto"]                  = df["Imposto Produto"]
    df["Receita Liquida"]          = df["Total Venda"] + df["Imposto Produto"]
    df["CMV"]                      = -df["Custo Total"]
    df["Margem Bruta"]             = df["Receita Liquida"] + df["CMV"]
    _rl                            = df["Receita Liquida"].replace(0, pd.NA)
    df["MB_pct"]                   = (df["Margem Bruta"] / _rl).fillna(0).round(4)
    df["Comissoes"]                = df["Comissao Produto"]
    df["Frete"]                    = df["Frete Pago Prod"]
    df["Margem Contribuicao Calc"] = df["Margem Bruta"] + df["Comissoes"] + df["Frete"]
    df["MC_pct"]                   = (df["Margem Contribuicao Calc"] / _rl).fillna(0).round(4)

    num_cols = [
        "Quantidade Vendida","Quantidade Pedido","Estoque Total","Custo Total",
        "Total Venda","Valor Desconto","Margem Produto","Valor Liquido Prod",
        "Perc Custo","Perc Margem","Imposto Produto","Frete Pago Prod","Comissao Produto",
        "Credito ICMS","ICMS Venda","IPI","PIS","COFINS","Simples","Carga Tributaria",
        "Frete Recebido","Frete Pago","Comissao Pedido","Diferenca Frete","Taxas",
        "Embalagem","Repasse Financeiro","Margem Contribuicao","Valor Liquido",
        "Total Custo Pedido","Total Venda Pedido",
        "Quantidade","Receita Bruta","Imposto","Receita Liquida","CMV",
        "Margem Bruta","MB_pct","Comissoes","Frete","Margem Contribuicao Calc","MC_pct",
    ]
    for col in num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    df = df[df["Produto ID"] > 0]
    df = df.drop_duplicates(subset=["Order ID", "Produto ID"])
    print(f"  ✔  {len(df):,} linhas processadas | {df['Order ID'].nunique():,} pedidos únicos")
    return df


# ─────────────────────────────────────────────────────────────────
# JANELAS MENSAIS (para full reload sem estourar 10k)
# ─────────────────────────────────────────────────────────────────
def gerar_janelas(date_after: str, date_before: str) -> list[tuple[str, str]]:
    from datetime import date
    start = datetime.strptime(date_after, "%Y-%m-%d").date()
    end   = datetime.strptime(date_before, "%Y-%m-%d").date()
    janelas = []
    cursor = start
    while cursor <= end:
        # Último dia do mês
        if cursor.month == 12:
            month_end = date(cursor.year + 1, 1, 1) - timedelta(days=1)
        else:
            month_end = date(cursor.year, cursor.month + 1, 1) - timedelta(days=1)
        month_end = min(month_end, end)
        janelas.append((cursor.isoformat(), month_end.isoformat()))
        cursor = month_end + timedelta(days=1)
    return janelas


# ─────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────
def main():
    inicio   = datetime.now()
    end_date = DATE_BEFORE if DATE_BEFORE else datetime.today().strftime("%Y-%m-%d")

    _sep("═")
    print("  S&OP Intelligence  ·  Preco Certo ETL")
    print(f"  Início: {inicio:%d/%m/%Y  %H:%M:%S}")
    _sep("═")

    # ── Etapa 1: Auth ─────────────────────────────────────────────
    if REPROCESS:
        print("  Modo: REPROCESS — usando cache local, sem chamadas à API")
        auth = None
    else:
        _step(1, 4, "Autenticando no Preco Certo via Browser")
        auth = autenticar_playwright()

    # ── Etapa 2: Banco ────────────────────────────────────────────
    _step(2, 4, "Conectando ao banco e criando tabela")
    engine = conectar()
    with engine.connect() as conn:
        pass  # testa conexão
    print("  ✔  PostgreSQL conectado")

    # ── Etapa 3: Calcular janelas ─────────────────────────────────
    _step(3, 4, "Calculando período")

    if REPROCESS or FULL_RELOAD:
        first_date = FIRST_DATE
        print(f"  Modo: {'REPROCESS' if REPROCESS else 'FULL RELOAD'} desde {first_date}")
    else:
        first_date = obter_data_ultimo_sync(engine)

    janelas = gerar_janelas(first_date, end_date)
    print(f"  Janelas mensais: {len(janelas)}  ({first_date} → {end_date})")
    for j_after, j_before in janelas:
        print(f"    • {j_after} → {j_before}")

    # ── Etapa 4: Download + Upsert por janela ─────────────────────
    _step(4, 4, "Baixando e salvando pedidos por janela mensal")
    total_inserido = 0

    for i, (d_after, d_before) in enumerate(janelas, 1):
        ts = datetime.now().strftime("%H:%M:%S")
        print(f"\n[{ts}] ─── Janela {i}/{len(janelas)}: {d_after} → {d_before}")

        # SEMPRE re-download na última janela (pedidos do dia atual podem ter mudado)
        is_ultima = (i == len(janelas))
        if is_ultima:
            cache_path = os.path.join(CACHE_DIR, f"export_{d_after}_{d_before}.xlsx")
            if os.path.exists(cache_path):
                os.remove(cache_path)
                print(f"    [cache] Apagado cache da última janela → re-download forçado")

        # Usar o auth dict já retornado pelo playwright
        if not REPROCESS and not auth:
            print("Erro: sem autenticação")
            continue

        try:
            print("    [fallback] Usando API paginada devido a falha no export...")
            linhas_api = baixar_todos(auth, d_after, d_before)
            if not linhas_api:
                df = pd.DataFrame()
            else:
                orders_map = baixar_orders(auth, d_after, d_before)
                df = processar(linhas_api, orders_map)
        except Exception as e:
            print(f"  [ERRO api] {e}")
            continue

        # Sempre apagar registros do período antes de inserir para evitar duplicação (idempotente)
        with engine.begin() as conn:
            deleted = conn.execute(
                text(f'DELETE FROM "{TABLE}" WHERE "Data" >= :d1 AND "Data" < :d2'),
                {"d1": d_after, "d2": (datetime.strptime(d_before, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")}
            ).rowcount
            if deleted:
                print(f"  [clean] {deleted:,} registros antigos removidos do período")

        if df.empty:
            print(f"  → 0 linhas nesta janela, pulando")
            continue

        if not df.empty:
            upsert_banco(engine, df)
            total_inserido += len(df)
            print(f"  ✔  Janela {i}/{len(janelas)} concluída — {len(df):,} registros | total acumulado: {total_inserido:,}")

    duracao = int((datetime.now() - inicio).total_seconds())
    _sep("═")
    print(f"  ✔  ETL finalizado em {duracao}s")
    print(f"  Total inserido/atualizado: {total_inserido:,} registros")
    print(f"  {datetime.now():%d/%m/%Y %H:%M:%S}")
    _sep("═")


if __name__ == "__main__":
    main()
