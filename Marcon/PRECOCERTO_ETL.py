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
PC_EMAIL     = os.getenv("PRECOCERTO_EMAIL",    "comercial@casaeletromarcon.com.br")
PC_SENHA     = os.getenv("PRECOCERTO_PASSWORD", "eletro123")
FULL_RELOAD  = bool(os.getenv("FULL_RELOAD"))
OVERLAP_DAYS = int(os.getenv("OVERLAP_DAYS", "2"))
FIRST_DATE   = os.getenv("PRECOCERTO_FIRST_DATE", "2025-01-01")
HEADED       = bool(os.getenv("HEADED"))    # HEADED=1 para ver o browser
PAGE_LIMIT   = 100     # páginas menores = mais estável
TABLE        = "bd_vendas"


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
        page.goto(f"{PC_URL}/login/", wait_until="networkidle", timeout=40_000)
        page.wait_for_timeout(1500)

        print(f"  Preenchendo credenciais ...")
        page.fill("input[name='username_login']", PC_EMAIL, timeout=10_000)
        page.fill("input[name='password_login']", PC_SENHA)
        page.click("button[type='submit']")

        # Aguardar redirect para dashboard (indica login bem-sucedido)
        print(f"  Aguardando redirect para dashboard ...")
        try:
            page.wait_for_url("**/dashboard**", timeout=25_000)
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
    """
    Headers para autenticação via Django Session.
    O sessionid é enviado no Cookie header, não no Authorization.
    """
    return {
        "Accept":        "application/json, text/plain, */*",
        "Origin":        PC_URL,
        "Referer":       f"{PC_URL}/gerenciar/pedidos-de-venda/",
        "User-Agent":    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124",
        "Accept-Language": "pt-BR,pt;q=0.9",
        "x-csrftoken":   auth["csrftoken"],
        "Cookie":        auth["cookie_str"],
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
            r = requests.get(url, headers=_api_headers(auth), params=params, timeout=90)
            if r.status_code == 200:
                return r.json()
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


WORKERS = 10  # 10 paralelos com pages de 100


def baixar_todos(auth: dict, date_after: str, date_before: str) -> list[dict]:
    from concurrent.futures import ThreadPoolExecutor, as_completed

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
    return todos


def baixar_orders(auth: dict, date_after: str, date_before: str) -> dict:
    """Busca /api/order → dict {number: row} para enriquecer Total Venda, Status e Frete."""
    from concurrent.futures import ThreadPoolExecutor, as_completed
    params = {
        "source_created": _fmt_date_range(date_after, date_before),
        "date_after":     date_after,
        "date_before":    date_before,
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
            oid = str(o.get("id", ""))
            if oid:
                order_map[oid] = o
    print(f"    ✔  {len(order_map):,} pedidos /api/order carregados")
    return order_map


# ─────────────────────────────────────────────────────────────────
# TRANSFORMAÇÃO → DATAFRAME  (mesmo schema de bd_vendas)
# ─────────────────────────────────────────────────────────────────
def _s(v, default=""):
    return str(v).strip() if v is not None else default

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

    # Pré-passo: somar custo total por order_id (para distribuição proporcional)
    custo_por_pedido: dict[str, float] = {}
    for L in linhas_api:
        oid_key  = str(L.get("order_id", "") or L.get("number", ""))
        qtd      = _n(L.get("quantity"))
        uc       = _n(L.get("unit_cost") or L.get("product_cost"))
        ct       = _n(L.get("product_cost") or uc * qtd)
        custo_por_pedido[oid_key] = custo_por_pedido.get(oid_key, 0.0) + ct

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
            status_venda     = _s(ordem.get("status"))
            frete_prod       = _n(ordem.get("shipping_cost"))
            perc_margem      = _n(ordem.get("percentage_margin"))
            valor_desconto   = _n(ordem.get("discount_subsidy"))
            total_venda_ped  = _n(ordem.get("total"))
            margem_contrib   = _n(ordem.get("profit"))
            numero_ecommerce = _s(ordem.get("channel_order_id") or L.get("number"))
            if ordem.get("number"):
                order_id = _i(ordem.get("number"))  # número visível no Preco Certo UI
            lines_count      = _i(ordem.get("lines_count", 0))
            if lines_count <= 1:
                total_venda    = total_venda_ped
                margem_produto = margem_contrib
            else:
                # Multi-produto: distribuir total e margem proporcionalmente ao custo da linha
                total_custo_ped = custo_por_pedido.get(str(L.get("order_id", "") or number), 0.0)
                ratio = (custo_total / total_custo_ped) if total_custo_ped > 0 else 0.0
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
            "Canal de venda"      : _s(L.get("channel")),
            "Canal Apelido"       : _s(L.get("channel")),
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
            "Imposto Produto"     : carga_trib if carga_trib != 0.0 else simples,
            "Frete Pago Prod"     : frete_prod,
            "Comissao Produto"    : comissao,
            "Frete Recebido"      : 0.0,
            "Frete Pago"          : 0.0,
            "Comissao Pedido"     : 0.0,
            "Diferenca Frete"     : 0.0,
            "Taxas"               : _n(icms_pct),
            "Embalagem"           : 0.0,
            "Repasse Financeiro"  : 0.0,
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
# BANCO DE DADOS
# ─────────────────────────────────────────────────────────────────
def conectar():
    url = (
        f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )
    return create_engine(url, connect_args={"options": "-c client_encoding=utf8"}, pool_pre_ping=True)


def obter_data_ultimo_sync(engine) -> str:
    try:
        with engine.connect() as conn:
            resultado = conn.execute(
                text(f'SELECT MAX("Data") FROM {TABLE}')
            ).scalar()
        if resultado:
            data_base = pd.to_datetime(resultado) - timedelta(days=OVERLAP_DAYS)
            data_str  = data_base.strftime("%Y-%m-%d")
            print(f"  ✔  Último registro: {pd.to_datetime(resultado).strftime('%d/%m/%Y')}")
            print(f"     Buscando desde: {data_str} (+{OVERLAP_DAYS}d sobreposição)")
            return data_str
    except Exception:
        pass
    print("  [i] Tabela vazia → carga inicial completa")
    return FIRST_DATE


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
    end_date = datetime.today().strftime("%Y-%m-%d")

    _sep("═")
    print("  S&OP Intelligence  ·  Preco Certo ETL")
    print(f"  Início: {inicio:%d/%m/%Y  %H:%M:%S}")
    _sep("═")

    # ── Etapa 1: Auth ─────────────────────────────────────────────
    _step(1, 4, "Autenticando no Preco Certo (Playwright)")
    auth = autenticar_playwright()

    # ── Etapa 2: Banco ────────────────────────────────────────────
    _step(2, 4, "Conectando ao banco e criando tabela")
    engine = conectar()
    with engine.connect() as conn:
        pass  # testa conexão
    print("  ✔  PostgreSQL conectado")

    # ── Etapa 3: Calcular janelas ─────────────────────────────────
    _step(3, 4, "Calculando período")

    if FULL_RELOAD:
        first_date = FIRST_DATE
        print(f"  Modo: FULL RELOAD desde {first_date}")
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

        try:
            pedidos = baixar_todos(auth, d_after, d_before)
        except RuntimeError as e:
            print(f"  [ERRO orderlines] {e}")
            continue

        try:
            orders = baixar_orders(auth, d_after, d_before)
        except RuntimeError as e:
            print(f"  [AVISO orders] {e} — processando sem enriquecimento")
            orders = {}

        if not pedidos:
            print(f"  → 0 orderlines nesta janela, pulando")
            continue

        print(f"  Processando e salvando...")
        df = processar(pedidos, orders)
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
