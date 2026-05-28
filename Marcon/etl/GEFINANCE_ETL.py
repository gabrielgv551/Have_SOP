"""
╔══════════════════════════════════════════════════════════════╗
║      S&OP Intelligence · Gefinance ETL v4 (Paralelo)         ║
║  Download paralelo com ThreadPoolExecutor                    ║
║  Carga incremental automática + upsert                       ║
╚══════════════════════════════════════════════════════════════╝

DEPENDÊNCIAS:
  pip install requests pandas sqlalchemy psycopg2-binary
"""

import io
import os
import sys
import time
import threading
import requests
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy import create_engine, text
from sqlalchemy.types import BigInteger, Numeric, Text, TIMESTAMP, Integer


# ─────────────────────────────────────────────────────────────────
# SPINNER / LOADING
# ─────────────────────────────────────────────────────────────────
class Spinner:
    _frames = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]

    def __init__(self, msg: str):
        self.msg     = msg
        self._stop   = threading.Event()
        self._thread = threading.Thread(target=self._spin, daemon=True)
        self._start  = time.time()

    def _spin(self):
        i = 0
        while not self._stop.is_set():
            elapsed = time.time() - self._start
            frame   = self._frames[i % len(self._frames)]
            sys.stdout.write(f"\r  {frame}  {self.msg}  ({elapsed:.1f}s)   ")
            sys.stdout.flush()
            time.sleep(0.1)
            i += 1

    def __enter__(self):
        self._thread.start()
        return self

    def __exit__(self, *_):
        self._stop.set()
        self._thread.join()
        elapsed = time.time() - self._start
        sys.stdout.write(f"\r  ✔  {self.msg}  ({elapsed:.1f}s)        \n")
        sys.stdout.flush()


def _sep(char="─", n=60):
    print(char * n)


def _step(n: int, total: int, msg: str):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"\n[{ts}] ── Etapa {n}/{total}: {msg}")

# ─────────────────────────────────────────────────────────────────
# CONFIGURAÇÃO
# ─────────────────────────────────────────────────────────────────
DB_CONFIG = {
    "host"    : os.getenv("MARCON_HOST",     ""),
    "port"    : os.getenv("MARCON_PORT",     5432),
    "database": os.getenv("MARCON_DB",       "Marcon"),
    "user"    : os.getenv("MARCON_USER",     "postgres"),
    "password": os.getenv("MARCON_PASSWORD", ""),
}

BASE_URL        = "https://gateway-web.ge.finance/api"
APP_ORIGIN      = "https://app.ge.finance"
PAGE_SIZE       = 500
FIRST_DATE_FULL = "2025-01-01"
OVERLAP_DAYS    = 2

# Quantas páginas buscar em paralelo
# 10 é seguro — aumente para 20 se quiser mais velocidade
WORKERS = 3


# ─────────────────────────────────────────────────────────────────
# LOGIN
# ─────────────────────────────────────────────────────────────────
def login(email: str, password: str) -> dict:
    url = f"{BASE_URL}/Auth/login"
    headers = {
        "Content-Type" : "application/json",
        "Accept"       : "application/json, text/plain, */*",
        "Origin"       : APP_ORIGIN,
        "Referer"      : f"{APP_ORIGIN}/",
        "language"     : "pt-BR",
        "User-Agent"   : "Mozilla/5.0 Chrome/146.0.0.0 Safari/537.36",
    }
    resp = requests.post(
        url,
        json={"username": email, "password": password},
        headers=headers,
        timeout=20,
    )
    resp.raise_for_status()
    data = resp.json()

    token = (
        data.get("token")
        or data.get("access_token")
        or data.get("accessToken")
        or (data.get("data") or {}).get("token")
    )
    customer_id = data.get("customerId") or (data.get("data") or {}).get("customerId") or ""
    plan_id     = data.get("customerPlanId") or (data.get("data") or {}).get("customerPlanId") or "507"

    if not token:
        raise RuntimeError(f"Login OK mas sem token: {data}")

    print(f"  ✔  Login bem-sucedido  (customerId={customer_id})")
    return {"token": token, "customerId": str(customer_id), "planId": str(plan_id)}


def _headers(auth: dict) -> dict:
    return {
        "Authorization"  : f"Bearer {auth['token']}",
        "customerid"     : auth["customerId"],
        "customerplanid" : auth["planId"],
        "language"       : "pt-BR",
        "Accept"         : "application/json, text/plain, */*",
        "Origin"         : APP_ORIGIN,
        "Referer"        : f"{APP_ORIGIN}/",
        "User-Agent"     : "Mozilla/5.0 Chrome/146.0.0.0 Safari/537.36",
    }


# ─────────────────────────────────────────────────────────────────
# BUSCAR UMA PÁGINA
# ─────────────────────────────────────────────────────────────────
def buscar_pagina(auth: dict, pagina: int, first_date: str, end_date: str) -> tuple[int, list]:
    """Retorna (numero_pagina, lista_de_pedidos)"""
    url = f"{BASE_URL}/SpreadSheet"
    params = {
        "pageSize"       : str(PAGE_SIZE),
        "firstDate"      : first_date,
        "endDate"        : end_date,
        "refreshDate"    : datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000"),
        "sortColumn"     : "date",
        "sortType"       : "2",
        "currentPage"    : str(pagina),
        "customerId"     : auth["customerId"],
        "customerPlanId" : auth["planId"],
        "isTrial"        : "false",
    }

    # Retry automático em caso de erro
    for tentativa in range(3):
        try:
            resp = requests.get(url, headers=_headers(auth), params=params, timeout=60)
            resp.raise_for_status()
            dados = resp.json()
            return pagina, dados.get("result", [])
        except Exception as e:
            if tentativa == 2:
                print(f"   [ERRO] Página {pagina} falhou após 3 tentativas: {e}")
                return pagina, []
            time.sleep(2)


# ─────────────────────────────────────────────────────────────────
# DOWNLOAD PARALELO
# ─────────────────────────────────────────────────────────────────
def baixar_todos(auth: dict, first_date: str, end_date: str) -> list:
    print(f"\n  Período: {first_date}  →  {end_date}")

    # Página 1 para descobrir o total
    _, pedidos_p1 = buscar_pagina(auth, 1, first_date, end_date)

    # Refaz a primeira chamada para pegar os metadados
    url = f"{BASE_URL}/SpreadSheet"
    params = {
        "pageSize"       : str(PAGE_SIZE),
        "firstDate"      : first_date,
        "endDate"        : end_date,
        "refreshDate"    : datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000"),
        "sortColumn"     : "date",
        "sortType"       : "2",
        "currentPage"    : "1",
        "customerId"     : auth["customerId"],
        "customerPlanId" : auth["planId"],
        "isTrial"        : "false",
    }
    meta        = requests.get(url, headers=_headers(auth), params=params, timeout=60).json()
    total_items = meta.get("totalItems", 0)
    total_pages = meta.get("totalPages", 1)
    pedidos_p1  = meta.get("result", [])

    print(f"       Total de pedidos : {total_items:,}")
    print(f"       Total de páginas : {total_pages} (pageSize={PAGE_SIZE})")
    print(f"       Workers paralelos: {WORKERS}")

    if total_pages == 1:
        print(f"  ✔  Apenas 1 página — {len(pedidos_p1):,} pedidos")
        return pedidos_p1

    # Baixa as páginas restantes em paralelo
    todos_pedidos = list(pedidos_p1)
    paginas_restantes = list(range(2, total_pages + 1))
    concluidas = 1

    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        futuros = {
            executor.submit(buscar_pagina, auth, p, first_date, end_date): p
            for p in paginas_restantes
        }

        # Coleta resultados na ordem que chegam (mais rápido)
        resultados = {}
        for futuro in as_completed(futuros):
            pagina, resultado = futuro.result()
            resultados[pagina] = resultado
            concluidas += 1
            total_acumulado = sum(len(v) for v in resultados.values()) + len(pedidos_p1)
            pct = int(concluidas / total_pages * 30)
            bar = "█" * pct + "░" * (30 - pct)
            sys.stdout.write(f"\r  [{bar}] {concluidas}/{total_pages} páginas  |  {total_acumulado:,} registros   ")
            sys.stdout.flush()

    # Reordena e achata
    for pagina in sorted(resultados.keys()):
        todos_pedidos.extend(resultados[pagina])

    print()  
    print(f"  ✔  Download concluído: {len(todos_pedidos):,} pedidos")
    return todos_pedidos


# ─────────────────────────────────────────────────────────────────
# PROCESSAR → DATAFRAME FLAT
# ─────────────────────────────────────────────────────────────────
def processar(pedidos: list) -> pd.DataFrame:
    print(f"  Processando {len(pedidos):,} pedidos...")
    linhas = []

    for pedido in pedidos:
        base = {
            "Order ID"            : pedido.get("id"),
            "Canal de venda"      : pedido.get("salesChannel"),
            "Canal Apelido"       : pedido.get("channelNickName"),
            "Data"                : pedido.get("date"),
            "Status"              : pedido.get("status"),
            "Numero Ecommerce"    : pedido.get("ecommerceNumber"),
            "Cliente"             : pedido.get("client"),
            "Estado"              : pedido.get("state"),
            "Cidade"              : pedido.get("city"),
            "Metodo Envio"        : pedido.get("shippingMethod"),
            "App Integracao"      : pedido.get("appIntegrationName"),
            "Frete Recebido"      : pedido.get("receivedFreight", 0),
            "Frete Pago"          : pedido.get("paidFreight", 0),
            "Comissao Pedido"     : pedido.get("commission", 0),
            "Diferenca Frete"     : pedido.get("freightDifference", 0),
            "Taxas"               : pedido.get("fees", 0),
            "Embalagem"           : pedido.get("package", 0),
            "Repasse Financeiro"  : pedido.get("financialTransfer", 0),
            "Margem Contribuicao" : pedido.get("margin", 0),
            "Valor Liquido"       : pedido.get("liquidValue", 0),
            "Total Custo Pedido"  : pedido.get("totalCost", 0),
            "Total Venda Pedido"  : pedido.get("totalSale", 0),
        }

        produtos = pedido.get("products", [])
        if not produtos:
            linhas.append(base)
            continue

        for produto in produtos:
            linha = base.copy()
            linha.update({
                "Produto ID"         : produto.get("productId"),
                "Sku"                : produto.get("sku"),
                "Sku Anterior"       : produto.get("previousSKU"),
                "Nome Produto"       : produto.get("productName"),
                "Categoria"          : produto.get("category"),
                "NCM"                : produto.get("ncm"),
                "Quantidade Vendida" : produto.get("soldAmount", 0),
                "Quantidade Pedido"  : produto.get("orderQuantity", 0),
                "Estoque Total"      : produto.get("totalStock", 0),
                "Custo Total"        : produto.get("totalCost", 0),
                "Total Venda"        : produto.get("totalSale", 0),
                "Valor Desconto"     : produto.get("valueWithDiscount", 0),
                "Margem Produto"     : produto.get("margin", 0),
                "Valor Liquido Prod" : produto.get("liquidValue", 0),
                "Perc Custo"         : produto.get("costPercentage", 0),
                "Perc Margem"        : produto.get("salePercentage", 0),
                "Imposto Produto"    : produto.get("tax", 0),
                "Frete Pago Prod"    : produto.get("paidFreight", 0),
                "Comissao Produto"   : produto.get("commission", 0),
            })
            linhas.append(linha)

    df = pd.DataFrame(linhas)

    df["Data"] = pd.to_datetime(df["Data"], format="mixed", dayfirst=True, errors="coerce")
    if df["Data"].dt.tz is not None:
        df["Data"] = df["Data"].dt.tz_convert("America/Sao_Paulo").dt.tz_localize(None)
    df["Ano"]  = df["Data"].dt.year
    df["Mes"]  = df["Data"].dt.month

    numericas = [
        "Quantidade Vendida", "Quantidade Pedido", "Estoque Total",
        "Custo Total", "Total Venda", "Margem Produto", "Valor Liquido Prod",
        "Imposto Produto", "Frete Pago Prod", "Comissao Produto",
        "Repasse Financeiro", "Margem Contribuicao", "Valor Liquido",
        "Total Custo Pedido", "Total Venda Pedido",
    ]
    for col in numericas:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    for col in ["Sku", "Status", "Canal de venda", "Nome Produto"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    df["Order ID"]   = pd.to_numeric(df.get("Order ID"),   errors="coerce").fillna(0).astype(int)
    df["Produto ID"] = pd.to_numeric(df.get("Produto ID"), errors="coerce").fillna(0).astype(int)

    df = df.drop_duplicates(subset=["Order ID", "Produto ID"])
    print(f"  ✔  {len(df):,} registros válidos")

    # ── DRE por produto ───────────────────────────────────────────────────────────────────
    df["Quantidade"]             = df["Quantidade Vendida"]
    df["Receita Bruta"]          = df["Total Venda"]
    df["Imposto"]                = df["Imposto Produto"]
    df["Receita Liquida"]        = df["Total Venda"] + df["Imposto Produto"]
    df["CMV"]                    = -df["Custo Total"] + df["Embalagem"]
    df["Margem Bruta"]           = df["Receita Liquida"] + df["CMV"]
    _rl                          = df["Receita Liquida"].replace(0, pd.NA)
    df["MB_pct"]                 = (df["Margem Bruta"] / _rl).fillna(0).round(4)
    df["Comissoes"]              = df["Comissao Produto"]
    df["Frete"]                  = df["Frete Pago Prod"]
    df["Margem Contribuicao Calc"] = df["Margem Bruta"] + df["Comissoes"] + df["Frete"]
    df["MC_pct"]                 = (df["Margem Contribuicao Calc"] / _rl).fillna(0).round(4)

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
            resultado = conn.execute(text('SELECT MAX("Data") FROM bd_vendas')).scalar()
        if resultado:
            data_base = pd.to_datetime(resultado) - timedelta(days=OVERLAP_DAYS)
            data_str  = data_base.strftime("%Y-%m-%d")
            print(f"  ✔  Último registro no banco: {pd.to_datetime(resultado).strftime('%d/%m/%Y')}")
            print(f"     Buscando desde: {data_str} (com {OVERLAP_DAYS}d de sobreposição)")
            return data_str
    except Exception:
        pass
    print("[i] Tabela vazia ou inexistente → carga inicial completa")
    return FIRST_DATE_FULL


def upsert_banco(engine, df: pd.DataFrame):
    print(f"  Salvando {len(df):,} registros no PostgreSQL...")

    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS bd_vendas (
                "Order ID"            BIGINT,
                "Produto ID"          BIGINT,
                "Sku"                 TEXT,
                "Sku Anterior"        TEXT,
                "Nome Produto"        TEXT,
                "Categoria"          TEXT,
                "NCM"                 TEXT,
                "Canal de venda"      TEXT,
                "Canal Apelido"       TEXT,
                "Data"                TIMESTAMP,
                "Status"              TEXT,
                "Numero Ecommerce"    TEXT,
                "Cliente"             TEXT,
                "Estado"              TEXT,
                "Cidade"              TEXT,
                "Metodo Envio"        TEXT,
                "App Integracao"      TEXT,
                "Quantidade Vendida"  NUMERIC,
                "Quantidade Pedido"   NUMERIC,
                "Estoque Total"       NUMERIC,
                "Custo Total"         NUMERIC,
                "Total Venda"         NUMERIC,
                "Valor Desconto"      NUMERIC,
                "Margem Produto"      NUMERIC,
                "Valor Liquido Prod"  NUMERIC,
                "Perc Custo"          NUMERIC,
                "Perc Margem"         NUMERIC,
                "Imposto Produto"     NUMERIC,
                "Frete Pago Prod"     NUMERIC,
                "Comissao Produto"    NUMERIC,
                "Frete Recebido"      NUMERIC,
                "Frete Pago"          NUMERIC,
                "Comissao Pedido"     NUMERIC,
                "Diferenca Frete"     NUMERIC,
                "Taxas"               NUMERIC,
                "Embalagem"           NUMERIC,
                "Repasse Financeiro"  NUMERIC,
                "Margem Contribuicao" NUMERIC,
                "Valor Liquido"       NUMERIC,
                "Total Custo Pedido"  NUMERIC,
                "Total Venda Pedido"  NUMERIC,
                "Ano"                 INTEGER,
                "Mes"                 INTEGER,
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

    cols        = list(df.columns)
    cols_quoted = ", ".join(f'"{c}"' for c in cols)

    # ── COPY → tabela temporária → upsert em um único statement ──────────────
    raw = engine.raw_connection()
    try:
        with raw.cursor() as cur:
            # Tabela temporária sem PK — COPY é mais rápido sem índice
            cur.execute("CREATE TEMP TABLE _tmp_vendas AS SELECT * FROM bd_vendas LIMIT 0")

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
                INSERT INTO bd_vendas ({cols_quoted})
                SELECT {cols_quoted} FROM _tmp_vendas
                ON CONFLICT ("Order ID", "Produto ID") DO UPDATE SET
                    {update_set}
            """)

        raw.commit()
    finally:
        raw.close()

    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS sync_log (
                id        SERIAL PRIMARY KEY,
                tabela    VARCHAR(100),
                registros INTEGER,
                data_sync TIMESTAMP DEFAULT NOW(),
                status    VARCHAR(50),
                origem    VARCHAR(100)
            )
        """))
        conn.execute(text("""
            INSERT INTO sync_log (tabela, registros, status, origem)
            VALUES ('bd_vendas', :n, 'OK', 'gefinance-json-v4-paralelo')
        """), {"n": len(df)})
        conn.commit()

    print(f"  ✔  Upsert concluído  |  sync registrado")


# ─────────────────────────────────────────────────────────────────
# CREDENCIAIS
# ─────────────────────────────────────────────────────────────────
def obter_credenciais() -> tuple[str, str]:
    email    = os.getenv("GEFINANCE_EMAIL")
    password = os.getenv("GEFINANCE_PASSWORD")
    if email and password:
        return email, password

    try:
        engine = conectar()
        with engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT chave, valor FROM configuracoes "
                "WHERE empresa = 'marcon' "
                "AND chave IN ('gefinance_email', 'gefinance_password')"
            ))
            cfg = {r[0]: r[1] for r in rows}
        email    = cfg.get("gefinance_email")
        password = cfg.get("gefinance_password")
        if email and password:
            print("  ✔  Credenciais carregadas da tabela configuracoes.")
            return email, password
    except Exception as e:
        print(f"[AVISO] {e}")

    raise ValueError("Credenciais não encontradas.")


# ─────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────
def main():
    inicio   = datetime.now()
    end_date = datetime.today().strftime("%Y-%m-%d")

    _sep("═")
    print("  S&OP Intelligence  ·  Gefinance ETL v4 (Paralelo)")
    print(f"  Início: {inicio.strftime('%d/%m/%Y  %H:%M:%S')}")
    _sep("═")

    _step(1, 5, "Carregando credenciais")
    with Spinner("Buscando credenciais no banco..."):
        email, password = obter_credenciais()

    _step(2, 5, "Autenticando na Gefinance")
    with Spinner("Fazendo login..."):
        auth = login(email, password)

    _step(3, 5, "Conectando ao banco e verificando último sync")
    with Spinner("Conectando ao PostgreSQL..."):
        engine     = conectar()
        first_date = obter_data_ultimo_sync(engine)

    _step(4, 5, "Baixando pedidos")
    pedidos = baixar_todos(auth, first_date, end_date)

    _step(5, 5, "Processando e salvando")
    with Spinner("Transformando dados..."):
        df = processar(pedidos)

    upsert_banco(engine, df)

    duracao = (datetime.now() - inicio).seconds
    _sep("═")
    print(f"  ✔  ETL finalizado em {duracao}s")
    print(f"  {len(df):,} registros  ·  {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    _sep("═")


if __name__ == "__main__":
    main()
