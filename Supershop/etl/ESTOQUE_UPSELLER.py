"""
ETL de Estoque — Upseller → PostgreSQL (Supershop)
Versão SSH/servidor — sem browser, usa token capturado pelo Windows.

Fluxo:
  1. Windows: python Scripts/get_upseller_token.py  → gera upseller_token.json
  2. Copie upseller_token.json para o servidor SSH
  3. SSH:     python ESTOQUE_UPSELLER.py

Dependências:
  pip install requests psycopg2-binary sqlalchemy pandas openpyxl
"""

import os, json, requests
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, text

# ─────────────────────────────────────────────────────────────────
# CONFIGURAÇÃO
# ─────────────────────────────────────────────────────────────────
PASTA         = Path(__file__).parent
TOKEN_FILE    = PASTA / "upseller_token.json"
API_INVENTORY = "https://app.upseller.com/api/inventory/list"
PAGE_SIZE     = 200

DB_CONFIG = {
    "host"    : os.getenv("SUPERSHOP_HOST",     "37.60.236.200"),
    "port"    : os.getenv("SUPERSHOP_PORT",     "5432"),
    "database": os.getenv("SUPERSHOP_DB",       "Supershop"),
    "user"    : os.getenv("SUPERSHOP_USER",     "postgres"),
    "password": os.getenv("SUPERSHOP_PASSWORD", "131105Gv"),
}

# ─────────────────────────────────────────────────────────────────
# AUTENTICAÇÃO
# ─────────────────────────────────────────────────────────────────
def _build_session() -> requests.Session:
    if not TOKEN_FILE.exists():
        raise FileNotFoundError(
            f"[!] {TOKEN_FILE} não encontrado.\n"
            "    Execute primeiro no Windows:\n"
            "      python Scripts/get_upseller_token.py"
        )
    token_data = json.loads(TOKEN_FILE.read_text(encoding="utf-8"))
    s = requests.Session()
    s.headers.update({
        "Accept"    : "application/json, text/plain, */*",
        "Origin"    : "https://app.upseller.com",
        "Referer"   : "https://app.upseller.com/pt/inventory/list",
        "User-Agent": "Mozilla/5.0 Chrome/124.0.0.0 Safari/537.36",
    })
    # Token JWT no header
    auth = token_data.get("auth") or {}
    jwt  = auth.get("token") or auth.get("accessToken") or auth.get("access_token")
    if jwt:
        s.headers["Authorization"] = f"Bearer {jwt}"
    # Cookies de sessão
    cookies = token_data.get("cookies") or {}
    for name, value in cookies.items():
        s.cookies.set(name, value, domain="app.upseller.com")
    return s

# ─────────────────────────────────────────────────────────────────
# BUSCA PAGINADA
# ─────────────────────────────────────────────────────────────────
def _fetch_all(s: requests.Session) -> list:
    todos    = []
    page_num = 1
    while True:
        r = s.get(API_INVENTORY, params={"page": page_num, "pageSize": PAGE_SIZE}, timeout=30)
        if r.status_code == 401:
            raise PermissionError(
                "[!] Token expirado (401). Rode novamente no Windows:\n"
                "      python Scripts/get_upseller_token.py"
            )
        r.raise_for_status()
        data  = r.json()
        items = (
            data.get("data") or data.get("result") or
            data.get("items") or data.get("list") or
            (data if isinstance(data, list) else [])
        )
        if not items:
            break
        todos.extend(items)
        print(f"  Página {page_num}: {len(items)} itens  (acumulado: {len(todos)})")
        total_pages = data.get("totalPages") or data.get("pages") or data.get("pageCount") or 1
        if page_num >= int(total_pages) or len(items) < PAGE_SIZE:
            break
        page_num += 1
    return todos

# ─────────────────────────────────────────────────────────────────
# NORMALIZAÇÃO
# ─────────────────────────────────────────────────────────────────
def _parse(items: list) -> pd.DataFrame:
    linhas = []
    for item in items:
        sku   = item.get("sku") or item.get("SKU") or item.get("code") or ""
        nome  = item.get("name") or item.get("productName") or item.get("title") or ""
        arm   = item.get("warehouse") or item.get("warehouseName") or "Principal"
        disp  = item.get("available") or item.get("availableQuantity") or 0
        total = item.get("total") or item.get("totalQuantity") or item.get("stockTotal") or disp
        if sku:
            linhas.append({"sku": sku, "nome": nome, "armazem": arm,
                           "disponivel": disp, "estoque_total": total})
    df = pd.DataFrame(linhas)
    for col in ["disponivel", "estoque_total"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    return df

# ─────────────────────────────────────────────────────────────────
# BANCO
# ─────────────────────────────────────────────────────────────────
def salvar_banco(df: pd.DataFrame):
    engine = create_engine(
        f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS estoque_upseller (
                sku           TEXT,
                nome          TEXT,
                armazem       TEXT,
                disponivel    NUMERIC,
                estoque_total NUMERIC,
                atualizado_em TIMESTAMP DEFAULT NOW(),
                PRIMARY KEY (sku, armazem)
            )
        """))
        for _, row in df.iterrows():
            conn.execute(text("""
                INSERT INTO estoque_upseller (sku, nome, armazem, disponivel, estoque_total, atualizado_em)
                VALUES (:sku, :nome, :armazem, :disponivel, :estoque_total, NOW())
                ON CONFLICT (sku, armazem) DO UPDATE SET
                    nome          = EXCLUDED.nome,
                    disponivel    = EXCLUDED.disponivel,
                    estoque_total = EXCLUDED.estoque_total,
                    atualizado_em = NOW()
            """), row.to_dict())
    print(f"[OK] {len(df)} registros salvos em estoque_upseller.")

# ─────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────
def main():
    print("=" * 55)
    print("  Upseller ETL  ·  Estoque → Supershop")
    print("=" * 55)

    print("[1] Carregando sessão...")
    s = _build_session()

    print("[2] Buscando estoque...")
    items = _fetch_all(s)

    if not items:
        print("[!] Nenhum item retornado. Verifique o token ou o endpoint.")
        return

    df = _parse(items)
    print(f"\n[OK] {len(df)} SKUs extraídos.")

    excel_path = PASTA / "estoque_upseller.xlsx"
    df.to_excel(excel_path, index=False)
    print(f"[OK] Excel: {excel_path}")

    salvar_banco(df)
    print("[OK] Concluído.")

if __name__ == "__main__":
    main()
