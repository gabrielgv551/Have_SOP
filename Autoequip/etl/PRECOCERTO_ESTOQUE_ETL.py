#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════╗
║  Preço Certo — Estoque ETL  ·  Autoequip                     ║
║  Extrai estoque do endpoint /api/product-warehouse           ║
║  e salva no PostgreSQL (tabela precocerto_estoque)           ║
╚══════════════════════════════════════════════════════════════╝

Dependências:
  pip install requests psycopg2-binary sqlalchemy python-dotenv

Uso:
  python PRECOCERTO_ESTOQUE_ETL.py              # extrai e salva
  python PRECOCERTO_ESTOQUE_ETL.py --dry-run    # só inspeciona, não salva
  python PRECOCERTO_ESTOQUE_ETL.py --explore    # só mostra estrutura da API
"""

import os
import sys
import json
import argparse
from datetime import datetime
from pathlib import Path

import requests
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

# ─── Config ───────────────────────────────────────────────────
PC_URL   = "https://sys.precocerto.co"
PC_EMAIL = os.getenv("PRECOCERTO_EMAIL",    "gabriel.viana@have.com")
PC_SENHA = os.getenv("PRECOCERTO_PASSWORD", "123456789Gv!")

DB_CONFIG = {
    "host"    : os.getenv("AUTOEQUIP_HOST",     "37.60.236.200"),
    "port"    : int(os.getenv("AUTOEQUIP_PORT", "5432")),
    "database": os.getenv("AUTOEQUIP_DB",       "Autoequip"),
    "user"    : os.getenv("AUTOEQUIP_USER",     "postgres"),
    "password": os.getenv("AUTOEQUIP_PASSWORD", ""),
}

# ─── Auth ─────────────────────────────────────────────────────

def autenticar():
    """Autentica no Preço Certo via JWT + Django session."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json",
    })

    # 1. JWT
    r = session.post(
        f"{PC_URL}/api/token/",
        json={"username": PC_EMAIL, "password": PC_SENHA},
        timeout=10,
    )
    r.raise_for_status()
    token = r.json()["access"]
    session.headers["Authorization"] = f"Bearer {token}"

    # 2. Django session (necessária para alguns endpoints)
    r_login = session.post(
        f"{PC_URL}/login/",
        data={"username_login": PC_EMAIL, "password_login": PC_SENHA},
        headers={"Referer": f"{PC_URL}/login/", "Content-Type": "application/x-www-form-urlencoded"},
        allow_redirects=True,
        timeout=15,
    )
    return session


# ─── Explorar ─────────────────────────────────────────────────

def explorar_estrutura(session):
    """Mostra estrutura do endpoint product-warehouse."""
    print("=== GET /api/product-warehouse ===")
    r = session.get(f"{PC_URL}/api/product-warehouse", timeout=15)
    print(f"Status: {r.status_code}")
    print(f"Content-Type: {r.headers.get('Content-Type', '')}")

    if r.status_code != 200:
        print(f"Erro: {r.text[:300]}")
        return None

    data = r.json()
    print(json.dumps(data, indent=2, ensure_ascii=False)[:2000])
    return data


# ─── Extrair ──────────────────────────────────────────────────

def extrair_estoque(session):
    """Extrai todos os registros de estoque do product-warehouse."""
    registros = []
    url = f"{PC_URL}/api/product-warehouse"
    params = {"limit": 1000, "offset": 0}

    while True:
        r = session.get(url, params=params, timeout=15)
        r.raise_for_status()
        data = r.json()

        if isinstance(data, dict):
            results = data.get("results", data.get("rows", data.get("data", [])))
            count = data.get("count", data.get("total", len(results)))
        elif isinstance(data, list):
            results = data
            count = len(data)
        else:
            print(f"[AVISO] Formato inesperado: {type(data)}")
            break

        if not results:
            break

        registros.extend(results)
        print(f"[INFO] {len(registros)}/{count} registros...")

        if len(registros) >= count:
            break

        params["offset"] = len(registros)

    print(f"[INFO] Total extraído: {len(registros)}")
    return registros


# ─── Banco ────────────────────────────────────────────────────

def salvar_estoque(engine, registros):
    """Cria tabela e salva registros no PostgreSQL."""
    agora = datetime.utcnow().isoformat()

    ddl = """
    CREATE TABLE IF NOT EXISTS precocerto_estoque (
        id              SERIAL PRIMARY KEY,
        sku             TEXT,
        nome            TEXT,
        quantidade      NUMERIC(12,2),
        deposito        TEXT,
        deposito_id     TEXT,
        estoque_minimo  NUMERIC(12,2),
        estoque_maximo  NUMERIC(12,2),
        custo           NUMERIC(12,2),
        preco           NUMERIC(12,2),
        ativo           BOOLEAN,
        raw_json        JSONB,
        atualizado_em   TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_pc_estoque_sku ON precocerto_estoque(sku);
    CREATE INDEX IF NOT EXISTS idx_pc_estoque_deposito ON precocerto_estoque(deposito);
    """

    with engine.begin() as conn:
        conn.execute(text(ddl))
        conn.execute(text("DELETE FROM precocerto_estoque"))

    linhas = []
    for r in registros:
        if not isinstance(r, dict):
            continue
        linhas.append({
            "sku": str(r.get("sku", r.get("code", r.get("codigo", r.get("product_code", ""))))).strip(),
            "nome": r.get("name", r.get("product_name", r.get("nome", r.get("description", "")))),
            "quantidade": float(r.get("quantity", r.get("quantidade", r.get("stock", r.get("current_stock", 0))))) or 0,
            "deposito": r.get("warehouse", r.get("deposito", r.get("warehouse_name", "Nosso Depósito"))),
            "deposito_id": str(r.get("warehouse_id", r.get("deposito_id", ""))),
            "estoque_minimo": float(r.get("min_stock", r.get("estoque_minimo", r.get("minimum_stock", 0)))) or 0,
            "estoque_maximo": float(r.get("max_stock", r.get("estoque_maximo", r.get("maximum_stock", 0)))) or 0,
            "custo": float(r.get("cost", r.get("custo", r.get("unit_cost", 0)))) or 0,
            "preco": float(r.get("price", r.get("preco", r.get("sale_price", 0)))) or 0,
            "ativo": bool(r.get("active", r.get("ativo", r.get("is_active", True)))),
            "raw_json": json.dumps(r, ensure_ascii=False),
            "atualizado_em": agora,
        })

    insert_sql = text("""
        INSERT INTO precocerto_estoque
        (sku, nome, quantidade, deposito, deposito_id, estoque_minimo, estoque_maximo, custo, preco, ativo, raw_json, atualizado_em)
        VALUES (:sku, :nome, :quantidade, :deposito, :deposito_id, :estoque_minimo, :estoque_maximo, :custo, :preco, :ativo, :raw_json, :atualizado_em)
    """)

    with engine.begin() as conn:
        conn.execute(insert_sql, linhas)

    print(f"[OK] {len(linhas)} registros salvos em precocerto_estoque.")


# ─── Main ─────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--explore", action="store_true", help="Só inspeciona a estrutura da API")
    parser.add_argument("--dry-run", action="store_true", help="Extrai mas não salva no banco")
    args = parser.parse_args()

    print("[INFO] Autenticando no Preço Certo (Autoequip)...")
    session = autenticar()
    print("[OK] Autenticado.")

    if args.explore:
        explorar_estrutura(session)
        return

    registros = extrair_estoque(session)
    if not registros:
        print("[AVISO] Nenhum registro de estoque encontrado.")
        return

    if args.dry_run:
        print("\n=== Primeiros 3 registros ===")
        for r in registros[:3]:
            print(json.dumps(r, indent=2, ensure_ascii=False))
        print(f"\n[DRY RUN] {len(registros)} registros. Nenhum dado salvo.")
        return

    engine = create_engine(
        f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )
    salvar_estoque(engine, registros)


if __name__ == "__main__":
    main()
