#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════╗
║  Tiny Estoque v2 — Marcon                                    ║
║  Usa API v2 (mais rápida, endpoint produto.obter.estoque.php) ║
║  Lê token do banco e salva em tiny_estoque_marcon            ║
╚══════════════════════════════════════════════════════════════╝

Uso:
  python tiny_estoque_v2.py --dry-run     → só lista
  python tiny_estoque_v2.py               → salva no banco
"""

import os
import sys
import json
import time
import argparse
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

# ─── Paths ────────────────────────────────────────────────────
# ─── DB connection (fallback se db_config não existir no servidor) ─
try:
    ROOT = Path(__file__).resolve().parent.parent.parent
    sys.path.insert(0, str(ROOT))
    from db_config import get_engine
except ImportError:
    # Servidor SSH — conecta direto no Marcon
    from sqlalchemy import create_engine
    def get_engine(empresa):
        import os
        pw = os.getenv("MARCON_PASSWORD", "131105Gv")
        url = f"postgresql+psycopg2://postgres:{pw}@37.60.236.200:5432/Marcon"
        return create_engine(url)

# ─── Config ───────────────────────────────────────────────────
BASE_URL = "https://api.tiny.com.br/api2"
CONCORRENCIA = int(os.getenv("TINY_CONCORRENCIA", "1"))
DELAY_MS = int(os.getenv("TINY_DELAY_MS", "1000"))
RETRY_LIMIT = 3
RATE_LIMIT_SLEEP = 60


def get_tiny_token(engine, empresa, cli_token=None):
    """Lê token da Tiny do banco de configuracoes ou argumento."""
    if cli_token:
        return cli_token
    with engine.connect() as conn:
        cur = conn.connection.cursor()
        for chave in ['tiny_token', 'tiny_marcon_token', 'tiny_api_token']:
            cur.execute("SELECT valor FROM configuracoes WHERE empresa=%s AND chave=%s", (empresa, chave))
            row = cur.fetchone()
            if row and row[0]:
                return row[0]
    return os.getenv("TINY_TOKEN", "")


def parse_num(v):
    if v is None or v == "":
        return 0.0
    try:
        return float(str(v).replace(",", ".")) or 0.0
    except (ValueError, TypeError):
        return 0.0


def is_rate_limit_error(text):
    if not text:
        return False
    s = str(text).lower()
    return any(x in s for x in ["api bloqueada", "excedido", "aguarde"])


def tiny_post(token, endpoint, params, retries=RETRY_LIMIT):
    """Faz POST para endpoint da Tiny API v2 com retry em rate limit."""
    all_params = {"token": token, "formato": "JSON", **params}
    body = "&".join(
        f"{k}={requests.utils.quote(str(v))}" for k, v in all_params.items()
    )
    url = f"{BASE_URL}/{endpoint}"
    headers = {"Content-Type": "application/x-www-form-urlencoded"}

    for tentativa in range(retries + 1):
        try:
            resp = requests.post(url, data=body, headers=headers, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            resp_str = json.dumps(data)

            if is_rate_limit_error(resp_str) and tentativa < retries:
                print(f"[RATE LIMIT] {endpoint} — aguardando {RATE_LIMIT_SLEEP}s...")
                time.sleep(RATE_LIMIT_SLEEP)
                continue
            return data
        except requests.exceptions.RequestException as e:
            if is_rate_limit_error(str(e)) and tentativa < retries:
                print(f"[RATE LIMIT] {endpoint} — aguardando {RATE_LIMIT_SLEEP}s...")
                time.sleep(RATE_LIMIT_SLEEP)
                continue
            raise
    return {}


def buscar_todos_produtos(token):
    """Coleta todos os produtos paginados da Tiny v2."""
    produtos = []
    pagina = 1
    tem_mais = True

    while tem_mais:
        try:
            resp = tiny_post(token, "produtos.pesquisa.php", {"pagina": pagina})
        except Exception as e:
            print(f"[ERRO] produtos.pesquisa.php pagina={pagina}: {e}")
            break

        retorno = resp.get("retorno", resp)
        status = retorno.get("status") if isinstance(retorno, dict) else None
        lista = retorno.get("produtos", []) if isinstance(retorno, dict) else []

        if status != "OK" or not lista:
            tem_mais = False
        else:
            for item in lista:
                p = item.get("produto", item)
                sku = str(p.get("codigo", "")).strip()
                pid = p.get("id")
                if pid and sku and sku != "0":
                    produtos.append({
                        "id": str(pid),
                        "sku": sku,
                        "nome": p.get("nome", ""),
                        "preco": parse_num(p.get("preco")),
                        "unidade": p.get("unidade", ""),
                        "situacao": p.get("situacao", ""),
                        "tipo": p.get("tipo", ""),
                    })
            tem_mais = len(lista) >= 100
            pagina += 1

    print(f"[INFO] {len(produtos)} produtos com SKU válido encontrados.")
    return produtos


def buscar_estoque_produto(token, produto):
    """Busca estoque de um produto via produto.obter.estoque.php."""
    pid = produto["id"]

    # Delay entre requisições
    if DELAY_MS > 0:
        time.sleep(DELAY_MS / 1000.0)

    try:
        r1 = tiny_post(token, "produto.obter.estoque.php", {"id": pid})
        ret1 = r1.get("retorno", r1) if isinstance(r1, dict) else {}

        if isinstance(ret1, dict) and ret1.get("status") == "OK":
            p = ret1.get("produto", {})
            raw_deps = p.get("depositos", [])
            depositos = []
            for entry in raw_deps:
                d = entry.get("deposito", entry)
                depositos.append({
                    "id": d.get("id", ""),
                    "nome": d.get("nome", ""),
                    "desconsiderar": d.get("desconsiderar", "N"),
                    "empresa": d.get("empresa", ""),
                    "saldo": parse_num(d.get("saldo")),
                })
            return {
                "produto": produto,
                "saldo_total": parse_num(p.get("saldo")),
                "saldo_reservado": parse_num(p.get("saldoReservado")),
                "depositos": depositos,
            }

        # Fallback: produto.obter.php
        r2 = tiny_post(token, "produto.obter.php", {"id": pid})
        ret2 = r2.get("retorno", r2) if isinstance(r2, dict) else {}

        if isinstance(ret2, dict) and ret2.get("status") == "OK":
            p = ret2.get("produto", {})
            raw_deps = p.get("depositos", [])
            depositos = []
            for entry in raw_deps:
                d = entry.get("deposito", entry)
                depositos.append({
                    "id": d.get("id", ""),
                    "nome": d.get("nome", ""),
                    "desconsiderar": d.get("desconsiderar", "N"),
                    "empresa": d.get("empresa", ""),
                    "saldo": parse_num(d.get("saldo")),
                })
            produto_rico = {
                **produto,
                "nome": p.get("nome", produto["nome"]),
                "preco": parse_num(p.get("preco")) or produto["preco"],
                "unidade": p.get("unidade", produto["unidade"]),
                "situacao": p.get("situacao", produto["situacao"]),
                "tipo": p.get("tipo", produto["tipo"]),
            }
            return {
                "produto": produto_rico,
                "saldo_total": parse_num(p.get("saldo")),
                "saldo_reservado": parse_num(p.get("saldoReservado")),
                "depositos": depositos,
            }

        return {"produto": produto, "depositos": [], "saldo_total": 0.0, "saldo_reservado": 0.0}

    except Exception as err:
        return {"produto": produto, "depositos": [], "saldo_total": 0.0, "saldo_reservado": 0.0}


def buscar_estoques(token, produtos):
    """Busca estoque de todos os produtos com ThreadPoolExecutor."""
    estoques = []
    total = len(produtos)
    processados = 0

    with ThreadPoolExecutor(max_workers=CONCORRENCIA) as executor:
        future_to_prod = {executor.submit(buscar_estoque_produto, token, p): p for p in produtos}
        for future in as_completed(future_to_prod):
            result = future.result()
            estoques.append(result)
            processados += 1
            if processados % 50 == 0:
                print(f"[INFO] {processados}/{total} produtos processados...")

    print(f"[INFO] Total de estoques buscados: {len(estoques)}")
    return estoques


DDL = """
CREATE TABLE IF NOT EXISTS tiny_estoque_marcon (
    id              SERIAL PRIMARY KEY,
    produto_id      TEXT NOT NULL,
    sku             TEXT NOT NULL,
    nome            TEXT,
    preco           NUMERIC(12,2),
    unidade         TEXT,
    situacao        TEXT,
    tipo            TEXT,
    saldo_total     NUMERIC(12,2),
    saldo_reservado NUMERIC(12,2),
    deposito_id     TEXT,
    deposito_nome   TEXT,
    deposito_empresa TEXT,
    desconsiderar   TEXT,
    quantidade      NUMERIC(12,2),
    atualizado_em   TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_tiny_estoque_marcon_sku ON tiny_estoque_marcon(sku);
CREATE INDEX IF NOT EXISTS idx_tiny_estoque_marcon_deposito ON tiny_estoque_marcon(deposito_nome);
"""


def salvar_no_banco(engine, estoques):
    """Salva estoques no PostgreSQL."""
    from sqlalchemy import text
    agora = datetime.utcnow().isoformat()
    linhas = []

    for est in estoques:
        prod = est["produto"]
        if not prod.get("sku") or prod["sku"] == "0":
            continue

        base = {
            "produto_id": prod["id"],
            "sku": prod["sku"],
            "nome": prod["nome"],
            "preco": prod["preco"],
            "unidade": prod["unidade"],
            "situacao": prod["situacao"],
            "tipo": prod["tipo"],
            "saldo_total": est["saldo_total"],
            "saldo_reservado": est["saldo_reservado"],
            "atualizado_em": agora,
        }

        depositos = est.get("depositos", [])
        if not depositos:
            linhas.append({
                **base,
                "deposito_id": None,
                "deposito_nome": "Nosso Depósito",
                "deposito_empresa": None,
                "desconsiderar": "N",
                "quantidade": max(0.0, est["saldo_total"]),
            })
        else:
            for dep in depositos:
                if not dep.get("nome"):
                    continue
                linhas.append({
                    **base,
                    "deposito_id": dep["id"],
                    "deposito_nome": dep["nome"],
                    "deposito_empresa": dep["empresa"],
                    "desconsiderar": dep["desconsiderar"],
                    "quantidade": max(0.0, dep["saldo"]),
                })

    print(f"[INFO] {len(linhas)} linhas para inserir no banco.")

    with engine.begin() as conn:
        conn.execute(text(DDL))
        conn.execute(text("TRUNCATE TABLE tiny_estoque_marcon"))

        insert_sql = text("""
            INSERT INTO tiny_estoque_marcon (
                produto_id, sku, nome, preco, unidade, situacao, tipo,
                saldo_total, saldo_reservado, deposito_id, deposito_nome,
                deposito_empresa, desconsiderar, quantidade, atualizado_em
            ) VALUES (
                :produto_id, :sku, :nome, :preco, :unidade, :situacao, :tipo,
                :saldo_total, :saldo_reservado, :deposito_id, :deposito_nome,
                :deposito_empresa, :desconsiderar, :quantidade, :atualizado_em
            )
        """)
        # Insert em batches de 500 para evitar travamento
        batch_size = 500
        total_inserido = 0
        for i in range(0, len(linhas), batch_size):
            batch = linhas[i:i + batch_size]
            conn.execute(insert_sql, batch)
            total_inserido += len(batch)
            print(f"[INFO] {total_inserido}/{len(linhas)} linhas inseridas...")

    print(f"[OK] Dados salvos na tabela tiny_estoque_marcon ({total_inserido} linhas).")

    # ─── Sincroniza com tabela v3 que o Have Gestor usa ─────────
    print("[INFO] Sincronizando bd_estoque_tiny_tiny_marcon...")
    conn.execute(text("TRUNCATE TABLE bd_estoque_tiny_tiny_marcon"))
    conn.execute(text("""
        INSERT INTO bd_estoque_tiny_tiny_marcon 
        (id_tiny, sku, nome, unidade, estoque_atual, estoque_minimo, preco_custo, preco_venda, marca, categoria, atualizado_em)
        SELECT 
            produto_id,
            sku,
            MAX(nome),
            MAX(unidade),
            SUM(quantidade),
            0,
            MAX(preco),
            MAX(preco),
            '',
            '',
            NOW()
        FROM tiny_estoque_marcon
        WHERE sku IS NOT NULL AND sku != ''
        GROUP BY produto_id, sku
    """))
    print("[OK] bd_estoque_tiny_tiny_marcon sincronizado.")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--empresa", default="marcon", help="Empresa no db_config")
    parser.add_argument("--token", help="Token v2 da Tiny (se não estiver no banco)")
    parser.add_argument("--full-reload", action="store_true", help="Recria tabela")
    parser.add_argument("--dry-run", action="store_true", help="Só imprime, não salva")
    args = parser.parse_args()

    empresa = args.empresa
    print(f"[INFO] Empresa: {empresa} | Concorrência: {CONCORRENCIA}, Delay: {DELAY_MS}ms")

    engine = get_engine(empresa)

    # Lê token do banco ou argumento
    token = get_tiny_token(engine, empresa, args.token)
    if not token:
        print("[ERRO] Token Tiny não encontrado no banco. Configure em configuracoes (chave: tiny_token ou tiny_marcon_token)")
        return
    print(f"[INFO] Token carregado (len={len(token)})")

    # 1. Busca produtos
    produtos = buscar_todos_produtos(token)
    if not produtos:
        print("[AVISO] Nenhum produto encontrado.")
        return

    # 2. Busca estoques
    estoques = buscar_estoques(token, produtos)

    # 3. Salva ou imprime
    if args.dry_run:
        print(json.dumps(estoques[:3], indent=2, ensure_ascii=False))
        print(f"[DRY RUN] {len(estoques)} produtos. Nenhum dado salvo.")
        return

    if args.full_reload:
        print("[INFO] Full reload — recriando tabela...")
        with engine.begin() as conn:
            from sqlalchemy import text
            conn.execute(text("DROP TABLE IF EXISTS tiny_estoque_marcon CASCADE"))
            conn.execute(text(DDL))

    salvar_no_banco(engine, estoques)


if __name__ == "__main__":
    main()
