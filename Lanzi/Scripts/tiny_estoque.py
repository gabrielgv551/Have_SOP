#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════╗
║  Tiny Estoque — Python (fora do n8n)                        ║
║  Puxa todos os produtos + estoque por depósito da Tiny v2    ║
║  e salva no PostgreSQL (tabela tiny_estoque)                 ║
╚══════════════════════════════════════════════════════════════╝

Dependências:
  pip install requests psycopg2-binary python-dotenv

Uso:
  python tiny_estoque.py                    → incremental (padrão)
  python tiny_estoque.py --full-reload      → recria tabela e recarrega tudo
  python tiny_estoque.py --empresa lanzi    → override empresa
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
ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))
from db_config import get_engine

# ─── Config ───────────────────────────────────────────────────
TOKEN = os.getenv("TINY_TOKEN", "adad5861e5d6cc4e25f4e0d6e2d17eafd87e7c90a2a535d3690a885761fd644e")
BASE_URL = "https://api.tiny.com.br/api2"
CONCORRENCIA = int(os.getenv("TINY_CONCORRENCIA", "2"))
DELAY_MS = int(os.getenv("TINY_DELAY_MS", "300"))
RETRY_LIMIT = 3
RATE_LIMIT_SLEEP = 60

# ─── Helpers ──────────────────────────────────────────────────

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


def tiny_post(endpoint, params, retries=RETRY_LIMIT):
    """Faz POST para endpoint da Tiny API v2 com retry em rate limit."""
    all_params = {"token": TOKEN, "formato": "JSON", **params}
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


def buscar_todos_produtos():
    """Coleta todos os produtos paginados da Tiny."""
    produtos = []
    pagina = 1
    tem_mais = True

    while tem_mais:
        try:
            resp = tiny_post("produtos.pesquisa.php", {"pagina": pagina})
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


def buscar_estoque_produto(produto):
    """Busca estoque de um produto (com fallback). Retorna dict padronizado."""
    pid = produto["id"]
    _dbg_estoque_status = "(sem status)"
    _dbg_estoque_erro = ""
    _dbg_fonte = ""

    # Delay entre requisições (respeita rate limit)
    if DELAY_MS > 0:
        time.sleep(DELAY_MS / 1000.0)

    try:
        # ── Tentativa 1: produto.obter.estoque.php ──
        r1 = tiny_post("produto.obter.estoque.php", {"id": pid})
        ret1 = r1.get("retorno", r1) if isinstance(r1, dict) else {}
        _dbg_estoque_status = ret1.get("status", "(sem status)") if isinstance(ret1, dict) else "(sem status)"
        _dbg_estoque_erro = json.dumps(ret1.get("erros", ret1.get("codigo_erro", ""))) if isinstance(ret1, dict) else ""

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
                "_dbg_estoque_status": _dbg_estoque_status,
                "_dbg_estoque_erro": _dbg_estoque_erro,
                "_dbg_fonte": "estoque",
            }

        # ── Tentativa 2: produto.obter.php ──
        r2 = tiny_post("produto.obter.php", {"id": pid})
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
                "_dbg_estoque_status": _dbg_estoque_status,
                "_dbg_estoque_erro": _dbg_estoque_erro,
                "_dbg_fonte": f"obter (saldo={p.get('saldo', 'MISSING')} | deps={len(raw_deps)})",
            }

        return {
            "produto": produto,
            "depositos": [],
            "saldo_total": 0.0,
            "saldo_reservado": 0.0,
            "_dbg_estoque_status": _dbg_estoque_status,
            "_dbg_estoque_erro": _dbg_estoque_erro,
            "_dbg_fonte": "ambos_falharam",
        }

    except Exception as err:
        return {
            "produto": produto,
            "depositos": [],
            "saldo_total": 0.0,
            "saldo_reservado": 0.0,
            "_dbg_estoque_status": "EXCEPTION",
            "_dbg_estoque_erro": str(err),
            "_dbg_fonte": "exception",
        }


def buscar_estoques(produtos):
    """Busca estoque de todos os produtos com ThreadPoolExecutor."""
    estoques = []
    total = len(produtos)
    processados = 0

    with ThreadPoolExecutor(max_workers=CONCORRENCIA) as executor:
        future_to_prod = {executor.submit(buscar_estoque_produto, p): p for p in produtos}
        for future in as_completed(future_to_prod):
            result = future.result()
            estoques.append(result)
            processados += 1
            if processados % 50 == 0:
                print(f"[INFO] {processados}/{total} produtos processados...")

    print(f"[INFO] Total de estoques buscados: {len(estoques)}")
    return estoques


# ─── Banco de dados ───────────────────────────────────────────

DDL = """
CREATE TABLE IF NOT EXISTS tiny_estoque (
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
    atualizado_em   TIMESTAMPTZ DEFAULT NOW(),
    _dbg_estoque_status TEXT,
    _dbg_estoque_erro   TEXT,
    _dbg_fonte          TEXT
);

CREATE INDEX IF NOT EXISTS idx_tiny_estoque_sku ON tiny_estoque(sku);
CREATE INDEX IF NOT EXISTS idx_tiny_estoque_deposito ON tiny_estoque(deposito_nome);
"""


def salvar_no_banco(engine, estoques, empresa):
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
            "_dbg_estoque_status": est.get("_dbg_estoque_status", ""),
            "_dbg_estoque_erro": est.get("_dbg_estoque_erro", ""),
            "_dbg_fonte": est.get("_dbg_fonte", ""),
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

        # Limpa dados antigos da execução atual (opcional)
        conn.execute(text("DELETE FROM tiny_estoque WHERE atualizado_em < :agora"), {"agora": agora})

        insert_sql = text("""
            INSERT INTO tiny_estoque (
                produto_id, sku, nome, preco, unidade, situacao, tipo,
                saldo_total, saldo_reservado, deposito_id, deposito_nome,
                deposito_empresa, desconsiderar, quantidade, atualizado_em,
                _dbg_estoque_status, _dbg_estoque_erro, _dbg_fonte
            ) VALUES (
                :produto_id, :sku, :nome, :preco, :unidade, :situacao, :tipo,
                :saldo_total, :saldo_reservado, :deposito_id, :deposito_nome,
                :deposito_empresa, :desconsiderar, :quantidade, :atualizado_em,
                :_dbg_estoque_status, :_dbg_estoque_erro, :_dbg_fonte
            )
        """)
        conn.execute(insert_sql, linhas)

    print(f"[OK] Dados salvos na tabela tiny_estoque ({empresa}).")


# ─── Main ─────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Sync Tiny estoque para PostgreSQL")
    parser.add_argument("--empresa", default="lanzi", help="Empresa no db_config (default: lanzi)")
    parser.add_argument("--full-reload", action="store_true", help="Recria tabela do zero")
    parser.add_argument("--dry-run", action="store_true", help="Só imprime, não salva no banco")
    args = parser.parse_args()

    empresa = args.empresa
    print(f"[INFO] Empresa: {empresa}")
    print(f"[INFO] Concorrência: {CONCORRENCIA}, Delay: {DELAY_MS}ms")

    engine = get_engine(empresa)

    # 1. Busca produtos
    produtos = buscar_todos_produtos()
    if not produtos:
        print("[AVISO] Nenhum produto encontrado.")
        return

    # 2. Busca estoques
    estoques = buscar_estoques(produtos)

    # 3. Salva ou imprime
    if args.dry_run:
        print(json.dumps(estoques[:5], indent=2, ensure_ascii=False))
        print(f"[DRY RUN] {len(estoques)} produtos processados. Nenhum dado salvo.")
    else:
        if args.full_reload:
            print("[INFO] Full reload — recriando tabela tiny_estoque...")
            with engine.begin() as conn:
                from sqlalchemy import text
                conn.execute(text("DROP TABLE IF EXISTS tiny_estoque CASCADE"))
                conn.execute(text(DDL))
        salvar_no_banco(engine, estoques, empresa)


if __name__ == "__main__":
    main()
