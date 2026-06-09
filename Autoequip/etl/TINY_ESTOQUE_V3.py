#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════╗
║  Tiny Estoque — API v3 (OAuth2)                              ║
║  Extrai estoque do Tiny ERP via API v3 e salva no PostgreSQL ║
╚══════════════════════════════════════════════════════════════╝

Dependências:
  pip install requests psycopg2-binary python-dotenv

Uso:
  python TINY_ESTOQUE_V3.py --empresa autoequip --account tiny_autoequip
"""

import os
import sys
import json
import time
import argparse
from datetime import datetime, timedelta
from pathlib import Path

import requests

# ─── Paths ────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))
from db_config import get_engine

# ─── Config ───────────────────────────────────────────────
TINY_API       = "https://erp.tiny.com.br/public-api/v3"
TINY_TOKEN_URL = "https://accounts.tiny.com.br/realms/tiny/protocol/openid-connect/token"
BATCH_SIZE     = 20   # detalhes por lote
PAGE_SIZE      = 100  # produtos por página
DELAY_MS       = 500  # delay entre chamadas de detalhe


def get_config(engine, empresa, account):
    """Lê configurações do Tiny do banco."""
    cfg = {}
    with engine.connect() as conn:
        cur = conn.connection.cursor()
        cur.execute(
            "SELECT chave, valor FROM configuracoes WHERE empresa=%s AND chave IN (%s, %s)",
            (empresa, 'tiny_client_id', 'tiny_client_secret')
        )
        for row in cur.fetchall():
            cfg[row[0]] = row[1]
        cur.execute(
            "SELECT chave, valor FROM configuracoes WHERE empresa=%s AND chave LIKE %s",
            (empresa, account + '_%')
        )
        for row in cur.fetchall():
            cfg[row[0]] = row[1]
    return cfg


def refresh_token(cfg, account_name):
    """Renova access_token via refresh_token."""
    refresh = cfg.get(f"{account_name}_refresh")
    client_id = cfg.get('tiny_client_id', os.getenv('TINY_CLIENT_ID', '')).strip()
    client_secret = cfg.get('tiny_client_secret', os.getenv('TINY_CLIENT_SECRET', '')).strip()

    if not refresh:
        raise RuntimeError(f"Refresh token não encontrado para {account_name}")
    if not client_id:
        raise RuntimeError("Client ID não configurado (tiny_client_id)")

    payload = {
        'grant_type': 'refresh_token',
        'client_id': client_id,
        'client_secret': client_secret,
        'refresh_token': refresh,
    }
    r = requests.post(TINY_TOKEN_URL, data=payload, timeout=15)

    if not r.ok:
        raise RuntimeError(f"Falha ao renovar token: {r.status_code}")

    data = r.json()
    return data['access_token'], data.get('refresh_token', refresh), data.get('expires_in', 21600)


def save_token(engine, empresa, account, access_token, refresh_token, expires_in):
    """Salva tokens renovados no banco."""
    exp = (datetime.utcnow() + timedelta(seconds=expires_in)).isoformat()
    with engine.begin() as conn:
        cur = conn.connection.cursor()
        for k, v in [
            (f"{account}_token", access_token),
            (f"{account}_refresh", refresh_token),
            (f"{account}_exp", exp),
        ]:
            cur.execute(
                """INSERT INTO configuracoes (empresa, chave, valor) VALUES (%s, %s, %s)
                   ON CONFLICT (empresa, chave) DO UPDATE SET valor=EXCLUDED.valor""",
                (empresa, k, v)
            )


def tiny_get(token, endpoint, params=None):
    """GET para API v3 do Tiny."""
    url = f"{TINY_API}{endpoint}"
    headers = {'Authorization': f'Bearer {token}'}
    r = requests.get(url, params=params, headers=headers, timeout=15)
    r.raise_for_status()
    return r.json()


def fetch_all_produtos(token):
    """Busca todos os produtos paginados."""
    produtos = []
    pagina = 1
    while True:
        data = tiny_get(token, '/produtos', {'pagina': pagina, 'limite': PAGE_SIZE})
        items = data.get('itens', data.get('data', data.get('produtos', [])))
        if not items:
            break
        produtos.extend(items)
        print(f"[INFO] {len(produtos)} produtos...")
        if len(items) < PAGE_SIZE:
            break
        pagina += 1
        time.sleep(0.3)
    return produtos


def fetch_produto_detalhes(token, produtos):
    """Busca detalhes (incluindo estoque) de cada produto em lotes."""
    total = len(produtos)
    for i in range(0, total, BATCH_SIZE):
        batch = produtos[i:i + BATCH_SIZE]
        for p in batch:
            pid = p.get('id')
            if not pid:
                continue
            try:
                d = tiny_get(token, f'/produtos/{pid}')
                p['_det'] = d
            except Exception as e:
                print(f"[AVISO] Erro detalhe {pid}: {e}")
                p['_det'] = {}
            if DELAY_MS > 0:
                time.sleep(DELAY_MS / 1000.0)
        print(f"[INFO] {min(i + BATCH_SIZE, total)}/{total} detalhes buscados...")
    return produtos


def extract_estoque_rows(produtos):
    """Extrai linhas de estoque dos produtos."""
    rows = []
    for p in produtos:
        d = p.get('_det', p)
        estoque = d.get('estoque', d.get('saldo', {}))
        precos = p.get('precos', p)
        rows.append({
            'id_tiny': str(p.get('id', '')),
            'sku': p.get('sku', p.get('codigo', '')),
            'nome': p.get('descricao', p.get('nome', '')),
            'unidade': p.get('unidade', ''),
            'estoque_atual': float(estoque.get('quantidade', 0)) or 0,
            'estoque_minimo': float(estoque.get('minimo', 0)) or 0,
            'estoque_maximo': float(estoque.get('maximo', 0)) or 0,
            'preco_custo': float(precos.get('precoCusto', p.get('precoCusto', 0))) or 0,
            'preco_venda': float(precos.get('preco', p.get('preco', 0))) or 0,
            'marca': d.get('marca', {}).get('nome', d.get('marca', '')),
            'categoria': d.get('categoria', {}).get('nome', d.get('categoria', '')),
        })
    return rows


def salvar_estoque(engine, empresa, account, rows):
    """Cria tabela e salva estoque no PostgreSQL."""
    safe_name = ''.join(c if c.isalnum() or c == '_' else '_' for c in account).lower()
    table = f"bd_estoque_tiny_{safe_name}"

    ddl = f"""
    CREATE TABLE IF NOT EXISTS {table} (
        id_tiny TEXT PRIMARY KEY,
        sku TEXT,
        nome TEXT,
        unidade TEXT,
        estoque_atual NUMERIC DEFAULT 0,
        estoque_minimo NUMERIC DEFAULT 0,
        estoque_maximo NUMERIC DEFAULT 0,
        preco_custo NUMERIC DEFAULT 0,
        preco_venda NUMERIC DEFAULT 0,
        marca TEXT,
        categoria TEXT,
        atualizado_em TIMESTAMP DEFAULT NOW()
    );
    """

    with engine.begin() as conn:
        from sqlalchemy import text
        conn.execute(text(ddl))

        conn.execute(text(f"""
            INSERT INTO {table} (id_tiny, sku, nome, unidade, estoque_atual, estoque_minimo, estoque_maximo,
                preco_custo, preco_venda, marca, categoria, atualizado_em)
            VALUES (:id_tiny, :sku, :nome, :unidade, :estoque_atual, :estoque_minimo, :estoque_maximo,
                :preco_custo, :preco_venda, :marca, :categoria, NOW())
            ON CONFLICT (id_tiny) DO UPDATE SET
                sku=EXCLUDED.sku, nome=EXCLUDED.nome, unidade=EXCLUDED.unidade,
                estoque_atual=EXCLUDED.estoque_atual, estoque_minimo=EXCLUDED.estoque_minimo, estoque_maximo=EXCLUDED.estoque_maximo,
                preco_custo=EXCLUDED.preco_custo, preco_venda=EXCLUDED.preco_venda,
                marca=EXCLUDED.marca, categoria=EXCLUDED.categoria, atualizado_em=NOW()
        """), rows)

    print(f"[OK] {len(rows)} produtos salvos em {table}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--empresa', required=True, help='Empresa no db_config (ex: autoequip)')
    parser.add_argument('--account', required=True, help='Nome da conta Tiny (ex: tiny_autoequip)')
    parser.add_argument('--dry-run', action='store_true', help='Só lista, não salva')
    args = parser.parse_args()

    empresa = args.empresa
    account = args.account

    print(f"[INFO] Empresa: {empresa} | Conta: {account}")

    engine = get_engine(empresa)

    print("[INFO] Lendo credenciais do banco...")
    cfg = get_config(engine, empresa, account)
    access_token = cfg.get(f"{account}_token")

    if not access_token:
        print("[ERRO] Token não encontrado. Execute o OAuth primeiro no Have Gestor.")
        return

    exp = cfg.get(f"{account}_exp")
    needs_refresh = False
    if exp:
        try:
            exp_dt = datetime.fromisoformat(exp.replace('Z', '+00:00'))
            if datetime.now(exp_dt.tzinfo) >= exp_dt - timedelta(minutes=5):
                needs_refresh = True
        except:
            needs_refresh = True
    else:
        needs_refresh = True

    if needs_refresh:
        print("[INFO] Renovando token...")
        access_token, ref, expires_in = refresh_token(cfg, account)
        save_token(engine, empresa, account, access_token, ref, expires_in)

    print("[INFO] Buscando produtos...")
    produtos = fetch_all_produtos(access_token)
    print(f"[INFO] Total: {len(produtos)} produtos")

    if not produtos:
        print("[AVISO] Nenhum produto encontrado.")
        return

    print("[INFO] Buscando detalhes/estoque...")
    produtos = fetch_produto_detalhes(access_token, produtos)

    rows = extract_estoque_rows(produtos)

    if args.dry_run:
        print(json.dumps(rows[:3], indent=2, ensure_ascii=False))
        print(f"\n[DRY RUN] {len(rows)} produtos. Nenhum dado salvo.")
        return

    salvar_estoque(engine, empresa, account, rows)


if __name__ == '__main__':
    main()
