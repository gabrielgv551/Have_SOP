"""
metabase_api.py — Wrapper da API do Metabase
=============================================
Funções para auto-provisioning:
  1. Login (session token)
  2. Criar/obter conexão de banco por empresa
  3. Sync do schema
  4. Clonar dashboard template para a empresa

Requer variáveis de ambiente:
  METABASE_URL         = http://localhost:3000
  METABASE_USER        = admin@have.com
  METABASE_PASSWORD    = senha_admin
  METABASE_TEMPLATE_ML = 42  (ID do dashboard template "Mercado Livre")
"""

import os
import time
import requests
from dotenv import load_dotenv

load_dotenv()

METABASE_URL  = os.getenv("METABASE_URL", "http://localhost:3000")
MB_USER       = os.getenv("METABASE_USER", "")
MB_PASSWORD   = os.getenv("METABASE_PASSWORD", "")
TEMPLATE_ML   = os.getenv("METABASE_TEMPLATE_ML", "")

_session_token = None


# ─────────────────────────────────────────────────────────────
# SESSÃO
# ─────────────────────────────────────────────────────────────
def get_session() -> str:
    """Faz login no Metabase e retorna o session token."""
    global _session_token
    if _session_token:
        # Testar se ainda é válido
        r = requests.get(
            f"{METABASE_URL}/api/user/current",
            headers={"X-Metabase-Session": _session_token},
            timeout=10,
        )
        if r.status_code == 200:
            return _session_token

    resp = requests.post(
        f"{METABASE_URL}/api/session",
        json={"username": MB_USER, "password": MB_PASSWORD},
        timeout=15,
    )
    resp.raise_for_status()
    _session_token = resp.json()["id"]
    print(f"  [Metabase] Sessão criada")
    return _session_token


def _headers():
    return {"X-Metabase-Session": get_session()}


# ─────────────────────────────────────────────────────────────
# LISTAR BANCOS EXISTENTES
# ─────────────────────────────────────────────────────────────
def listar_databases() -> list:
    """Retorna lista de databases cadastradas no Metabase."""
    resp = requests.get(
        f"{METABASE_URL}/api/database",
        headers=_headers(),
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json().get("data", resp.json()) if isinstance(resp.json(), dict) else resp.json()


def encontrar_database_por_nome(nome: str) -> dict | None:
    """Busca database no Metabase pelo nome (display name)."""
    for db in listar_databases():
        if db.get("name", "").lower() == nome.lower():
            return db
    return None


# ─────────────────────────────────────────────────────────────
# CRIAR CONEXÃO DO BANCO DA EMPRESA
# ─────────────────────────────────────────────────────────────
def get_or_create_database(company: str, db_config: dict) -> int:
    """
    Cria conexão PostgreSQL no Metabase para a empresa, se não existir.
    db_config deve ter: host, port, dbname, user, password
    Retorna o database_id do Metabase.
    """
    display_name = f"Have - {company.capitalize()}"

    # Verificar se já existe
    existing = encontrar_database_por_nome(display_name)
    if existing:
        print(f"  [Metabase] Database '{display_name}' já existe (id={existing['id']})")
        return existing["id"]

    # Criar nova
    payload = {
        "name": display_name,
        "engine": "postgres",
        "details": {
            "host": db_config["host"],
            "port": int(db_config["port"]),
            "dbname": db_config["dbname"],
            "user": db_config["user"],
            "password": db_config["password"],
            "ssl": False,
            "tunnel-enabled": False,
        },
        "auto_run_queries": True,
        "is_full_sync": True,
        "is_on_demand": False,
        "schedules": {
            "metadata_sync": {"schedule_type": "daily", "schedule_hour": 1},
            "cache_field_values": {"schedule_type": "daily", "schedule_hour": 2},
        },
    }

    resp = requests.post(
        f"{METABASE_URL}/api/database",
        headers=_headers(),
        json=payload,
        timeout=30,
    )
    resp.raise_for_status()
    db_id = resp.json()["id"]
    print(f"  [Metabase] Database '{display_name}' criada (id={db_id})")
    return db_id


# ─────────────────────────────────────────────────────────────
# SYNC DO SCHEMA
# ─────────────────────────────────────────────────────────────
def sync_database(db_id: int):
    """Força sync do schema no Metabase para descobrir tabelas novas."""
    resp = requests.post(
        f"{METABASE_URL}/api/database/{db_id}/sync_schema",
        headers=_headers(),
        timeout=15,
    )
    if resp.status_code in (200, 204):
        print(f"  [Metabase] Sync iniciado para database {db_id}")
    else:
        print(f"  [Metabase] Sync warning: {resp.status_code} {resp.text[:200]}")

    # Aguardar um pouco para o sync processar
    time.sleep(5)


# ─────────────────────────────────────────────────────────────
# OBTER TABELAS DE UM BANCO
# ─────────────────────────────────────────────────────────────
def get_tables(db_id: int) -> list:
    """Retorna lista de tabelas do banco no Metabase."""
    resp = requests.get(
        f"{METABASE_URL}/api/database/{db_id}/metadata",
        headers=_headers(),
        timeout=30,
    )
    resp.raise_for_status()
    tables = resp.json().get("tables", [])
    return tables


def find_table_id(db_id: int, table_name: str) -> int | None:
    """Encontra o ID interno do Metabase para uma tabela específica."""
    for t in get_tables(db_id):
        if t.get("name", "").lower() == table_name.lower():
            return t["id"]
    return None


# ─────────────────────────────────────────────────────────────
# CLONAR DASHBOARD TEMPLATE
# ─────────────────────────────────────────────────────────────
def get_dashboard(dashboard_id: int) -> dict:
    """Obtém detalhes completos de um dashboard."""
    resp = requests.get(
        f"{METABASE_URL}/api/dashboard/{dashboard_id}",
        headers=_headers(),
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()


def listar_collections() -> list:
    resp = requests.get(
        f"{METABASE_URL}/api/collection",
        headers=_headers(),
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()


def get_or_create_collection(name: str, parent_id: int | None = None) -> int:
    """Cria ou encontra uma collection no Metabase."""
    for c in listar_collections():
        if c.get("name", "").lower() == name.lower():
            return c["id"]

    payload = {"name": name, "color": "#007CDC"}
    if parent_id:
        payload["parent_id"] = parent_id

    resp = requests.post(
        f"{METABASE_URL}/api/collection",
        headers=_headers(),
        json=payload,
        timeout=15,
    )
    resp.raise_for_status()
    coll_id = resp.json()["id"]
    print(f"  [Metabase] Collection '{name}' criada (id={coll_id})")
    return coll_id


def clone_dashboard_for_company(template_id: int, target_db_id: int,
                                 company: str) -> dict:
    """
    Clona um dashboard template e remapeia as questions para o banco da empresa.

    Fluxo:
    1. Cria collection da empresa (se não existe)
    2. Copia o dashboard para a collection
    3. Para cada card, atualiza o database_id da question
    """
    company_title = company.capitalize()

    # 1. Collection da empresa
    have_coll = get_or_create_collection("Have Clientes")
    company_coll = get_or_create_collection(company_title, parent_id=have_coll)

    # 2. Copiar dashboard
    resp = requests.post(
        f"{METABASE_URL}/api/dashboard/{template_id}/copy",
        headers=_headers(),
        json={
            "name": f"Mercado Livre - {company_title}",
            "collection_id": company_coll,
            "is_deep_copy": True,
        },
        timeout=30,
    )
    resp.raise_for_status()
    new_dash = resp.json()
    new_dash_id = new_dash["id"]
    print(f"  [Metabase] Dashboard clonado (id={new_dash_id}) para {company_title}")

    # 3. Remapear database_id nas questions copiadas
    dashboard_full = get_dashboard(new_dash_id)
    for card in dashboard_full.get("dashcards", []):
        card_data = card.get("card", {})
        card_id = card_data.get("id")
        if not card_id:
            continue

        dataset_query = card_data.get("dataset_query", {})
        if dataset_query.get("database"):
            # Atualizar a question para apontar pro banco certo
            dataset_query["database"] = target_db_id

            # Atualizar source-table se possível
            if dataset_query.get("type") == "query":
                query = dataset_query.get("query", {})
                source_table = query.get("source-table")
                if isinstance(source_table, int):
                    # Tentar encontrar tabela equivalente no banco novo
                    new_table_id = find_table_id(target_db_id, "bd_vendas_ml")
                    if new_table_id:
                        query["source-table"] = new_table_id

            try:
                requests.put(
                    f"{METABASE_URL}/api/card/{card_id}",
                    headers=_headers(),
                    json={"dataset_query": dataset_query},
                    timeout=15,
                )
            except Exception as e:
                print(f"  [Metabase] Aviso: não conseguiu remapear card {card_id}: {e}")

    print(f"  [Metabase] ✔ Dashboard '{dashboard_full.get('name')}' pronto!")
    return {
        "dashboard_id": new_dash_id,
        "dashboard_url": f"{METABASE_URL}/dashboard/{new_dash_id}",
        "collection_id": company_coll,
    }


# ─────────────────────────────────────────────────────────────
# FUNÇÃO PRINCIPAL — PROVISIONAR TUDO
# ─────────────────────────────────────────────────────────────
def provisionar_metabase(company: str, db_config: dict) -> dict:
    """
    Provisiona tudo no Metabase para uma empresa:
    1. Cria conexão do banco
    2. Sync do schema
    3. Clona dashboard template

    Retorna dict com dashboard_id e url.
    """
    template_id = TEMPLATE_ML
    if not template_id:
        print("  [Metabase] METABASE_TEMPLATE_ML não configurado — pulando provisioning")
        return {"skipped": True, "reason": "METABASE_TEMPLATE_ML not set"}

    template_id = int(template_id)

    # 1. Criar/obter conexão
    db_id = get_or_create_database(company, db_config)

    # 2. Sync schema
    sync_database(db_id)

    # 3. Verificar se já existe dashboard pra essa empresa
    have_coll = get_or_create_collection("Have Clientes")
    company_title = company.capitalize()
    company_coll_id = None
    for c in listar_collections():
        if (c.get("name", "").lower() == company_title.lower()
                and c.get("location", "").startswith(f"/{have_coll}/")):
            company_coll_id = c["id"]
            break

    if company_coll_id:
        # Verificar se já tem dashboard ML nessa collection
        resp = requests.get(
            f"{METABASE_URL}/api/collection/{company_coll_id}/items",
            headers=_headers(),
            params={"models": "dashboard"},
            timeout=15,
        )
        if resp.status_code == 200:
            items = resp.json().get("data", resp.json())
            if isinstance(items, list):
                for item in items:
                    if "mercado livre" in item.get("name", "").lower():
                        print(f"  [Metabase] Dashboard ML já existe para {company_title} (id={item['id']})")
                        return {
                            "dashboard_id": item["id"],
                            "dashboard_url": f"{METABASE_URL}/dashboard/{item['id']}",
                            "already_existed": True,
                        }

    # 4. Clonar template
    result = clone_dashboard_for_company(template_id, db_id, company)
    return result
