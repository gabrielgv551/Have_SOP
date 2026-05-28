"""
╔══════════════════════════════════════════════════════════════╗
║  etl_common/pluggy_extrato.py — Lógica compartilhada ETL    ║
║  Usado por: Lanzi, Marcon, Supershop                        ║
║  Cada empresa só precisa chamar:  run("empresa")            ║
╚══════════════════════════════════════════════════════════════╝

Dependências:
  pip install psycopg2-binary python-dotenv

Uso direto (para testes):
  python pluggy_extrato.py lanzi
"""

import os
import sys
import time
import threading
import psycopg2
import psycopg2.extras
from datetime import datetime, date, timedelta


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


def _step(n, total, desc):
    print(f"\n  [{n}/{total}] {desc}")


# ─────────────────────────────────────────────────────────────────
# FONTE: banco "extratos" (Extrator Bancários)
# ─────────────────────────────────────────────────────────────────

def _resolver_client_ids(conn_ext, cfg: dict) -> list:
    """
    Retorna TODOS os client_ids vinculados a esta empresa no banco extratos.
    Suporta múltiplos clientes apontando para o mesmo gestor.
    Fallback para EXTRATOR_CLIENT_ID se definido.
    """
    empresa            = cfg["empresa"]
    extrator_client_id = cfg["extrator_client_id"]

    if extrator_client_id:
        return [(extrator_client_id, empresa)]

    cur = conn_ext.cursor()
    cur.execute(
        "SELECT id, name FROM clients WHERE gestor_empresa = %s ORDER BY name",
        (empresa,),
    )
    rows = cur.fetchall()
    cur.close()

    if not rows:
        print(f"\n  ❌ Nenhum cliente vinculado a empresa='{empresa}' no banco extratos.")
        print("     Vincule no app: extrator-bancario.vercel.app\n")
        sys.exit(1)

    return [(str(r[0]), r[1]) for r in rows]


def _obter_items_do_cliente(conn_ext, client_id: str) -> list:
    cur = conn_ext.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(
        """
        SELECT pluggy_item_id AS id, institution_name AS institution
        FROM items
        WHERE client_id = %s
        ORDER BY created_at
        """,
        (client_id,),
    )
    rows = cur.fetchall()
    cur.close()
    return [dict(r) for r in rows]


def _buscar_transacoes_extrato(conn_ext, client_id: str, pluggy_item_id: str,
                                date_from: str, date_to: str) -> list:
    cur = conn_ext.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(
        """
        SELECT id, date, description, type, amount, institution_name,
               account_name, account_number,
               counterparty_name AS razao_social, counterparty_document
        FROM transactions
        WHERE client_id = %s
          AND pluggy_item_id = %s
          AND date::date >= %s::date
          AND date::date <= %s::date
        ORDER BY date
        """,
        (client_id, pluggy_item_id, date_from, date_to),
    )
    rows = cur.fetchall()
    cur.close()
    return [dict(r) for r in rows]


# ─────────────────────────────────────────────────────────────────
# DESTINO: banco da empresa (caixa_extrato)
# ─────────────────────────────────────────────────────────────────

def _obter_data_inicio_sync(conn_destino, item_id: str, cfg: dict) -> str:
    if cfg["full_reload"]:
        return cfg["first_date"]
    cur = conn_destino.cursor()
    cur.execute(
        "SELECT ultimo_sync FROM belvo_links WHERE empresa=%s AND link_id=%s",
        (cfg["empresa"], item_id),
    )
    row = cur.fetchone()
    cur.close()
    if row and row[0]:
        dt = row[0].date() - timedelta(days=1)
        return str(dt)
    return cfg["first_date"]


def _garantir_belvo_link(conn_destino, item_id: str, institution: str, cfg: dict):
    cur = conn_destino.cursor()
    cur.execute(
        """
        INSERT INTO belvo_links (empresa, link_id, institution, account_type)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (empresa, link_id) DO UPDATE
          SET institution=EXCLUDED.institution, ativo=true
        """,
        (cfg["empresa"], item_id, (institution or "Banco")[:100], ""),
    )
    conn_destino.commit()
    cur.close()


def _garantir_banco(conn_destino, nome: str, cfg: dict) -> int:
    cur = conn_destino.cursor()
    cur.execute(
        """
        INSERT INTO caixa_bancos (empresa, nome)
        VALUES (%s, %s)
        ON CONFLICT (empresa, nome) DO UPDATE SET nome=EXCLUDED.nome
        RETURNING id
        """,
        (cfg["empresa"], (nome or "Banco")[:100]),
    )
    banco_id = cur.fetchone()[0]
    conn_destino.commit()
    cur.close()
    return banco_id


def _upsert_transacoes(conn_destino, transacoes: list, banco_id: int, cfg: dict) -> int:
    if not transacoes:
        return 0
    rows = []
    for tx in transacoes:
        raw_date = tx.get("date")
        if not raw_date:
            continue
        d = datetime.fromisoformat(str(raw_date)[:10])
        tx_id = str(tx.get("id", "")).strip()
        if not tx_id:
            continue
        rows.append((
            cfg["empresa"],
            d.year, d.month, d.day,
            str(tx.get("description") or "")[:500],
            str(tx.get("razao_social") or "")[:300] or None,
            str(tx.get("account_number") or "")[:100] or None,
            str(tx.get("counterparty_document") or "")[:255] or None,
            round(float(tx.get("amount") or 0) * 100),
            tx_id,
            banco_id,
        ))
    if not rows:
        return 0
    cur = conn_destino.cursor()
    psycopg2.extras.execute_values(
        cur,
        """
        INSERT INTO caixa_extrato
          (empresa, ano, mes, dia, descricao, razao_social, account_number,
           counterparty_document, valor, belvo_tx_id, banco_id)
        VALUES %s
        ON CONFLICT (empresa, belvo_tx_id) DO UPDATE
          SET ano=EXCLUDED.ano, mes=EXCLUDED.mes, dia=EXCLUDED.dia,
              descricao=EXCLUDED.descricao, razao_social=EXCLUDED.razao_social,
              account_number=EXCLUDED.account_number,
              counterparty_document=EXCLUDED.counterparty_document,
              valor=EXCLUDED.valor, banco_id=EXCLUDED.banco_id,
              atualizado_em=CURRENT_TIMESTAMP
        """,
        rows,
        page_size=200,
    )
    conn_destino.commit()
    cur.close()
    return len(rows)


def _atualizar_ultimo_sync(conn_destino, item_id: str, cfg: dict):
    cur = conn_destino.cursor()
    cur.execute(
        "UPDATE belvo_links SET ultimo_sync=NOW() WHERE empresa=%s AND link_id=%s",
        (cfg["empresa"], item_id),
    )
    conn_destino.commit()
    cur.close()


# ─────────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────────

def run(empresa: str):
    """
    Executa o ETL de extrato para a empresa informada.
    Lê configuração das variáveis de ambiente.
    """
    _KEY = empresa.upper()
    cfg = {
        "empresa": empresa,
        "db_destino": {
            "host"    : os.getenv(f"{_KEY}_HOST",     "37.60.236.200"),
            "port"    : int(os.getenv(f"{_KEY}_PORT", "5432")),
            "database": os.getenv(f"{_KEY}_DB",       empresa.capitalize()),
            "user"    : os.getenv(f"{_KEY}_USER",     "postgres"),
            "password": os.getenv(f"{_KEY}_PASSWORD", ""),
        },
        "db_extratos": {
            "host"    : os.getenv("EXTRATOS_HOST",     "37.60.236.200"),
            "port"    : int(os.getenv("EXTRATOS_PORT", "5432")),
            "database": os.getenv("EXTRATOS_DB",       "extratos"),
            "user"    : os.getenv("EXTRATOS_USER",     "postgres"),
            "password": os.getenv("EXTRATOS_PASSWORD", ""),
        },
        "extrator_client_id": os.getenv("EXTRATOR_CLIENT_ID", ""),
        "first_date"        : os.getenv("EXTRATO_FIRST_DATE", "2026-01-01"),
        "full_reload"       : os.getenv("FULL_RELOAD", "").strip() == "1",
    }

    inicio = datetime.now()
    _sep("═")
    print(f"  EXTRATO ETL · Extrator Bancários → {empresa.capitalize()}")
    modo = "FULL RELOAD" if cfg["full_reload"] else "INCREMENTAL"
    ref  = cfg["first_date"] if cfg["full_reload"] else "último sync salvo"
    print(f"  Modo: {modo}  |  Data base: {ref}")
    _sep("═")

    _step(1, 4, "Conectando aos bancos")
    _ka = dict(keepalives=1, keepalives_idle=30, keepalives_interval=10, keepalives_count=5)
    with Spinner(f"Conectando a extratos e {empresa.capitalize()}..."):
        conn_ext     = psycopg2.connect(**cfg["db_extratos"], **_ka)
        conn_destino = psycopg2.connect(**cfg["db_destino"],  **_ka)

    _step(2, 4, "Buscando clientes vinculados")
    with Spinner("Resolvendo clientes no banco extratos..."):
        clients = _resolver_client_ids(conn_ext, cfg)

    print(f"  ✔  {len(clients)} cliente(s) vinculado(s) a '{empresa}':")
    for cid, cname in clients:
        print(f"     · {cname}  ({cid[:12]}...)")

    today           = str(date.today())
    total_importado = 0

    _step(3, 4, "Importando transações")
    for client_id, client_name in clients:
        with Spinner(f"Lendo items de '{client_name}'..."):
            items = _obter_items_do_cliente(conn_ext, client_id)

        if not items:
            print(f"\n     ○  '{client_name}' sem bancos conectados — pulando")
            continue

        print(f"\n  ▶  {client_name}  ({len(items)} banco(s))")

        for item in items:
            item_id     = item["id"]
            institution = item["institution"] or "Banco"

            print(f"     · {institution}")
            _garantir_belvo_link(conn_destino, item_id, institution, cfg)
            banco_id  = _garantir_banco(conn_destino, institution, cfg)
            date_from = _obter_data_inicio_sync(conn_destino, item_id, cfg)
            print(f"       Período: {date_from} → {today}")

            with Spinner("Lendo transações do banco extratos..."):
                txs = _buscar_transacoes_extrato(conn_ext, client_id, item_id, date_from, today)

            if txs:
                with Spinner(f"Salvando {len(txs):,} transações..."):
                    n = _upsert_transacoes(conn_destino, txs, banco_id, cfg)
                total_importado += n
                print(f"       ✔  {n:,} transações importadas")
            else:
                print(f"       ○  Nenhuma transação no período")

            _atualizar_ultimo_sync(conn_destino, item_id, cfg)

    _step(4, 4, "Concluído")
    duracao = (datetime.now() - inicio).seconds
    _sep("═")
    print(f"  ✅ {total_importado:,} transações importadas  |  {duracao}s")
    _sep("═")
    conn_ext.close()
    conn_destino.close()


if __name__ == "__main__":
    empresa_arg = sys.argv[1] if len(sys.argv) > 1 else None
    if not empresa_arg:
        print("Uso: python pluggy_extrato.py <empresa>  (ex: lanzi, marcon, supershop)")
        sys.exit(1)
    run(empresa_arg)
