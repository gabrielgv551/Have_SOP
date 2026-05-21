"""
╔══════════════════════════════════════════════════════════════╗
║      Extrato ETL · Lanzi                                     ║
║  Puxa transações do banco "extratos" (Extrator Bancários)    ║
║  e sincroniza para a tabela caixa_extrato do Lanzi           ║
╚══════════════════════════════════════════════════════════════╝

O link entre Lanzi e o Extrator Bancários é feito via:
  LANZI_EXTRATOR_CLIENT_ID=98d138b9-d8dc-4ec2-ba90-1e025bde158b

Dependências:
  pip install psycopg2-binary python-dotenv

Uso:
  python EXTRATO_PLUGGY.py              → incremental desde último sync
  FULL_RELOAD=1 python EXTRATO_PLUGGY.py → recarrega desde FIRST_DATE
"""

import os
import sys
import time
import threading
import psycopg2
import psycopg2.extras
from datetime import datetime, date, timedelta
from pathlib import Path

# ─── Carrega .env do workspace root (pasta pai da Lanzi/) ────────
_env_path = Path(__file__).resolve().parent.parent / ".env"
if _env_path.exists():
    try:
        from dotenv import load_dotenv
        load_dotenv(str(_env_path))
    except ImportError:
        pass


# ─────────────────────────────────────────────────────────────────
# CONFIGURAÇÃO
# ─────────────────────────────────────────────────────────────────

# Empresa no Have Gestor — única linha que muda entre empresas
EMPRESA     = "lanzi"

# Banco destino — credenciais derivadas automaticamente de EMPRESA
_KEY = EMPRESA.upper()
DB_DESTINO = {
    "host"    : os.getenv(f"{_KEY}_HOST",     "37.60.236.200"),
    "port"    : int(os.getenv(f"{_KEY}_PORT", "5432")),
    "database": os.getenv(f"{_KEY}_DB",       EMPRESA.capitalize()),
    "user"    : os.getenv(f"{_KEY}_USER",     "postgres"),
    "password": os.getenv(f"{_KEY}_PASSWORD", "131105Gv"),
}

# Banco Extratos (fonte — Extrator Bancários)
DB_EXTRATOS = {
    "host"    : os.getenv("EXTRATOS_HOST",     "37.60.236.200"),
    "port"    : int(os.getenv("EXTRATOS_PORT", "5432")),
    "database": os.getenv("EXTRATOS_DB",       "extratos"),
    "user"    : os.getenv("EXTRATOS_USER",     "postgres"),
    "password": os.getenv("EXTRATOS_PASSWORD", "131105Gv"),
}

EXTRATOR_CLIENT_ID = os.getenv("EXTRATOR_CLIENT_ID", "")  # override manual do client_id se necessário

FIRST_DATE  = os.getenv("EXTRATO_FIRST_DATE", "2026-01-01")
FULL_RELOAD = os.getenv("FULL_RELOAD", "").strip() == "1"


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
def resolver_client_ids(conn_ext) -> list:
    """
    Retorna TODOS os client_ids vinculados a esta empresa no banco extratos.
    Suporta múltiplos clientes apontando para o mesmo gestor (ex: Lanzi + MS → lanzi).
    Fallback para LANZI_EXTRATOR_CLIENT_ID se definido.
    """
    if EXTRATOR_CLIENT_ID:
        return [(EXTRATOR_CLIENT_ID, EMPRESA)]
    cur = conn_ext.cursor()
    cur.execute(
        "SELECT id, name FROM clients WHERE gestor_empresa = %s ORDER BY name",
        (EMPRESA,),
    )
    rows = cur.fetchall()
    cur.close()
    if not rows:
        print(f"\n  ❌ Nenhum cliente vinculado a empresa='{EMPRESA}' no banco extratos.")
        print("     Vincule no app: extrator-bancario.vercel.app\n")
        sys.exit(1)
    return [(str(r[0]), r[1]) for r in rows]


def obter_items_do_cliente(conn_ext, client_id: str) -> list:
    """
    Retorna os items (conexões bancárias) de um cliente no banco extratos.
    """
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


def buscar_transacoes_extrato(conn_ext, client_id: str, pluggy_item_id: str, date_from: str, date_to: str) -> list:
    """
    Lê transações bancárias (somente conta corrente, sem cartão de crédito)
    da tabela transactions do banco extratos.
    """
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
# DESTINO: banco Lanzi (caixa_extrato)
# ─────────────────────────────────────────────────────────────────
def obter_data_inicio_sync(conn_lanzi, item_id: str) -> str:
    """Retorna a data de início do sync para um item."""
    if FULL_RELOAD:
        return FIRST_DATE
    cur = conn_lanzi.cursor()
    cur.execute(
        "SELECT ultimo_sync FROM belvo_links WHERE empresa=%s AND link_id=%s",
        (EMPRESA, item_id),
    )
    row = cur.fetchone()
    cur.close()
    if row and row[0]:
        dt = row[0].date() - timedelta(days=1)
        return str(dt)
    return FIRST_DATE


def garantir_belvo_link(conn_lanzi, item_id: str, institution: str):
    cur = conn_lanzi.cursor()
    cur.execute(
        """
        INSERT INTO belvo_links (empresa, link_id, institution, account_type)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (empresa, link_id) DO UPDATE
          SET institution=EXCLUDED.institution, ativo=true
        """,
        (EMPRESA, item_id, (institution or "Banco")[:100], ""),
    )
    conn_lanzi.commit()
    cur.close()


def garantir_banco(conn_lanzi, nome: str) -> int:
    cur = conn_lanzi.cursor()
    cur.execute(
        """
        INSERT INTO caixa_bancos (empresa, nome)
        VALUES (%s, %s)
        ON CONFLICT (empresa, nome) DO UPDATE SET nome=EXCLUDED.nome
        RETURNING id
        """,
        (EMPRESA, (nome or "Banco")[:100]),
    )
    banco_id = cur.fetchone()[0]
    conn_lanzi.commit()
    cur.close()
    return banco_id


def upsert_transacoes(conn_lanzi, transacoes: list, banco_id: int) -> int:
    """Faz upsert em batch das transações em caixa_extrato."""
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
            EMPRESA,
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
    cur = conn_lanzi.cursor()
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
    conn_lanzi.commit()
    cur.close()
    return len(rows)


def atualizar_ultimo_sync(conn_lanzi, item_id: str):
    cur = conn_lanzi.cursor()
    cur.execute(
        "UPDATE belvo_links SET ultimo_sync=NOW() WHERE empresa=%s AND link_id=%s",
        (EMPRESA, item_id),
    )
    conn_lanzi.commit()
    cur.close()


# ─────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────
def main():
    inicio = datetime.now()
    _sep("═")
    print(f"  EXTRATO ETL · Extrator Bancários → {EMPRESA.capitalize()}")
    modo = "FULL RELOAD" if FULL_RELOAD else "INCREMENTAL"
    ref  = FIRST_DATE if FULL_RELOAD else "último sync salvo"
    print(f"  Modo: {modo}  |  Data base: {ref}")
    _sep("═")

    _step(1, 4, "Conectando aos bancos")
    _ka = dict(keepalives=1, keepalives_idle=30, keepalives_interval=10, keepalives_count=5)
    with Spinner(f"Conectando a extratos e {EMPRESA.capitalize()}..."):
        conn_ext   = psycopg2.connect(**DB_EXTRATOS, **_ka)
        conn_lanzi = psycopg2.connect(**DB_DESTINO,  **_ka)

    _step(2, 4, "Buscando clientes vinculados")
    with Spinner("Resolvendo clientes no banco extratos..."):
        clients = resolver_client_ids(conn_ext)

    print(f"  ✔  {len(clients)} cliente(s) vinculado(s) a '{EMPRESA}':")
    for cid, cname in clients:
        print(f"     · {cname}  ({cid[:12]}...)")

    today          = str(date.today())
    total_importado = 0

    _step(3, 4, "Importando transações")
    for client_id, client_name in clients:
        with Spinner(f"Lendo items de '{client_name}'..."):
            items = obter_items_do_cliente(conn_ext, client_id)

        if not items:
            print(f"\n     ○  '{client_name}' sem bancos conectados — pulando")
            continue

        print(f"\n  ▶  {client_name}  ({len(items)} banco(s))")

        for item in items:
            item_id     = item["id"]
            institution = item["institution"] or "Banco"

            print(f"     · {institution}")
            garantir_belvo_link(conn_lanzi, item_id, institution)
            banco_id  = garantir_banco(conn_lanzi, institution)
            date_from = obter_data_inicio_sync(conn_lanzi, item_id)
            print(f"       Período: {date_from} → {today}")

            with Spinner("Lendo transações do banco extratos..."):
                txs = buscar_transacoes_extrato(conn_ext, client_id, item_id, date_from, today)

            if txs:
                with Spinner(f"Salvando {len(txs):,} transações..."):
                    n = upsert_transacoes(conn_lanzi, txs, banco_id)
                total_importado += n
                print(f"       ✔  {n:,} transações importadas")
            else:
                print(f"       ○  Nenhuma transação no período")

            atualizar_ultimo_sync(conn_lanzi, item_id)

    _step(4, 4, "Concluído")
    duracao = (datetime.now() - inicio).seconds
    _sep("═")
    print(f"  ✅ {total_importado:,} transações importadas  |  {duracao}s")
    _sep("═")
    conn_ext.close()
    conn_lanzi.close()


if __name__ == "__main__":
    main()
