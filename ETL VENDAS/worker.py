"""
worker.py — ETL Worker API
===========================
Micro-serviço Flask que recebe webhooks da Vercel API após OAuth ML
e executa: ETL de vendas + provisioning do Metabase.

Roda no servidor 37.60.236.200 (mesmo do PostgreSQL e Metabase).

ENDPOINTS:
  POST /etl/trigger     — dispara ETL + Metabase para uma empresa
  GET  /etl/status      — verifica se o worker está rodando
  GET  /etl/jobs        — lista jobs recentes

USO:
  python worker.py                          # dev (debug mode)
  gunicorn worker:app -b 0.0.0.0:5050      # produção
"""

import os
import sys
import uuid
import threading
import traceback
import time
import requests
from datetime import datetime, timedelta
from flask import Flask, request, jsonify
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

# Garante que o diretório ETL VENDAS está no path para imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

app = Flask(__name__)

ETL_SECRET = os.getenv("ETL_SECRET", "")

# ─────────────────────────────────────────────────────────────
# REGISTRO DE JOBS
# ─────────────────────────────────────────────────────────────
_jobs = {}  # job_id → {status, company, account_id, started_at, finished_at, result, error}
_jobs_lock = threading.Lock()
_etl_lock  = threading.Lock()  # garante que apenas 1 ETL roda por vez (monkey-patch não é thread-safe)

# Mapeamento de companies → env var prefix (mesmo padrão do data.js)
COMPANIES = {
    "lanzi":  "LANZI",
    "marcon": "MARCON",
}


def get_company_engine(company: str):
    """Cria engine SQLAlchemy para o banco da empresa."""
    key = COMPANIES.get(company)
    if not key:
        raise ValueError(f"Empresa '{company}' não configurada. Disponíveis: {list(COMPANIES.keys())}")

    host     = os.getenv(f"{key}_HOST", "37.60.236.200")
    port     = os.getenv(f"{key}_PORT", "5432")
    dbname   = os.getenv(f"{key}_DB", company.capitalize())
    user     = os.getenv(f"{key}_USER", "postgres")
    password = os.getenv(f"{key}_PASSWORD", "")

    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
    return create_engine(url, pool_pre_ping=True, pool_size=5)


def get_company_db_config(company: str) -> dict:
    """Retorna dict com config do banco para o Metabase."""
    key = COMPANIES.get(company, company.upper())
    return {
        "host":     os.getenv(f"{key}_HOST", "37.60.236.200"),
        "port":     os.getenv(f"{key}_PORT", "5432"),
        "dbname":   os.getenv(f"{key}_DB", company.capitalize()),
        "user":     os.getenv(f"{key}_USER", "postgres"),
        "password": os.getenv(f"{key}_PASSWORD", ""),
    }


# ─────────────────────────────────────────────────────────────
# LER TOKENS DO BANCO (tabela configuracoes)
# ─────────────────────────────────────────────────────────────
def carregar_tokens_do_banco(engine, company: str, account_id: str) -> dict | None:
    """
    Lê access_token, refresh_token, user_id da tabela configuracoes.
    Formato no banco: {account_id}_token, {account_id}_token_refresh, etc.
    """
    chave = account_id + "_token"
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT chave, valor FROM configuracoes WHERE empresa=:emp AND chave LIKE :patt"),
            {"emp": company, "patt": account_id + "%"},
        ).fetchall()
    if not rows:
        return None
    cfg = {r[0]: r[1] for r in rows}
    access  = cfg.get(chave)
    refresh = cfg.get(chave + "_refresh")
    user_id = cfg.get(chave + "_user_id")
    exp_at  = cfg.get(chave + "_exp")
    nick    = cfg.get(chave + "_nick")
    if not access:
        return None
    return {
        "access_token":  access,
        "refresh_token": refresh,
        "user_id":       user_id,
        "expires_at":    exp_at,
        "nickname":      nick,
    }


def renovar_token_se_necessario(engine, company: str, account_id: str, tokens: dict) -> str:
    """Renova token se expirado. Retorna access_token válido."""
    import datetime as dt
    import requests as req

    exp_at = tokens.get("expires_at")
    expired = True
    if exp_at:
        try:
            exp_dt = dt.datetime.fromisoformat(exp_at.replace("Z", "+00:00"))
            if exp_dt.tzinfo is None:
                exp_dt = exp_dt.replace(tzinfo=dt.timezone.utc)
            expired = (exp_dt - dt.timedelta(minutes=5)) < dt.datetime.now(dt.timezone.utc)
        except Exception:
            expired = True

    if not expired:
        return tokens["access_token"]

    # Renovar
    print(f"  [Worker] Token expirado para {account_id} — renovando...")
    ML_CLIENT_ID     = os.getenv("ML_CLIENT_ID", "")
    ML_CLIENT_SECRET = os.getenv("ML_CLIENT_SECRET", "")

    resp = req.post("https://api.mercadolibre.com/oauth/token", json={
        "grant_type":    "refresh_token",
        "client_id":     ML_CLIENT_ID,
        "client_secret": ML_CLIENT_SECRET,
        "refresh_token": tokens["refresh_token"],
    }, timeout=15)
    resp.raise_for_status()
    new_data = resp.json()

    # Salvar no banco
    chave = account_id + "_token"
    new_exp = (dt.datetime.now(dt.timezone.utc)
               + dt.timedelta(seconds=new_data.get("expires_in", 21600))).isoformat()

    with engine.connect() as conn:
        for k, v in [
            (chave,              new_data["access_token"]),
            (chave + "_refresh", new_data["refresh_token"]),
            (chave + "_exp",     new_exp),
        ]:
            conn.execute(text(
                "INSERT INTO configuracoes (empresa,chave,valor,atualizado_em) "
                "VALUES (:emp,:k,:v,NOW()) "
                "ON CONFLICT (empresa,chave) DO UPDATE SET valor=EXCLUDED.valor, atualizado_em=NOW()"
            ), {"emp": company, "k": k, "v": v})
        conn.commit()

    print(f"  [Worker] Token renovado com sucesso")
    return new_data["access_token"]


# ─────────────────────────────────────────────────────────────
# TABELA DINÂMICA POR CONTA
# ─────────────────────────────────────────────────────────────
def _sanitize_table_name(account_id: str) -> str:
    """Converte account_id em nome de tabela seguro. ex: ml_pp → bd_vendas_ml_ml_pp"""
    import re
    safe = re.sub(r'[^a-z0-9_]', '_', account_id.lower())
    return f"bd_vendas_ml_{safe}"


def recriar_view_consolidada(engine):
    """
    Recria bd_vendas_consolidado como UNION ALL de todas as tabelas bd_vendas_ml_*
    existentes no banco, mais qualquer outra fonte (Shopee, Amazon, etc.) se existir.
    """
    with engine.connect() as conn:
        # Descobrir todas as tabelas bd_vendas_ml_*
        tabelas = conn.execute(text("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_name LIKE 'bd_vendas_ml_%'
            ORDER BY table_name
        """)).scalars().all()

        if not tabelas:
            print("  [View] Nenhuma tabela bd_vendas_ml_* encontrada — view não recriada")
            return

        # Montar SELECT padronizado para cada tabela ML
        def ml_select(t):
            return f"""
            SELECT
                'Mercado Livre-' || numero_ecommerce   AS id_pedido_canal,
                '{t}'                                  AS origem,
                ano                                    AS "Ano",
                mes                                    AS "Mes",
                data                                   AS "Data",
                CASE status
                    WHEN 'paid'      THEN 'Aprovado'
                    WHEN 'cancelled' THEN 'Cancelado'
                    ELSE status
                END                                    AS "Status",
                numero_ecommerce                       AS "Order ID",
                total_venda_pedido                     AS "Total Venda Pedido",
                total_item                             AS "Total Venda",
                quantidade                             AS "Quantidade Vendida",
                comissao_item                          AS "Comissao Produto",
                taxes_amount                           AS "Imposto Produto",
                'Mercado Livre'                        AS "Canal de venda",
                seller_nickname                        AS "Canal Apelido",
                repasse_financeiro                     AS "Repasse Financeiro",
                sku                                    AS "Sku",
                nome_produto                           AS "Nome Produto",
                NULL::NUMERIC                          AS "Custo Total",
                NULL::NUMERIC                          AS "Margem Produto",
                categoria_id                           AS "Categoria",
                frete_item                             AS "Frete Pago Prod"
            FROM {t}"""

        unions = "\n            UNION ALL".join(ml_select(t) for t in tabelas)

        view_sql = f"""
            CREATE OR REPLACE VIEW bd_vendas_consolidado AS
            {unions}
        """

        conn.execute(text(view_sql))
        conn.commit()

    print(f"  [View] bd_vendas_consolidado recriada com {len(tabelas)} tabela(s): {', '.join(tabelas)}")


# ─────────────────────────────────────────────────────────────
# ETL RUNNER (usa as funções do etl.py existente sem modificar)
# ─────────────────────────────────────────────────────────────
def run_ml_etl(engine, access_token: str, user_id: str, account_id: str,
               enrich_ship: bool = True, company: str = "lanzi", full_reload: bool = False) -> dict:
    """
    Executa o ETL do ML usando tabela dinâmica por conta (bd_vendas_ml_{account_id}).
    Monkey-patcha as funções de banco do etl_mod para usar a tabela correta.
    Após o ETL, recria a view bd_vendas_consolidado.
    """
    import io as _io
    import mercadolivre.etl as etl_mod
    import db as db_mod

    table_name = _sanitize_table_name(account_id)
    print(f"  [Worker] Tabela alvo: {table_name}")

    # Salvar estado original
    original_get_access    = etl_mod.get_access_token
    original_carregar      = etl_mod.carregar_tokens
    original_criar         = etl_mod.criar_tabela_se_necessario
    original_upsert        = etl_mod.upsert_banco
    original_sync          = etl_mod.obter_data_ultimo_sync
    original_engine        = db_mod._engine
    original_account_label = etl_mod.ML_ACCOUNT_LABEL

    # Derivar label legível do account_id removendo prefixos ml_ repetidos
    # ex: ml_lanzi → "Lanzi", ml_marcon → "Marcon", ml_ml_matriz → "Matriz", ml_ml_filial → "Filial"
    import re as _re
    _raw = _re.sub(r'^(ml_)+', '', account_id.lower()).replace("_", " ").strip()
    account_label = _raw.title() if _raw else account_id
    print(f"  [Worker] Conta detectada como: {account_label} (account_id={account_id})")

    # ── Versões patchadas com tabela dinâmica ──
    def criar_tabela_patch(eng):
        with eng.connect() as conn:
            conn.execute(text(
                etl_mod.criar_tabela_se_necessario.__doc__ or ""  # não usado
            )) if False else None
        # Reutilizar SQL original substituindo o nome fixo
        import inspect
        src = inspect.getsource(original_criar)
        # Chamar original e depois renomear se necessário
        original_criar(eng)
        # Criar cópia com nome dinâmico se ainda não existe
        with eng.connect() as conn:
            conn.execute(text(f"""
                CREATE TABLE IF NOT EXISTS {table_name}
                (LIKE bd_vendas_ml INCLUDING ALL)
            """))
            conn.commit()

    def upsert_patch(eng, df):
        cols        = list(df.columns)
        cols_quoted = ", ".join(f'"{c}"' for c in cols)
        update_set  = ",\n                    ".join(
            f'"{c}" = EXCLUDED."{c}"'
            for c in cols if c not in ("order_id", "produto_id")
        )
        raw = eng.raw_connection()
        try:
            with raw.cursor() as cur:
                cur.execute(f"CREATE TEMP TABLE _tmp_ml AS SELECT * FROM {table_name} LIMIT 0")
                buf = _io.StringIO()
                df.to_csv(buf, index=False, header=False, sep="\t", na_rep="\\N",
                          date_format="%Y-%m-%d")
                buf.seek(0)
                cur.copy_expert(
                    f'COPY _tmp_ml ({cols_quoted}) FROM STDIN '
                    f"WITH (FORMAT CSV, DELIMITER E'\\t', NULL '\\N')",
                    buf,
                )
                cur.execute(f"""
                    INSERT INTO {table_name} ({cols_quoted})
                    SELECT {cols_quoted} FROM _tmp_ml
                    ON CONFLICT (order_id, produto_id) DO UPDATE SET
                        {update_set}
                """)
            raw.commit()
            print(f"  ✔  Upsert em {table_name} — {len(df):,} registros")
        finally:
            raw.close()

    def sync_patch(eng):
        try:
            with eng.connect() as conn:
                resultado = conn.execute(
                    text(f"SELECT MAX(data) FROM {table_name}")
                ).scalar()
            if resultado:
                from datetime import timedelta
                import pandas as _pd
                data_base = _pd.to_datetime(resultado) - timedelta(days=etl_mod.OVERLAP_DAYS)
                data_str  = data_base.strftime("%Y-%m-%d")
                print(f"  Último registro em {table_name}: {_pd.to_datetime(resultado).strftime('%d/%m/%Y')}")
                print(f"  Buscando desde: {data_str}")
                return data_str
        except Exception:
            pass
        return etl_mod.FIRST_DATE

    # ── Verificar se existe carga anterior (last_sync) ──
    chave_sync = f"{account_id}_last_sync"
    last_sync  = None
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text("SELECT valor FROM configuracoes WHERE empresa=:e AND chave=:c"),
                {"e": account_id.split("_")[0] if "_" in account_id else account_id, "c": chave_sync}
            ).fetchone()
            if not row:
                # Tenta com o nome da empresa (company)
                row = conn.execute(
                    text("SELECT valor FROM configuracoes WHERE chave=:c"),
                    {"c": chave_sync}
                ).fetchone()
        last_sync = row[0] if row else None
    except Exception:
        last_sync = None

    if full_reload:
        last_sync = None  # ignora last_sync para forçar recarga total
    modo = "full reload forçado" if full_reload else ("incremental" if last_sync else "full load")
    print(f"  [Worker] Modo: {modo}" + (f" (desde {last_sync[:10]})" if last_sync else f" (desde FIRST_DATE)"))

    with _etl_lock:  # serializa ETLs para evitar race condition no monkey-patch
        try:
            etl_mod.get_access_token            = lambda: access_token
            etl_mod.carregar_tokens             = lambda: {"user_id": user_id, "access_token": access_token}
            etl_mod.criar_tabela_se_necessario  = criar_tabela_patch
            etl_mod.upsert_banco                = upsert_patch
            etl_mod.obter_data_ultimo_sync      = sync_patch
            db_mod._engine                      = engine
            etl_mod.ENRICH_SHIP                 = enrich_ship
            etl_mod.FULL_RELOAD                 = full_reload
            etl_mod.ML_ACCOUNT_LABEL            = account_label

            etl_mod.main()

            with engine.connect() as conn:
                total = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()

            # Salvar last_sync = agora (para próximo run ser incremental)
            now_iso = datetime.utcnow().isoformat()
            try:
                with engine.connect() as conn:
                    conn.execute(text("""
                        INSERT INTO configuracoes (empresa, chave, valor, atualizado_em)
                        VALUES (:e, :c, :v, NOW())
                        ON CONFLICT (empresa, chave) DO UPDATE
                        SET valor = EXCLUDED.valor, atualizado_em = NOW()
                    """), {"e": company, "c": chave_sync, "v": now_iso})
                    conn.commit()
                print(f"  [Worker] last_sync salvo: {now_iso}")
            except Exception as se:
                print(f"  [Worker] WARN: não salvou last_sync: {se}")

            # Recriar view consolidada
            recriar_view_consolidada(engine)

            return {"status": "ok", "tabela": table_name, "total_registros": total, "modo": modo}

        finally:
            etl_mod.get_access_token            = original_get_access
            etl_mod.carregar_tokens             = original_carregar
            etl_mod.criar_tabela_se_necessario  = original_criar
            etl_mod.upsert_banco                = original_upsert
            etl_mod.obter_data_ultimo_sync      = original_sync
            db_mod._engine                      = original_engine
            etl_mod.ML_ACCOUNT_LABEL            = original_account_label


# ─────────────────────────────────────────────────────────────
# JOB ASYNC — ETL + METABASE
# ─────────────────────────────────────────────────────────────
def _run_job(job_id: str, company: str, account_id: str, enrich_ship: bool = True, full_reload: bool = False):
    """Thread que executa ETL + provisioning Metabase."""
    try:
        print(f"\n{'═'*60}")
        print(f"  [Job {job_id[:8]}] Iniciando ETL para {company}/{account_id}")
        print(f"  {datetime.now():%d/%m/%Y %H:%M:%S}")
        print(f"{'═'*60}")

        # 1. Conectar ao banco da empresa
        engine = get_company_engine(company)
        print(f"  [1/4] Conectado ao banco de {company}")

        # 2. Ler tokens do banco
        tokens = carregar_tokens_do_banco(engine, company, account_id)
        if not tokens:
            raise RuntimeError(f"Tokens não encontrados para {company}/{account_id}")
        print(f"  [2/4] Tokens carregados (user: {tokens.get('nickname', tokens.get('user_id', '?'))})")

        # Renovar se necessário
        access_token = renovar_token_se_necessario(engine, company, account_id, tokens)
        user_id = tokens.get("user_id", "")

        # 3. Executar ETL
        print(f"  [3/4] Executando ETL ML... (enrich_ship={enrich_ship})")
        etl_result = run_ml_etl(engine, access_token, user_id, account_id, enrich_ship=enrich_ship, company=company, full_reload=full_reload)
        print(f"  [3/4] ETL concluído: {etl_result}")

        # 4. Provisionar Metabase
        print(f"  [4/4] Provisionando Metabase...")
        try:
            from metabase_api import provisionar_metabase
            db_config = get_company_db_config(company)
            mb_result = provisionar_metabase(company, db_config)
            print(f"  [4/4] Metabase: {mb_result}")
        except Exception as mb_err:
            print(f"  [4/4] Metabase warning: {mb_err}")
            mb_result = {"error": str(mb_err)}

        # Registrar sucesso
        with _jobs_lock:
            _jobs[job_id].update({
                "status": "done",
                "finished_at": datetime.now().isoformat(),
                "result": {
                    "etl": etl_result,
                    "metabase": mb_result,
                },
            })

        print(f"\n{'═'*60}")
        print(f"  [Job {job_id[:8]}] ✔ Concluído!")
        print(f"{'═'*60}")

    except Exception as e:
        traceback.print_exc()
        with _jobs_lock:
            _jobs[job_id].update({
                "status": "error",
                "finished_at": datetime.now().isoformat(),
                "error": str(e),
            })


# ─────────────────────────────────────────────────────────────
# ENDPOINTS
# ─────────────────────────────────────────────────────────────
@app.route("/etl/status", methods=["GET"])
def etl_status():
    """Health check."""
    return jsonify({
        "status": "running",
        "time": datetime.now().isoformat(),
        "companies": list(COMPANIES.keys()),
    })


@app.route("/etl/trigger", methods=["POST"])
def etl_trigger():
    """
    Dispara ETL + Metabase para uma empresa.
    Body: { "company": "lanzi", "account_id": "ml_conta1", "secret": "...", "enrich_ship": true }
    Retorna 202 Accepted com job_id para acompanhar.
    """
    # Validar secret
    data = request.get_json(force=True, silent=True) or {}
    secret = data.get("secret", "")
    if ETL_SECRET and secret != ETL_SECRET:
        return jsonify({"error": "Unauthorized"}), 401

    company    = data.get("company", "").lower().strip()
    account_id = data.get("account_id", "").strip()
    enrich_ship = bool(data.get("enrich_ship", True))
    full_reload  = bool(data.get("full_reload", False))

    if not company:
        return jsonify({"error": "company é obrigatório"}), 400
    if not account_id:
        return jsonify({"error": "account_id é obrigatório"}), 400
    if company not in COMPANIES:
        return jsonify({"error": f"Empresa '{company}' não configurada", "available": list(COMPANIES.keys())}), 400

    # Criar job
    job_id = str(uuid.uuid4())
    with _jobs_lock:
        _jobs[job_id] = {
            "status": "running",
            "company": company,
            "account_id": account_id,
            "started_at": datetime.now().isoformat(),
            "finished_at": None,
            "result": None,
            "error": None,
        }

    # Disparar em thread (não bloqueia o response)
    t = threading.Thread(target=_run_job, args=(job_id, company, account_id, enrich_ship, full_reload), daemon=True)
    t.start()

    return jsonify({
        "ok": True,
        "job_id": job_id,
        "message": f"ETL iniciado para {company}/{account_id} (enrich_ship={enrich_ship})",
    }), 202


@app.route("/etl/remove-account", methods=["POST"])
def etl_remove_account():
    """
    Remove uma conta ML: dropa a tabela de vendas e apaga os tokens/configs do banco.
    Body: { "company": "lanzi", "account_id": "ml_ml_matriz", "secret": "..." }
    """
    data = request.get_json(force=True, silent=True) or {}
    secret = data.get("secret", "")
    if ETL_SECRET and secret != ETL_SECRET:
        return jsonify({"error": "Unauthorized"}), 401

    company    = data.get("company", "").lower().strip()
    account_id = data.get("account_id", "").strip()

    if not company or not account_id:
        return jsonify({"error": "company e account_id são obrigatórios"}), 400
    if company not in COMPANIES:
        return jsonify({"error": f"Empresa '{company}' não configurada"}), 400

    try:
        engine     = get_company_engine(company)
        table_name = _sanitize_table_name(account_id)
        dropped    = False
        keys_deleted = 0

        with engine.connect() as conn:
            # 1. Dropar tabela de vendas se existir
            exists = conn.execute(text(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name=:t)"
            ), {"t": table_name}).scalar()

            if exists:
                conn.execute(text(f"DROP TABLE {table_name}"))
                dropped = True

            # 2. Apagar todos os tokens/configs da conta em configuracoes
            result = conn.execute(text(
                "DELETE FROM configuracoes WHERE chave LIKE :prefix"
            ), {"prefix": f"{account_id}%"})
            keys_deleted = result.rowcount

            conn.commit()

        # 3. Recriar view consolidada (sem a tabela removida)
        recriar_view_consolidada(engine)

        print(f"  [Remove] {company}/{account_id} removido. Tabela: {table_name} dropped={dropped}, keys={keys_deleted}")
        return jsonify({
            "ok": True,
            "tabela_dropada": table_name if dropped else None,
            "chaves_removidas": keys_deleted,
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/etl/jobs", methods=["GET"])
def etl_jobs():
    """Lista jobs recentes."""
    with _jobs_lock:
        # Últimos 20 jobs, mais recentes primeiro
        sorted_jobs = sorted(_jobs.items(), key=lambda x: x[1].get("started_at", ""), reverse=True)[:20]
    return jsonify([{"job_id": jid, **jdata} for jid, jdata in sorted_jobs])


@app.route("/etl/jobs/<job_id>", methods=["GET"])
def etl_job_detail(job_id):
    """Detalhes de um job específico."""
    with _jobs_lock:
        job = _jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job não encontrado"}), 404
    return jsonify({"job_id": job_id, **job})


# ─────────────────────────────────────────────────────────────
# TINY ERP ETL
# ─────────────────────────────────────────────────────────────
TINY_API_BASE  = "https://api.tiny.com.br/public-api/v3"
TINY_TOKEN_URL = "https://accounts.tiny.com.br/realms/tiny/protocol/openid-connect/token"


def _tiny_get(path, access_token, params=None):
    """GET autenticado na API Tiny v3 com retry em 429."""
    headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}
    for attempt in range(5):
        resp = requests.get(f"{TINY_API_BASE}{path}", headers=headers, params=params or {}, timeout=30)
        if resp.status_code == 429:
            wait = 60 * (attempt + 1)
            print(f"\n  [429 Tiny] Rate limit — aguardando {wait}s...")
            time.sleep(wait)
            continue
        if resp.status_code == 404:
            raise requests.exceptions.HTTPError(f"404 Not Found: {path}", response=resp)
        resp.raise_for_status()
        return resp.json()
    raise RuntimeError("Tiny API rate limit excedido após 5 tentativas")


def _tiny_get_access_token(engine, company, account_id):
    """Retorna access_token válido, renovando se expirado."""
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT chave, valor FROM configuracoes WHERE chave LIKE :p"),
            {"p": f"{account_id}%"}
        ).fetchall()
    cfg = {r[0]: r[1] for r in rows}

    access_token  = cfg.get(f"{account_id}_token", "")
    refresh_token = cfg.get(f"{account_id}_refresh", "")
    exp_at        = cfg.get(f"{account_id}_exp", "")

    needs_refresh = False
    if exp_at:
        try:
            from dateutil import parser as _dp
            exp = _dp.parse(exp_at)
            if not exp.tzinfo:
                from datetime import timezone
                exp = exp.replace(tzinfo=timezone.utc)
            from datetime import timezone
            now = datetime.now(timezone.utc)
            needs_refresh = (exp - now).total_seconds() < 300
        except Exception:
            needs_refresh = True

    if needs_refresh and refresh_token:
        client_id     = os.getenv("TINY_CLIENT_ID", "")
        client_secret = os.getenv("TINY_CLIENT_SECRET", "")
        r = requests.post(TINY_TOKEN_URL, data={
            "grant_type":    "refresh_token",
            "client_id":     client_id,
            "client_secret": client_secret,
            "refresh_token": refresh_token,
        }, timeout=15)
        r.raise_for_status()
        new_tok = r.json()
        access_token  = new_tok["access_token"]
        new_refresh   = new_tok.get("refresh_token", refresh_token)
        new_exp       = (datetime.utcnow() + timedelta(seconds=new_tok.get("expires_in", 21600))).isoformat()
        with engine.connect() as conn:
            for k, v in [
                (f"{account_id}_token",   access_token),
                (f"{account_id}_refresh", new_refresh),
                (f"{account_id}_exp",     new_exp),
            ]:
                conn.execute(text(
                    "INSERT INTO configuracoes (empresa,chave,valor,atualizado_em) VALUES (:empresa,:chave,:valor,NOW()) "
                    "ON CONFLICT (empresa,chave) DO UPDATE SET valor=EXCLUDED.valor,atualizado_em=NOW()"
                ), {"empresa": company, "chave": k, "valor": v})
            conn.commit()

    return access_token


def _tiny_etl_vendas(engine, account_id, access_token):
    """Busca pedidos de venda no Tiny e faz upsert em bd_vendas_tiny_{account_id}."""
    safe = account_id.replace("-", "_").replace(" ", "_").lower()
    table = f"bd_vendas_tiny_{safe}"

    print(f"  [Tiny Vendas] Buscando pedidos → {table}")
    linhas = []
    page = 1
    while True:
        data = _tiny_get("/pedidos", access_token, {"limit": 100, "offset": (page - 1) * 100})
        items = data.get("itens") or data.get("data") or []
        if not items:
            break

        for pedido_resumo in items:
            pid = pedido_resumo.get("id") or pedido_resumo.get("numero")
            if not pid:
                continue
            try:
                det = _tiny_get(f"/pedidos/{pid}", access_token)
                p = det.get("data") or det.get("pedido") or det
            except Exception as e:
                print(f"\n  [WARN] pedido {pid}: {e}")
                continue

            numero    = str(p.get("numero") or p.get("id") or "")
            num_ec    = str(p.get("numeroEcommerce") or p.get("numero_ecommerce") or "")
            data_p    = (str(p.get("dataPedido") or p.get("data_pedido") or "")[:10]) or None
            situacao  = p.get("situacao") or ""
            canal     = p.get("canalVenda") or p.get("canal_venda") or p.get("ecommerce", {}).get("canal") or ""
            plat      = p.get("ecommerce", {}).get("nomePlataforma") or p.get("plataforma") or ""
            cli       = p.get("contato") or p.get("cliente") or {}
            cli_nome  = cli.get("nome") or ""
            cli_doc   = cli.get("cpfCnpj") or cli.get("cpf_cnpj") or ""
            cli_uf    = (cli.get("endereco") or {}).get("uf") or cli.get("uf") or ""
            val_frete = float(p.get("valorFrete") or p.get("valor_frete") or 0)
            val_desc  = float(p.get("valorDesconto") or p.get("valor_desconto") or 0)
            tot_prod  = float(p.get("totalProdutos") or p.get("total_produtos") or 0)
            tot_ped   = float(p.get("total") or p.get("total_pedido") or 0)
            pagamento = (p.get("pagamentos") or [{}])[0]
            forma_pag = pagamento.get("formaPagamento") or pagamento.get("forma_pagamento") or ""
            parcelas  = int(pagamento.get("numeroParcelas") or pagamento.get("numero_parcelas") or 1)
            transp    = p.get("transportadora") or ""
            rastreio  = p.get("codigoRastreamento") or p.get("codigo_rastreamento") or ""

            for item in p.get("itens") or []:
                sku        = str(item.get("sku") or item.get("codigo") or "")
                nome_prod  = item.get("descricao") or item.get("nome") or ""
                qtd        = float(item.get("quantidade") or 0)
                preco_unit = float(item.get("valorUnitario") or item.get("preco_unitario") or 0)
                preco_cust = float(item.get("precoCusto") or item.get("preco_custo") or 0)
                preco_fin  = float(item.get("valorTotal") or item.get("preco_final") or 0)
                desc_item  = float(item.get("desconto") or 0)

                if not sku:
                    continue

                linhas.append({
                    "numero_pedido": numero, "numero_ecommerce": num_ec,
                    "data_pedido": data_p, "situacao": situacao,
                    "canal_venda": canal, "plataforma": plat,
                    "cliente_nome": cli_nome, "cliente_cpf_cnpj": cli_doc, "cliente_uf": cli_uf,
                    "sku": sku, "nome_produto": nome_prod,
                    "quantidade": qtd, "preco_unitario": preco_unit,
                    "preco_custo": preco_cust, "preco_final": preco_fin,
                    "desconto_item": desc_item, "total_produtos": tot_prod,
                    "valor_frete": val_frete, "valor_desconto": val_desc,
                    "total_pedido": tot_ped, "forma_pagamento": forma_pag,
                    "numero_parcelas": parcelas, "transportadora": transp,
                    "codigo_rastreamento": rastreio,
                })
            time.sleep(0.2)

        total = data.get("paginacao", {}).get("total") or data.get("total") or 0
        if page * 100 >= total:
            break
        page += 1
        time.sleep(0.3)

    if not linhas:
        print(f"  [Tiny Vendas] Nenhum pedido encontrado")
        return 0

    with engine.connect() as conn:
        for row in linhas:
            cols = ", ".join(row.keys())
            vals = ", ".join(f":{k}" for k in row.keys())
            upd  = ", ".join(f"{k}=EXCLUDED.{k}" for k in row.keys() if k not in ("numero_pedido", "sku"))
            conn.execute(text(
                f"INSERT INTO {table} ({cols}) VALUES ({vals}) "
                f"ON CONFLICT (numero_pedido, sku) DO UPDATE SET {upd}, atualizado_em=NOW()"
            ), row)
        conn.commit()

    print(f"  [Tiny Vendas] {len(linhas)} linhas salvas em {table}")
    return len(linhas)


def _tiny_etl_estoque(engine, account_id, access_token):
    """Busca produtos/estoque no Tiny e faz upsert em bd_estoque_tiny_{account_id}."""
    safe  = account_id.replace("-", "_").replace(" ", "_").lower()
    table = f"bd_estoque_tiny_{safe}"

    print(f"  [Tiny Estoque] Buscando produtos → {table}")
    linhas = []
    page = 1
    while True:
        data = _tiny_get("/produtos", access_token, {"limit": 100, "offset": (page - 1) * 100})
        items = data.get("itens") or data.get("data") or []
        if not items:
            break

        for prod in items:
            sku      = str(prod.get("sku") or prod.get("codigo") or "")
            if not sku:
                continue
            linhas.append({
                "sku":            sku,
                "nome":           prod.get("nome") or prod.get("descricao") or "",
                "unidade":        prod.get("unidade") or "",
                "estoque_atual":  float(prod.get("estoque", {}).get("saldoFisicoTotal") or prod.get("saldoEstoque") or 0),
                "estoque_minimo": float(prod.get("estoque", {}).get("estoqueMinimo") or prod.get("estoqueMinimo") or 0),
                "preco_custo":    float(prod.get("precoCusto") or prod.get("preco_custo") or 0),
                "preco_venda":    float(prod.get("preco") or prod.get("preco_venda") or 0),
                "marca":          prod.get("marca") or "",
                "categoria":      prod.get("categoria") or "",
            })

        total = data.get("paginacao", {}).get("total") or data.get("total") or 0
        if page * 100 >= total:
            break
        page += 1
        time.sleep(0.3)

    if not linhas:
        print(f"  [Tiny Estoque] Nenhum produto encontrado")
        return 0

    with engine.connect() as conn:
        for row in linhas:
            cols = ", ".join(row.keys())
            vals = ", ".join(f":{k}" for k in row.keys())
            upd  = ", ".join(f"{k}=EXCLUDED.{k}" for k in row.keys() if k != "sku")
            conn.execute(text(
                f"INSERT INTO {table} ({cols}) VALUES ({vals}) "
                f"ON CONFLICT (sku) DO UPDATE SET {upd}, atualizado_em=NOW()"
            ), row)
        conn.commit()

    print(f"  [Tiny Estoque] {len(linhas)} SKUs salvos em {table}")
    return len(linhas)


def _tiny_etl_pedidos_compra(engine, account_id, access_token):
    """Busca pedidos de compra no Tiny e faz upsert em po_tiny_{account_id}."""
    safe  = account_id.replace("-", "_").replace(" ", "_").lower()
    table = f"po_tiny_{safe}"

    print(f"  [Tiny Pedidos Compra] Buscando → {table}")
    linhas = []
    page = 1
    while True:
        try:
            data = _tiny_get("/pedidos-compras", access_token, {"limit": 100, "offset": (page - 1) * 100})
        except requests.exceptions.HTTPError as e:
            if "404" in str(e):
                print(f"  [Tiny Pedidos Compra] Endpoint não disponível nesta conta — pulando")
                return 0
            raise
        items = data.get("itens") or data.get("data") or []
        if not items:
            break

        for pc in items:
            pid = pc.get("id") or pc.get("numero")
            if not pid:
                continue
            try:
                det = _tiny_get(f"/pedidos-compras/{pid}", access_token)
                p = det.get("data") or det.get("pedido") or det
            except Exception as e:
                print(f"\n  [WARN] pedido-compra {pid}: {e}")
                continue

            numero     = str(p.get("numero") or p.get("id") or "")
            fornecedor = (p.get("fornecedor") or {}).get("nome") or p.get("fornecedor") or ""
            data_p     = (str(p.get("dataPedido") or p.get("data_pedido") or "")[:10]) or None
            data_prev  = (str(p.get("dataPrevista") or p.get("data_prevista") or "")[:10]) or None
            situacao   = p.get("situacao") or ""
            tot_ped    = float(p.get("total") or p.get("total_pedido") or 0)

            for item in p.get("itens") or []:
                sku       = str(item.get("sku") or item.get("codigo") or "")
                nome_prod = item.get("descricao") or item.get("nome") or ""
                qtd       = float(item.get("quantidade") or 0)
                preco_u   = float(item.get("valorUnitario") or item.get("preco_unitario") or 0)

                if not sku:
                    continue

                linhas.append({
                    "numero_pedido": numero, "sku": sku,
                    "fornecedor": fornecedor, "data_pedido": data_p,
                    "data_prevista": data_prev, "situacao": situacao,
                    "nome_produto": nome_prod, "quantidade": qtd,
                    "preco_unitario": preco_u, "total_pedido": tot_ped,
                })
            time.sleep(0.2)

        total = data.get("paginacao", {}).get("total") or data.get("total") or 0
        if page * 100 >= total:
            break
        page += 1
        time.sleep(0.3)

    if not linhas:
        print(f"  [Tiny Pedidos Compra] Nenhum pedido encontrado")
        return 0

    with engine.connect() as conn:
        for row in linhas:
            cols = ", ".join(row.keys())
            vals = ", ".join(f":{k}" for k in row.keys())
            upd  = ", ".join(f"{k}=EXCLUDED.{k}" for k in row.keys() if k not in ("numero_pedido", "sku"))
            conn.execute(text(
                f"INSERT INTO {table} ({cols}) VALUES ({vals}) "
                f"ON CONFLICT (numero_pedido, sku) DO UPDATE SET {upd}, atualizado_em=NOW()"
            ), row)
        conn.commit()

    print(f"  [Tiny Pedidos Compra] {len(linhas)} linhas salvas em {table}")
    return len(linhas)


def _run_tiny_job(job_id, company, account_id, modules):
    """Executa ETL Tiny em thread separada."""
    _hdr = f"  [Job {job_id[:8]}]"
    try:
        print(f"\n{'═'*60}\n{_hdr} Iniciando ETL Tiny para {company}/{account_id}\n{'═'*60}")

        engine = get_company_engine(company)
        access_token = _tiny_get_access_token(engine, company, account_id)

        mods_list = [m.strip() for m in modules.split(",")]
        resultado = {}

        if "vendas" in mods_list:
            resultado["vendas"] = _tiny_etl_vendas(engine, account_id, access_token)

        if "estoque" in mods_list:
            resultado["estoque"] = _tiny_etl_estoque(engine, account_id, access_token)

        if "pedidos" in mods_list:
            resultado["pedidos"] = _tiny_etl_pedidos_compra(engine, account_id, access_token)

        print(f"{_hdr} ETL Tiny concluído: {resultado}")
        with _jobs_lock:
            _jobs[job_id].update({"status": "done", "finished_at": datetime.now().isoformat(), "result": resultado})

    except Exception as e:
        tb = traceback.format_exc()
        print(f"{_hdr} ERRO: {e}\n{tb}")
        with _jobs_lock:
            _jobs[job_id].update({"status": "error", "finished_at": datetime.now().isoformat(), "error": str(e)})


@app.route("/etl/trigger-tiny", methods=["POST"])
def etl_trigger_tiny():
    """
    Dispara ETL Tiny ERP para uma empresa.
    Body: { "company": "lanzi", "account_id": "tiny_matriz", "modules": "vendas,estoque,pedidos", "secret": "..." }
    """
    data   = request.get_json(force=True, silent=True) or {}
    secret = data.get("secret", "")
    if ETL_SECRET and secret != ETL_SECRET:
        return jsonify({"error": "Unauthorized"}), 401

    company    = data.get("company", "").lower().strip()
    account_id = data.get("account_id", "").strip()
    modules    = data.get("modules", "vendas,estoque,pedidos")

    if not company or not account_id:
        return jsonify({"error": "company e account_id são obrigatórios"}), 400
    if company not in COMPANIES:
        return jsonify({"error": f"Empresa '{company}' não configurada"}), 400

    job_id = str(uuid.uuid4())
    with _jobs_lock:
        _jobs[job_id] = {
            "status":      "running",
            "company":     company,
            "account_id":  account_id,
            "platform":    "tiny",
            "modules":     modules,
            "started_at":  datetime.now().isoformat(),
            "finished_at": None,
            "result":      None,
            "error":       None,
        }

    t = threading.Thread(target=_run_tiny_job, args=(job_id, company, account_id, modules), daemon=True)
    t.start()

    return jsonify({
        "ok":      True,
        "job_id":  job_id,
        "message": f"ETL Tiny iniciado para {company}/{account_id} (módulos: {modules})",
    }), 202


@app.route("/etl/trigger-tiny-all", methods=["POST"])
def etl_trigger_tiny_all():
    """
    Dispara ETL Tiny para TODAS as contas Tiny configuradas em TODAS as empresas.
    Lê a tabela configuracoes de cada empresa e descobre os account_ids que possuem
    chave no formato {account_id}_token (prefixo 'tiny_').
    Body: { "secret": "...", "modules": "vendas,estoque,pedidos" (opcional) }
    """
    data   = request.get_json(force=True, silent=True) or {}
    secret = data.get("secret", "")
    if ETL_SECRET and secret != ETL_SECRET:
        return jsonify({"error": "Unauthorized"}), 401

    modules = data.get("modules", "vendas,estoque,pedidos")

    jobs_iniciados = []
    erros = []

    for company in COMPANIES.keys():
        try:
            engine = get_company_engine(company)
            with engine.connect() as conn:
                rows = conn.execute(text(
                    "SELECT DISTINCT chave FROM configuracoes "
                    "WHERE chave LIKE 'tiny\\_%\\_token' AND chave NOT LIKE '%\\_refresh' "
                    "  AND chave NOT LIKE '%\\_exp' AND chave NOT LIKE '%\\_nick' "
                    "  AND chave NOT LIKE '%\\_modulos'"
                )).fetchall()

            for (chave,) in rows:
                account_id = chave[:-len("_token")]
                mods = modules

                try:
                    with engine.connect() as conn:
                        r = conn.execute(text(
                            "SELECT valor FROM configuracoes WHERE chave=:k"
                        ), {"k": f"{account_id}_modulos"}).fetchone()
                        if r and r[0]:
                            mods = r[0]
                except Exception:
                    pass

                job_id = str(uuid.uuid4())
                with _jobs_lock:
                    _jobs[job_id] = {
                        "status":      "running",
                        "company":     company,
                        "account_id":  account_id,
                        "platform":    "tiny",
                        "modules":     mods,
                        "started_at":  datetime.now().isoformat(),
                        "finished_at": None,
                        "result":      None,
                        "error":       None,
                    }

                t = threading.Thread(
                    target=_run_tiny_job,
                    args=(job_id, company, account_id, mods),
                    daemon=True,
                )
                t.start()
                jobs_iniciados.append({"company": company, "account_id": account_id, "job_id": job_id})
                print(f"  [TinyAll] Job disparado: {company}/{account_id} → {job_id[:8]}")

        except Exception as e:
            erros.append({"company": company, "error": str(e)})
            print(f"  [TinyAll] Erro ao descobrir contas de {company}: {e}")

    return jsonify({
        "ok":     True,
        "jobs":   jobs_iniciados,
        "errors": erros,
        "total":  len(jobs_iniciados),
    }), 202


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    port = int(os.getenv("WORKER_PORT", "5050"))
    print(f"\n{'═'*60}")
    print(f"  Have ETL Worker")
    print(f"  http://0.0.0.0:{port}")
    print(f"  Companies: {list(COMPANIES.keys())}")
    print(f"  ETL_SECRET: {'✔ configurado' if ETL_SECRET else '⚠ NÃO configurado'}")
    print(f"{'═'*60}\n")
    app.run(host="0.0.0.0", port=port, debug=True)
