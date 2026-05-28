"""
Roda todas as migrations do have-gestor-api contra o banco Marcon.
Substitui automaticamente 'lanzi' por 'marcon' nos INSERTs.

Uso:
  python run_migrations.py "postgresql://postgres:senha@host:5432/Marcon"
"""

import os
import re
import sys
import glob
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# ─────────────────────────────────────────────
# CONFIGURAÇÃO
# ─────────────────────────────────────────────
MIGRATIONS_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "..", "have-gestor-api", "migrations"
)

DB_CONFIG = {
    "host"    : os.getenv("MARCON_HOST",     ""),
    "port"    : os.getenv("MARCON_PORT",     "5432"),
    "dbname"  : "Marcon",
    "user"    : os.getenv("MARCON_USER",     "postgres"),
    "password": os.getenv("MARCON_PASSWORD", ""),
}

# Tabelas criadas pelo ETL Python — migrations que as alteram rodam depois
TABELAS_ETL = {"bd_vendas", "forecast_diario", "cadastros_sku",
               "fornecedores_config", "contas_pagar", "vendas_grupos_canais"}


def get_dsn() -> str:
    if len(sys.argv) > 1:
        return sys.argv[1]
    return (
        f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
    )


def adaptar_sql(sql: str) -> str:
    return sql.replace("'lanzi'", "'marcon'")


def split_sql(sql: str) -> list[str]:
    """
    Divide SQL em statements respeitando:
      - blocos dollar-quoted  ($$...$$  ou  $tag$...$tag$)
      - strings entre aspas simples
      - comentários de linha (--)
    """
    statements = []
    buf        = []
    i          = 0
    n          = len(sql)

    while i < n:
        ch = sql[i]

        # Comentário de linha
        if ch == '-' and i + 1 < n and sql[i + 1] == '-':
            end = sql.find('\n', i)
            end = end if end >= 0 else n
            buf.append(sql[i:end])
            i = end
            continue

        # Dollar-quote  $tag$...$tag$
        if ch == '$':
            m = re.match(r'\$([A-Za-z_0-9]*)\$', sql[i:])
            if m:
                tag     = m.group(0)
                close   = sql.find(tag, i + len(tag))
                if close >= 0:
                    buf.append(sql[i: close + len(tag)])
                    i = close + len(tag)
                    continue

        # String entre aspas simples  '...'
        if ch == "'":
            end = i + 1
            while end < n:
                if sql[end] == "'" and (end + 1 >= n or sql[end + 1] != "'"):
                    end += 1
                    break
                if sql[end] == "'" and sql[end + 1] == "'":
                    end += 2
                    continue
                end += 1
            buf.append(sql[i:end])
            i = end
            continue

        # Fim de statement
        if ch == ';':
            stmt = ''.join(buf).strip()
            # Remove comentários do início para checar se há conteúdo real
            sem_comentarios = re.sub(r'--[^\n]*', '', stmt).strip()
            if sem_comentarios:
                statements.append(stmt)
            buf = []
            i  += 1
            continue

        buf.append(ch)
        i += 1

    # Último statement sem ponto-e-vírgula
    stmt = ''.join(buf).strip()
    sem_comentarios = re.sub(r'--[^\n]*', '', stmt).strip()
    if sem_comentarios:
        statements.append(stmt)

    return statements


def is_etl_table_error(msg: str) -> bool:
    """True se o erro é sobre tabela criada pelo ETL (não pela migration)."""
    for t in TABELAS_ETL:
        if t in msg:
            return True
    return False


def main():
    print("=" * 60)
    print("  Migrations → Banco Marcon")
    print("=" * 60)

    dsn = get_dsn()
    try:
        conn = psycopg2.connect(dsn)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        print("[OK] Conexão com o banco Marcon estabelecida.\n")
    except Exception as e:
        print(f"[ERRO] Falha ao conectar: {e}")
        sys.exit(1)

    pattern  = os.path.join(MIGRATIONS_DIR, "*.sql")
    arquivos = sorted(glob.glob(pattern))

    if not arquivos:
        print(f"[ERRO] Nenhuma migration encontrada em: {MIGRATIONS_DIR}")
        sys.exit(1)

    print(f"Encontradas {len(arquivos)} migrations:\n")

    sucesso  = 0
    avisos   = 0
    erros    = 0

    cur = conn.cursor()

    for caminho in arquivos:
        nome = os.path.basename(caminho)

        with open(caminho, "r", encoding="utf-8") as f:
            sql = adaptar_sql(f.read())

        statements = split_sql(sql)
        arquivo_ok = True
        aviso      = False

        for stmt in statements:
            try:
                cur.execute(stmt)
            except psycopg2.Error as e:
                msg = str(e).lower()
                conn.rollback()

                if is_etl_table_error(msg):
                    aviso = True
                else:
                    print(f"  [ERRO] {nome}:\n         {e}")
                    arquivo_ok = False
                    break

        if not arquivo_ok:
            erros += 1
        elif aviso:
            print(f"  [AVISO] {nome}  (tabela ETL ainda não existe — rode após o ETL)")
            avisos += 1
        else:
            print(f"  [OK] {nome}")
            sucesso += 1

    cur.close()
    conn.close()

    print(f"\n{'='*60}")
    print(f"  Resultado: {sucesso} OK · {avisos} avisos · {erros} erros")
    print(f"{'='*60}")

    if erros == 0:
        print("\n[OK] Migrations aplicadas!")
        if avisos:
            print(f"[i]  {avisos} migrations de índice/coluna em tabelas ETL devem")
            print("     ser re-rodadas depois do primeiro UPLOAD_ETL.py e GEFINANCE_ETL.py.")
        print("\n  Próximo passo: rode temp_user.py para criar o usuário admin.")
    else:
        print("\n[!] Corrija os erros acima e rode novamente.")


if __name__ == "__main__":
    main()
