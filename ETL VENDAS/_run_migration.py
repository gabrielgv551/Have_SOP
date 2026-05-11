"""Executa as migrations 031 (bd_vendas_ml schema completo) e 032 (bd_vendas_consolidado) no banco Lanzi."""
import os, re, sys
from dotenv import load_dotenv
load_dotenv()
from sqlalchemy import create_engine, text

DB_TARGET = os.getenv("MIGRATION_DB", "Lanzi")
url = (
    f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT', 5432)}/{DB_TARGET}"
)
engine = create_engine(url)
BASE = r"C:\Users\HAVE\Desktop\Arquivos\Have I\have-gestor-api\migrations"


# ── Passo 1: criar bd_vendas_ml com schema COMPLETO (via etl.py) ──────
# O migration 031 tem schema reduzido; o etl.py tem o schema completo com
# taxes_amount, seller_nickname, e ~101 colunas.
# Dropa a tabela se ela existir com schema antigo e recria com o novo.
print(f"[1/2] Criando bd_vendas_ml (schema completo) em [{DB_TARGET}]...")

# Carrega a função criar_tabela_se_necessario do etl.py
sys.path.insert(0, r"C:\Users\HAVE\Desktop\Arquivos\Have I\ETL VENDAS")
os.environ["DB_NAME"] = DB_TARGET   # aponta engine do db.py para Lanzi

import db as _db_mod
_db_mod._engine = None   # reseta singleton para pegar novo DB_NAME
engine_lanzi = _db_mod.get_engine()

# Dropa a tabela antiga (schema reduzido do 031) para recriar com schema completo
with engine_lanzi.connect() as conn:
    conn.execute(text("DROP TABLE IF EXISTS bd_vendas_ml CASCADE"))
    conn.commit()
print("   Tabela antiga removida (se existia).")

from mercadolivre.etl import criar_tabela_se_necessario
criar_tabela_se_necessario(engine_lanzi)
print("   ✔  bd_vendas_ml OK (schema completo ~101 colunas)")

# ── Passo 2: criar VIEW bd_vendas_consolidado ─────────────────────────
view_sql_file = open(f"{BASE}\\032_create_bd_vendas_consolidado.sql", encoding="utf-8").read()
match_view = re.search(
    r"(CREATE OR REPLACE VIEW\s+bd_vendas_consolidado[\s\S]+?)\n\s*;",
    view_sql_file, re.IGNORECASE
)
if not match_view:
    raise RuntimeError("Bloco CREATE OR REPLACE VIEW bd_vendas_consolidado não encontrado em 032")

print(f"[2/2] Criando VIEW bd_vendas_consolidado em [{DB_TARGET}]...")
with engine_lanzi.connect() as conn:
    conn.execute(text(match_view.group(1)))
    conn.commit()
print("   ✔  VIEW criada com sucesso!")

# ── Validação ─────────────────────────────────────────────────────────
with engine_lanzi.connect() as conn:
    total = conn.execute(text("SELECT COUNT(*) FROM bd_vendas_consolidado")).scalar()
print(f"\n   Total de registros na view: {total:,}")
