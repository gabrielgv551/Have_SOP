"""
setup_empresa.py — Onboarding padronizado de nova empresa no Gestor Have
Uso: python setup_empresa.py

Passos executados:
  1. Conecta ao banco da empresa
  2. Roda todas as migrations (CREATE TABLE IF NOT EXISTS — idempotente)
  3. Semeia categorias padrão do Fluxo de Caixa
  4. Cria usuário admin inicial
  5. Exibe variáveis de ambiente para adicionar no Vercel
  6. Exibe bloco para adicionar em companies.js
"""

import psycopg2
import bcrypt
import os
import glob

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURAÇÃO DA NOVA EMPRESA
# ─────────────────────────────────────────────────────────────────────────────
EMPRESA_SLUG   = input("Slug da empresa (ex: supershop): ").strip().lower()
EMPRESA_NOME   = input("Nome exibido (ex: Supershop): ").strip()
DB_HOST        = input("Host do banco (ex: 37.60.236.200): ").strip()
DB_PORT        = input("Porta (Enter = 5432): ").strip() or "5432"
DB_NAME        = input("Nome do banco (ex: Supershop): ").strip()
DB_USER        = input("Usuário do banco (Enter = postgres): ").strip() or "postgres"
DB_PASSWORD    = input("Senha do banco: ").strip()
ADMIN_USUARIO  = input("Usuário admin inicial (Enter = admin): ").strip() or "admin"
ADMIN_SENHA    = input("Senha admin inicial: ").strip()
ADMIN_NOME     = input("Nome do admin (ex: Administrador): ").strip() or "Administrador"

print("\n" + "="*60)
print("Conectando ao banco...")

conn = psycopg2.connect(
    host=DB_HOST, port=int(DB_PORT), dbname=DB_NAME,
    user=DB_USER, password=DB_PASSWORD
)
conn.autocommit = False
cur = conn.cursor()

# ─────────────────────────────────────────────────────────────────────────────
# PASSO 1: Rodar migrations
# ─────────────────────────────────────────────────────────────────────────────
print("\n[1/5] Rodando migrations...")
MIGRATIONS_DIR = os.path.join(os.path.dirname(__file__), "migrations")
migration_files = sorted(glob.glob(os.path.join(MIGRATIONS_DIR, "0*.sql")))

for mf in migration_files:
    name = os.path.basename(mf)
    try:
        with open(mf, "r", encoding="utf-8") as f:
            sql = f.read()
        # Remove INSERT de empresas hardcoded (ex: 'lanzi')
        import re
        sql = re.sub(r"INSERT INTO usuarios.*?ON CONFLICT.*?DO NOTHING;", "", sql, flags=re.DOTALL)
        cur.execute(sql)
        conn.commit()
        print(f"  [OK] {name}")
    except Exception as e:
        conn.rollback()
        print(f"  [WARN] {name}: {e}")

# ─────────────────────────────────────────────────────────────────────────────
# PASSO 2: Semear categorias do Fluxo de Caixa
# ─────────────────────────────────────────────────────────────────────────────
print("\n[2/5] Semeando categorias Fluxo de Caixa...")

CATEGORIAS = [
    ('SALDO INICIAL DO CAIXA', 'saldo_ini', None, 0),
    ('ATIVIDADES OPERACIONAIS', 'section', None, 10),
    ('ENTRADAS', 'section', 'ATIVIDADES OPERACIONAIS', 11),
    ('MERCADO LIVRE', 'item', 'ENTRADAS', 111),
    ('SHOPEE', 'item', 'ENTRADAS', 112),
    ('AMAZON', 'item', 'ENTRADAS', 113),
    ('MAGALU', 'item', 'ENTRADAS', 114),
    ('TIK TOK', 'item', 'ENTRADAS', 115),
    ('ALI EXPRESS', 'item', 'ENTRADAS', 116),
    ('TEMU', 'item', 'ENTRADAS', 117),
    ('KWAI', 'item', 'ENTRADAS', 118),
    ('DAFITI', 'item', 'ENTRADAS', 119),
    ('B2B', 'item', 'ENTRADAS', 120),
    ('OUTRAS ENTRADAS', 'item', 'ENTRADAS', 121),
    ('SAÍDAS', 'section', 'ATIVIDADES OPERACIONAIS', 20),
    ('FORNECEDORES', 'item', 'SAÍDAS', 201),
    ('MATERIAL DE EMBALAGEM', 'item', 'SAÍDAS', 202),
    ('FRETE DE COMPRA', 'item', 'SAÍDAS', 203),
    ('FRETE DE VENDA', 'item', 'SAÍDAS', 204),
    ('MARKETING', 'item', 'SAÍDAS', 205),
    ('PESSOAL - SALÁRIOS E ENCARGOS', 'item', 'SAÍDAS', 206),
    ('PESSOAL - BENEFÍCIOS', 'item', 'SAÍDAS', 207),
    ('BONIFICAÇÕES', 'item', 'SAÍDAS', 208),
    ('RETIRADA SÓCIOS', 'item', 'SAÍDAS', 209),
    ('COMBUSTÍVEL', 'item', 'SAÍDAS', 210),
    ('ALUGUEL', 'item', 'SAÍDAS', 211),
    ('ENERGIA', 'item', 'SAÍDAS', 212),
    ('ÁGUA', 'item', 'SAÍDAS', 213),
    ('MANUTENÇÃO', 'item', 'SAÍDAS', 214),
    ('LIMPEZA', 'item', 'SAÍDAS', 215),
    ('MATERIAIS DE CONSUMO', 'item', 'SAÍDAS', 216),
    ('INTERNET', 'item', 'SAÍDAS', 217),
    ('SISTEMAS', 'item', 'SAÍDAS', 218),
    ('PRESTAÇÃO DE SERVIÇOS', 'item', 'SAÍDAS', 219),
    ('CARTÃO DE CRÉDITO', 'item', 'SAÍDAS', 220),
    ('IMPOSTOS ESTADUAIS', 'item', 'SAÍDAS', 221),
    ('IMPOSTOS FEDERAIS', 'item', 'SAÍDAS', 222),
    ('OUTRAS SAÍDAS', 'item', 'SAÍDAS', 223),
    ('ATIVIDADES NÃO OPERACIONAIS', 'section', None, 50),
    ('ANO ENTRADAS', 'section', 'ATIVIDADES NÃO OPERACIONAIS', 51),
    ('RECEITAS FINANCEIRAS', 'item', 'ANO ENTRADAS', 511),
    ('CAPTAÇÃO DE EMPRÉSTIMOS', 'item', 'ANO ENTRADAS', 512),
    ('RESGATE DE APLICAÇÕES', 'item', 'ANO ENTRADAS', 513),
    ('OUTRAS ENTRADAS / APLICAÇÕES', 'item', 'ANO ENTRADAS', 514),
    ('ANO SAÍDAS', 'section', 'ATIVIDADES NÃO OPERACIONAIS', 54),
    ('IMOBILIZADO', 'item', 'ANO SAÍDAS', 541),
    ('INVESTIMENTOS', 'item', 'ANO SAÍDAS', 542),
    ('PARTICIPAÇÕES', 'item', 'ANO SAÍDAS', 543),
    ('PAGAMENTO DE EMPRÉSTIMOS', 'item', 'ANO SAÍDAS', 544),
    ('JUROS', 'item', 'ANO SAÍDAS', 545),
    ('DESPESAS BANCÁRIAS', 'item', 'ANO SAÍDAS', 546),
    ('DIVIDENDOS', 'item', 'ANO SAÍDAS', 547),
    ('OUTRAS SAÍDAS NÃO OPERACIONAIS', 'item', 'ANO SAÍDAS', 548),
    ('SALDO FINAL', 'saldo_fin', None, 99),
]

cur.execute("SELECT COUNT(*) FROM caixa_categorias WHERE empresa=%s", (EMPRESA_SLUG,))
if cur.fetchone()[0] == 0:
    for nome, tipo, parent, ordem in CATEGORIAS:
        cur.execute("""INSERT INTO caixa_categorias (empresa, nome, tipo, parent, ordem)
                       VALUES (%s,%s,%s,%s,%s) ON CONFLICT (empresa, nome) DO NOTHING""",
                    (EMPRESA_SLUG, nome, tipo, parent, ordem))
    conn.commit()
    print(f"  [OK] {len(CATEGORIAS)} categorias inseridas.")
else:
    print("  [SKIP] Categorias já existem.")

# ─────────────────────────────────────────────────────────────────────────────
# PASSO 3: Criar usuário admin
# ─────────────────────────────────────────────────────────────────────────────
print("\n[3/5] Criando usuário admin...")
senha_hash = bcrypt.hashpw(ADMIN_SENHA.encode(), bcrypt.gensalt(10)).decode()
cur.execute("""
    INSERT INTO usuarios (empresa, nome, usuario, senha_hash, perfil, ativo)
    VALUES (%s,%s,%s,%s,'admin',TRUE)
    ON CONFLICT (empresa, usuario) DO NOTHING
""", (EMPRESA_SLUG, ADMIN_NOME, ADMIN_USUARIO, senha_hash))
conn.commit()
print(f"  [OK] Usuário '{ADMIN_USUARIO}' criado.")

conn.close()

# ─────────────────────────────────────────────────────────────────────────────
# PASSO 4: Exibir variáveis de ambiente para o Vercel
# ─────────────────────────────────────────────────────────────────────────────
KEY = EMPRESA_SLUG.upper()
print(f"""
[4/5] Adicione estas variáveis no Vercel (have-gestor-api → Settings → Environment Variables):

  {KEY}_HOST     = {DB_HOST}
  {KEY}_PORT     = {DB_PORT}
  {KEY}_DB       = {DB_NAME}
  {KEY}_USER     = {DB_USER}
  {KEY}_PASSWORD = {DB_PASSWORD}
""")

# ─────────────────────────────────────────────────────────────────────────────
# PASSO 5: Exibir bloco para companies.js
# ─────────────────────────────────────────────────────────────────────────────
print(f"""[5/5] Adicione este bloco em have-gestor-api/lib/companies.js:

  {EMPRESA_SLUG}: {{
    name: "{EMPRESA_NOME}",
    dbEnvKey: "{KEY}",
    users: {{
      admin:  process.env.{KEY}_PASS_ADMIN,
      gestor: process.env.{KEY}_PASS_GESTOR,
      have:   process.env.{KEY}_PASS_HAVE,
    }}
  }},
""")

print("="*60)
print("Setup concluído! Depois faça: vercel --prod (na pasta have-gestor-api)")
