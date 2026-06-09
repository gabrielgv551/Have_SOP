# -*- coding: utf-8 -*-
"""
Script nao-interativo - continua o setup da Autoequip (banco ja criado).
Roda apenas: categorias, usuarios, sopc_config.
"""
import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

import psycopg2
import bcrypt
import os
import glob
import re
import sys
from pathlib import Path

# ─── Configuração da Autoequip ───────────────────────────────────────────────
EMPRESA_SLUG  = "autoequip"
EMPRESA_NOME  = "Autoequip"
DB_HOST       = "37.60.236.200"
DB_PORT       = "5432"
DB_NAME       = "Autoequip"
DB_USER       = "postgres"
DB_PASSWORD   = "131105Gv"
ADMIN_USUARIO = "admin"
ADMIN_SENHA   = "autoequip2024"
ADMIN_NOME    = "Administrador"

GESTOR_USUARIO = "gestor"
GESTOR_SENHA   = "gestor2024"
HAVE_USUARIO   = "have"
HAVE_SENHA     = "have2024"

MIGRATIONS_DIR = Path(__file__).parent / "migrations"

# ─────────────────────────────────────────────────────────────────────────────
# Banco e migrations ja foram executados. Apenas finaliza o setup.
# ─────────────────────────────────────────────────────────────────────────────
print("=" * 60)
print("  Setup Autoequip - Finalizando (banco ja criado + migrations OK)")
print("="  * 60)

# ─────────────────────────────────────────────────────────────────────────────
# PASSO 1: Conectar ao banco Autoequip
# ─────────────────────────────────────────────────────────────────────────────
print(f"\n[1/4] Conectando ao banco '{DB_NAME}'...")
conn = psycopg2.connect(
    host=DB_HOST, port=int(DB_PORT),
    dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
)
conn.autocommit = False
cur = conn.cursor()
print("  [OK] Conectado!")
print("  [SKIP] Migrations - ja executadas na etapa anterior.")

# ─────────────────────────────────────────────────────────────────────────────
# PASSO 3: Semear categorias Fluxo de Caixa
# ─────────────────────────────────────────────────────────────────────────────
print("\n[2/4] Semeando categorias Fluxo de Caixa...")

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
        cur.execute(
            "INSERT INTO caixa_categorias (empresa, nome, tipo, parent, ordem) "
            "VALUES (%s,%s,%s,%s,%s) ON CONFLICT (empresa, nome) DO NOTHING",
            (EMPRESA_SLUG, nome, tipo, parent, ordem)
        )
    conn.commit()
    print(f"  [OK] {len(CATEGORIAS)} categorias inseridas.")
else:
    print("  [SKIP] Categorias já existem.")

# ─────────────────────────────────────────────────────────────────────────────
# PASSO 4: Criar usuários (admin, gestor, have)
# ─────────────────────────────────────────────────────────────────────────────
print("\n[3/4] Criando usuarios...")

usuarios = [
    (ADMIN_NOME,    ADMIN_USUARIO,   ADMIN_SENHA,   'admin'),
    ('Gestor',      GESTOR_USUARIO,  GESTOR_SENHA,  'gestor'),
    ('Have',        HAVE_USUARIO,    HAVE_SENHA,    'have'),
]

for nome, usuario, senha, perfil in usuarios:
    senha_hash = bcrypt.hashpw(senha.encode(), bcrypt.gensalt(10)).decode()
    cur.execute("""
        INSERT INTO usuarios (empresa, nome, usuario, senha_hash, perfil, ativo)
        VALUES (%s,%s,%s,%s,%s,TRUE)
        ON CONFLICT (empresa, usuario) DO NOTHING
    """, (EMPRESA_SLUG, nome, usuario, senha_hash, perfil))
    print(f"  [OK] Usuário '{usuario}' ({perfil}) criado.")

conn.commit()

# ─────────────────────────────────────────────────────────────────────────────
# PASSO 5: Semear sopc_config
# ─────────────────────────────────────────────────────────────────────────────
print("\n[4/4] Semeando sopc_config S&OP...")

sopc_rows = [
    (EMPRESA_SLUG, 'curva_abc',    'janela_meses',           '6'),
    (EMPRESA_SLUG, 'curva_abc',    'corte_a',                '0.20'),
    (EMPRESA_SLUG, 'curva_abc',    'corte_b',                '0.50'),
    (EMPRESA_SLUG, 'curva_abc',    'nivel_servico_AA',       '0.98'),
    (EMPRESA_SLUG, 'curva_abc',    'nivel_servico_AB',       '0.97'),
    (EMPRESA_SLUG, 'curva_abc',    'nivel_servico_BA',       '0.97'),
    (EMPRESA_SLUG, 'curva_abc',    'nivel_servico_BB',       '0.95'),
    (EMPRESA_SLUG, 'curva_abc',    'nivel_servico_AC',       '0.95'),
    (EMPRESA_SLUG, 'curva_abc',    'nivel_servico_CA',       '0.95'),
    (EMPRESA_SLUG, 'curva_abc',    'nivel_servico_BC',       '0.92'),
    (EMPRESA_SLUG, 'curva_abc',    'nivel_servico_CB',       '0.92'),
    (EMPRESA_SLUG, 'curva_abc',    'nivel_servico_CC',       '0.90'),
    (EMPRESA_SLUG, 'estoque_seg',  'janela_meses',           '12'),
    (EMPRESA_SLUG, 'estoque_seg',  'fator_z_AA',             '2.05'),
    (EMPRESA_SLUG, 'estoque_seg',  'fator_z_AB',             '1.88'),
    (EMPRESA_SLUG, 'estoque_seg',  'fator_z_BA',             '1.88'),
    (EMPRESA_SLUG, 'estoque_seg',  'fator_z_BB',             '1.65'),
    (EMPRESA_SLUG, 'estoque_seg',  'fator_z_AC',             '1.65'),
    (EMPRESA_SLUG, 'estoque_seg',  'fator_z_CA',             '1.65'),
    (EMPRESA_SLUG, 'estoque_seg',  'fator_z_BC',             '1.41'),
    (EMPRESA_SLUG, 'estoque_seg',  'fator_z_CB',             '1.41'),
    (EMPRESA_SLUG, 'estoque_seg',  'fator_z_CC',             '1.28'),
    (EMPRESA_SLUG, 'estoque_seg',  'teto_dias_A',            '20'),
    (EMPRESA_SLUG, 'estoque_seg',  'teto_dias_BC',           '15'),
    (EMPRESA_SLUG, 'prev_12m',     'blend_longo',            '0.40'),
    (EMPRESA_SLUG, 'prev_12m',     'blend_curto',            '0.60'),
    (EMPRESA_SLUG, 'prev_12m',     'peso_t_minus2',          '1'),
    (EMPRESA_SLUG, 'prev_12m',     'peso_t_minus1',          '2'),
    (EMPRESA_SLUG, 'prev_12m',     'peso_t',                 '4'),
    (EMPRESA_SLUG, 'prev_12m',     'min_meses_grupo_a',      '6'),
    (EMPRESA_SLUG, 'ponto_pedido', 'horizonte_demanda_dias', '90'),
    (EMPRESA_SLUG, 'ponto_pedido', 'ciclo_reposicao_dias',   '30'),
    (EMPRESA_SLUG, 'ponto_pedido', 'fator_excesso',          '2.0'),
]

for row in sopc_rows:
    cur.execute(
        "INSERT INTO sopc_config (empresa, modulo, chave, valor) "
        "VALUES (%s,%s,%s,%s) ON CONFLICT (empresa, modulo, chave) DO NOTHING",
        row
    )
conn.commit()
print(f"  [OK] {len(sopc_rows)} configuracoes S&OP inseridas.")

conn.close()

print(f"""
==================================================
  Setup Autoequip CONCLUIDO!

  Banco   : {DB_NAME} @ {DB_HOST}
  Empresa : {EMPRESA_SLUG}

  Usuarios criados:
    admin  / {ADMIN_SENHA}
    gestor / {GESTOR_SENHA}
    have   / {HAVE_SENHA}

  PROXIMOS PASSOS:
  1. Adicione no Vercel (Settings > Environment Variables):
       AUTOEQUIP_HOST      = {DB_HOST}
       AUTOEQUIP_PORT      = {DB_PORT}
       AUTOEQUIP_DB        = {DB_NAME}
       AUTOEQUIP_USER      = {DB_USER}
       AUTOEQUIP_PASSWORD  = {DB_PASSWORD}
       AUTOEQUIP_PASS_ADMIN  = {ADMIN_SENHA}
       AUTOEQUIP_PASS_GESTOR = {GESTOR_SENHA}
       AUTOEQUIP_PASS_HAVE   = {HAVE_SENHA}

  2. Deploy: cd have-gestor-api && vercel --prod
==================================================""")
