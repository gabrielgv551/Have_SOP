"""Cria tabelas e semeia categorias padrão do Fluxo de Caixa para Supershop."""
import psycopg2

EMPRESA = 'supershop'

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

conn = psycopg2.connect(host='37.60.236.200', dbname='Supershop', user='postgres', password='131105Gv')
cur = conn.cursor()

# Recria tabela com parent TEXT (nome do pai, não ID)
cur.execute("DROP TABLE IF EXISTS caixa_lancamentos")
cur.execute("DROP TABLE IF EXISTS caixa_categorias")
conn.commit()
cur.execute("""
    CREATE TABLE caixa_categorias (
        id SERIAL PRIMARY KEY,
        empresa VARCHAR(50) NOT NULL,
        nome VARCHAR(100) NOT NULL,
        tipo VARCHAR(30) DEFAULT 'item',
        parent TEXT,
        ordem INT DEFAULT 0,
        UNIQUE(empresa, nome)
    )
""")
cur.execute("""
    CREATE TABLE IF NOT EXISTS caixa_lancamentos (
        id SERIAL PRIMARY KEY,
        empresa VARCHAR(50) NOT NULL,
        categoria_id INT,
        ano INT NOT NULL,
        mes INT NOT NULL,
        dia INT,
        valor NUMERIC(15,2) DEFAULT 0,
        descricao TEXT,
        tipo VARCHAR(20) DEFAULT 'previsto',
        subempresa_id INT,
        criado_em TIMESTAMP DEFAULT NOW(),
        atualizado_em TIMESTAMP DEFAULT NOW()
    )
""")
conn.commit()

# Verifica se já tem categorias
cur.execute("SELECT COUNT(*) FROM caixa_categorias WHERE empresa=%s", (EMPRESA,))
count = cur.fetchone()[0]
if count > 0:
    print(f"[SKIP] Já existem {count} categorias para {EMPRESA}.")
else:
    inseridos = 0
    for nome, tipo, parent_nome, ordem in CATEGORIAS:
        cur.execute("""
            INSERT INTO caixa_categorias (empresa, nome, tipo, parent, ordem)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (empresa, nome) DO NOTHING
        """, (EMPRESA, nome, tipo, parent_nome, ordem))
        inseridos += 1
    conn.commit()
    print(f"[OK] {inseridos} categorias inseridas para {EMPRESA}.")

conn.close()
print("Concluído.")
