import psycopg2

conn = psycopg2.connect(host='37.60.236.200', port=5432, dbname='Marcon', user='postgres', password='131105Gv')
cur = conn.cursor()

cur.execute('DROP TABLE IF EXISTS estoque_consolidado')

cur.execute("""
CREATE TABLE estoque_consolidado (
  produto       TEXT,
  sku           TEXT        NOT NULL,
  estoque_base  NUMERIC     DEFAULT 0,
  origem        TEXT        NOT NULL DEFAULT 'Geral',
  atualizado_em TIMESTAMPTZ DEFAULT NOW()
)
""")

conn.commit()
print('[OK] Tabela estoque_consolidado criada na Marcon')
cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='estoque_consolidado' ORDER BY ordinal_position")
print('Colunas:', [r[0] for r in cur.fetchall()])
cur.close()
conn.close()
