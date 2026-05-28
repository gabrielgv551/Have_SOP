import psycopg2

conn = psycopg2.connect(host='37.60.236.200', port=5432, dbname='Marcon', user='postgres', password='131105Gv')
cur = conn.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS po (
  id               SERIAL PRIMARY KEY,
  "SKU"            TEXT,
  "Quantidade"     NUMERIC DEFAULT 0,
  "Previsao_Entrega" DATE,
  criado_em        TIMESTAMP DEFAULT NOW()
)
""")

conn.commit()
print('[OK] Tabela po criada na Marcon')
cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='po' ORDER BY ordinal_position")
print('Colunas:', [r[0] for r in cur.fetchall()])
cur.close()
conn.close()
