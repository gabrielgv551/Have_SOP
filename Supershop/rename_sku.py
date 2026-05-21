import psycopg2
conn = psycopg2.connect(host='37.60.236.200', dbname='Supershop', user='postgres', password='131105Gv')
cur = conn.cursor()
cur.execute('ALTER TABLE cadastros_sku RENAME COLUMN "Nome" TO "Sku"')
conn.commit()
print('[OK] Coluna "Nome" renomeada para "Sku".')
cur.execute('SELECT column_name FROM information_schema.columns WHERE table_name=\'cadastros_sku\' ORDER BY ordinal_position')
print('Colunas:', [r[0] for r in cur.fetchall()])
conn.close()
