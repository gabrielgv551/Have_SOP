import psycopg2
conn = psycopg2.connect(host='37.60.236.200', dbname='Supershop', user='postgres', password='131105Gv')
cur = conn.cursor()

print("=== cadastros_sku.Nome (5 primeiros) ===")
cur.execute('SELECT "Nome", "Marca" FROM cadastros_sku LIMIT 5')
for r in cur.fetchall():
    print(f"  Nome={r[0]}  Marca={r[1]}")

print("\n=== bd_vendas.Sku (5 primeiros) ===")
cur.execute('SELECT DISTINCT "Sku" FROM bd_vendas WHERE "Sku" IS NOT NULL LIMIT 5')
for r in cur.fetchall():
    print(f"  Sku={r[0]}")

conn.close()
