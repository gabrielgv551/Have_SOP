import psycopg2

conn = psycopg2.connect(host="37.60.236.200", port=5432, dbname="Marcon", user="postgres", password="131105Gv")
cur = conn.cursor()

cur.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name='sku_kits')")
exists = cur.fetchone()[0]
print(f"Tabela sku_kits existe no banco Marcon: {exists}")

if exists:
    cur.execute("SELECT sku_kit, sku_componente, quantidade, ativo FROM sku_kits WHERE empresa='marcon'")
    rows = cur.fetchall()
    if rows:
        for r in rows:
            print(f"  KIT={r[0]}  COMP={r[1]}  QTY={r[2]}  ativo={r[3]}")
    else:
        print("  (tabela existe mas está vazia para empresa=marcon)")

conn.close()
