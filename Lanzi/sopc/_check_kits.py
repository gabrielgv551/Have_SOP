import psycopg2

conn = psycopg2.connect(host="37.60.236.200", port=5432, dbname="Lanzi", user="postgres", password="131105Gv")
cur = conn.cursor()

print("=== sku_kits (empresa=lanzi) ===")
cur.execute("SELECT sku_kit, sku_componente, quantidade, ativo FROM sku_kits WHERE empresa='lanzi'")
rows = cur.fetchall()
if rows:
    for r in rows:
        print(f"  KIT={r[0]}  COMP={r[1]}  QTY={r[2]}  ativo={r[3]}")
else:
    print("  (vazio — nenhum kit cadastrado)")

print("\n=== ponto_pedido com alerta=KIT ===")
cur.execute("SELECT sku, alerta, qty_sugerida FROM ponto_pedido WHERE alerta='KIT'")
rows2 = cur.fetchall()
if rows2:
    for r in rows2:
        print(f"  SKU={r[0]}  alerta={r[1]}  qty={r[2]}")
else:
    print("  (nenhum SKU marcado como KIT no ponto_pedido)")

conn.close()
