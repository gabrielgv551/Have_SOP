import sqlalchemy as sa
url = 'postgresql+psycopg2://postgres:131105Gv@37.60.236.200:5432/Lanzi'
eng = sa.create_engine(url)
with eng.begin() as c:
    r = c.execute(sa.text("""
        SELECT 
            COUNT(DISTINCT "Order ID") as pedidos_distintos,
            SUM("Total Venda") as total_venda_produtos,
            SUM("Total Venda Pedido") as total_venda_pedido_all_lines
        FROM bd_vendas 
        WHERE "Data" >= '2026-05-01' AND "Data" < '2026-06-01'
    """)).fetchone()
    print(f"Pedidos distintos: {r[0]:,}")
    print(f"Total Venda (produtos): R$ {r[1]:,.2f}")
    print(f"Total Venda Pedido (todas linhas): R$ {r[2]:,.2f}")
    
    r2 = c.execute(sa.text("""
        SELECT SUM(pedido_total) as soma_pedidos_unicos
        FROM (
            SELECT DISTINCT ON ("Order ID") "Order ID", "Total Venda Pedido" as pedido_total
            FROM bd_vendas 
            WHERE "Data" >= '2026-05-01' AND "Data" < '2026-06-01'
        ) t
    """)).fetchone()
    print(f"Total Venda Pedido (pedidos unicos): R$ {r2[0]:,.2f}")
    
    r3 = c.execute(sa.text("""
        SELECT COUNT(*) as pedidos_multi_prod
        FROM (
            SELECT "Order ID", COUNT(*) as n_produtos
            FROM bd_vendas 
            WHERE "Data" >= '2026-05-01' AND "Data" < '2026-06-01'
            GROUP BY "Order ID"
            HAVING COUNT(*) > 1
        ) t
    """)).scalar()
    print(f"Pedidos com multiplos produtos: {r3:,}")
    
    # Verificar exemplo de pedido com multiplos produtos
    r4 = c.execute(sa.text("""
        SELECT "Order ID", COUNT(*) as n_prod, SUM("Total Venda") as sum_prod, MAX("Total Venda Pedido") as pedido_total
        FROM bd_vendas 
        WHERE "Data" >= '2026-05-01' AND "Data" < '2026-06-01'
        GROUP BY "Order ID"
        HAVING COUNT(*) > 1
        ORDER BY n_prod DESC
        LIMIT 5
    """)).fetchall()
    print("\nExemplos de pedidos com multiplos produtos:")
    for row in r4:
        print(f"  Order {row[0]}: {row[1]} produtos, sum produtos=R$ {row[2]:,.2f}, pedido total=R$ {row[3]:,.2f}")
