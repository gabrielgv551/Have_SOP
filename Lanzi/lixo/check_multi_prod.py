import sqlalchemy as sa
url = 'postgresql+psycopg2://postgres:131105Gv@37.60.236.200:5432/Lanzi'
eng = sa.create_engine(url)
with eng.begin() as c:
    # Pedidos com multiplos produtos em maio
    r = c.execute(sa.text("""
        SELECT "Order ID", COUNT(*) as n_prod, 
               SUM("Total Venda") as sum_total_venda,
               MAX("Total Venda Pedido") as pedido_total,
               SUM("Frete Recebido") as sum_frete_rec,
               MAX("Frete Recebido") as max_frete_rec
        FROM bd_vendas 
        WHERE "Data" >= '2026-05-01' AND "Data" < '2026-06-01'
        GROUP BY "Order ID"
        HAVING COUNT(*) > 1
        ORDER BY n_prod DESC
        LIMIT 10
    """)).fetchall()
    
    print("Pedidos com multiplos produtos:")
    for row in r:
        oid, n, sum_venda, pedido_total, sum_frete, max_frete = row
        diff = (pedido_total or 0) - (sum_venda or 0)
        print(f"  Order {oid}: {n} prods, sum_prod=R$ {sum_venda:,.2f}, pedido_total=R$ {pedido_total:,.2f}, diff=R$ {diff:,.2f}, sum_frete=R$ {sum_frete:,.2f}")
    
    # Calcular: se cada produto tivesse o valor do pedido (como na planilha?)
    print("\n--- Simulacoes ---")
    r2 = c.execute(sa.text("""
        SELECT 
            SUM("Total Venda") as sum_venda,
            SUM("Total Venda Pedido") as sum_pedido_all_lines,
            (SELECT SUM(tvp) FROM (SELECT MAX("Total Venda Pedido") as tvp FROM bd_vendas WHERE "Data" >= '2026-05-01' AND "Data" < '2026-06-01' GROUP BY "Order ID") t) as sum_pedido_unico,
            SUM("Frete Recebido") as sum_frete,
            SUM("Total Venda") + SUM("Frete Recebido") as venda_mais_frete,
            SUM("Total Venda") + SUM(CASE WHEN "Frete Recebido" > 0 THEN "Frete Recebido" ELSE 0 END) as venda_mais_frete_pos
        FROM bd_vendas 
        WHERE "Data" >= '2026-05-01' AND "Data" < '2026-06-01'
    """)).fetchone()
    
    print(f"SUM(Total Venda): R$ {r2[0]:,.2f}")
    print(f"SUM(Pedido all lines): R$ {r2[1]:,.2f}")
    print(f"SUM(Pedido unico): R$ {r2[2]:,.2f}")
    print(f"SUM(Frete Recebido): R$ {r2[3]:,.2f}")
    print(f"Venda + Frete: R$ {r2[4]:,.2f}")
    print(f"Venda + Frete Pos: R$ {r2[5]:,.2f}")
    print(f"\nAlvo planilha: R$ 1,392,531.86")
    print(f"Diff vs Venda: R$ {1392531.86 - r2[0]:,.2f}")
    print(f"Diff vs Pedido Unico: R$ {1392531.86 - r2[2]:,.2f}")
    print(f"Diff vs Venda+Frete: R$ {1392531.86 - r2[4]:,.2f}")
