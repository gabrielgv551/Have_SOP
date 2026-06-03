import sqlalchemy as sa

url = 'postgresql+psycopg2://postgres:131105Gv@37.60.236.200:5432/Lanzi'
eng = sa.create_engine(url)

with eng.begin() as c:
    # Total venda maio 2026
    r = c.execute(sa.text("""
        SELECT 
            COUNT(*) as registros,
            SUM("Total Venda") as total_venda,
            SUM("Total Venda Pedido") as total_venda_pedido,
            SUM("Receita Bruta") as receita_bruta,
            SUM("Receita Liquida") as receita_liquida,
            SUM("Valor Liquido") as valor_liquido,
            SUM("Valor Liquido Prod") as valor_liquido_prod
        FROM bd_vendas 
        WHERE "Data" >= '2026-05-01' AND "Data" < '2026-06-01'
    """)).fetchone()
    print(f"Registros: {r[0]:,}")
    print(f"Total Venda (soma produtos): R$ {r[1]:,.2f}")
    print(f"Total Venda Pedido: R$ {r[2]:,.2f}")
    print(f"Receita Bruta: R$ {r[3]:,.2f}")
    print(f"Receita Liquida: R$ {r[4]:,.2f}")
    print(f"Valor Liquido (pedido): R$ {r[5]:,.2f}")
    print(f"Valor Liquido Prod: R$ {r[6]:,.2f}")
    
    # Verificar duplicatas
    r2 = c.execute(sa.text("""
        SELECT COUNT(*) FROM (
            SELECT DISTINCT "Order ID", "Produto ID" 
            FROM bd_vendas 
            WHERE "Data" >= '2026-05-01' AND "Data" < '2026-06-01'
        ) t
    """)).scalar()
    print(f"Unique Order+Produto: {r2:,}")
    
    # Por status
    r3 = c.execute(sa.text("""
        SELECT "Status", COUNT(*) as qtd, SUM("Total Venda") as total
        FROM bd_vendas 
        WHERE "Data" >= '2026-05-01' AND "Data" < '2026-06-01'
        GROUP BY "Status"
        ORDER BY total DESC NULLS LAST
    """)).fetchall()
    print("\nPor Status:")
    for s in r3:
        print(f"  {s[0]}: {s[1]:,} registros = R$ {s[2]:,.2f}")
    
    # Comparar com dashboard_kpis query
    r4 = c.execute(sa.text("""
        SELECT
          SUM(COALESCE("Total Venda Pedido", "Total Venda")) AS receita_bruta,
          SUM(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN COALESCE("Total Venda", 0) ELSE 0 END) AS receita_liquida,
          SUM(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN COALESCE("Quantidade Vendida", 0) ELSE 0 END) AS qtd_liquida,
          SUM(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN COALESCE("Margem Produto", 0) ELSE 0 END) AS margem_bruta,
          SUM(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN COALESCE("Custo Total", 0) ELSE 0 END) AS custo_total
        FROM bd_vendas
        WHERE "Ano" = 2026 AND "Mes" = 5
    """)).fetchone()
    print(f"\n=== Dashboard KPIs query ===")
    print(f"Receita Bruta (COALESCE Total Venda Pedido, Total Venda): R$ {r4[0]:,.2f}")
    print(f"Receita Liquida (status ok): R$ {r4[1]:,.2f}")
    print(f"Qtd Liquida: {r4[2]:,.0f}")
    print(f"Margem Bruta: R$ {r4[3]:,.2f}")
    print(f"Custo Total: R$ {r4[4]:,.2f}")
