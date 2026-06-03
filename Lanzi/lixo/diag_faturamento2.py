import sqlalchemy as sa
url = 'postgresql+psycopg2://postgres:131105Gv@37.60.236.200:5432/Lanzi'
eng = sa.create_engine(url)
with eng.begin() as c:
    r = c.execute(sa.text("""
        SELECT 
            SUM("Total Venda") as sum_total_venda,
            (SELECT SUM(tvp) FROM (
                SELECT MAX("Total Venda Pedido") as tvp
                FROM bd_vendas
                WHERE "Data" >= '2026-05-01' AND "Data" < '2026-06-01'
                GROUP BY "Order ID"
            ) t) as sum_pedido_unico,
            SUM(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN COALESCE("Total Venda", 0) ELSE 0 END) as receita_liq_venda,
            SUM(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN COALESCE("Total Venda Pedido", 0) ELSE 0 END) as receita_liq_pedido
        FROM bd_vendas 
        WHERE "Data" >= '2026-05-01' AND "Data" < '2026-06-01'
    """)).fetchone()
    print(f'SUM(Total Venda): R$ {r[0]:,.2f}')
    print(f'SUM(Pedido unico): R$ {r[1]:,.2f}')
    print(f'Receita Liq (Total Venda): R$ {r[2]:,.2f}')
    print(f'Receita Liq (Total Venda Pedido): R$ {r[3]:,.2f}')
    print(f'Diferenca bruta: R$ {r[1] - r[0]:,.2f}')
    print(f'Diferenca liquida: R$ {r[3] - r[2]:,.2f}')
