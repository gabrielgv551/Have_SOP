from sqlalchemy import create_engine, text
engine = create_engine("postgresql+psycopg2://postgres:131105Gv@37.60.236.200:5432/Marcon")
with engine.connect() as conn:
    r = conn.execute(text(
        'SELECT COUNT(*) linhas, COUNT(DISTINCT "Order ID") pedidos, '
        'SUM("Total Venda") receita_bruta, '
        'SUM("Total Venda Pedido") total_venda_pedido '
        'FROM bd_vendas WHERE "Ano"=2025 AND "Mes"=2'
    )).fetchone()
    print(f"Linhas:          {r[0]:,}")
    print(f"Pedidos únicos:  {r[1]:,}")
    print(f"Receita Bruta:   R$ {r[2]:,.2f}")
    print(f"Total Venda Ped: R$ {r[3]:,.2f}  (soma repetida por produto)")
