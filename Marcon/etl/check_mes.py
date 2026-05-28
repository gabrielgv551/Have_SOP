import sqlalchemy as sa, sys

mes = sys.argv[1] if len(sys.argv) > 1 else "2025-02"
y, m = int(mes[:4]), int(mes[5:7])
d1 = f"{mes}-01"
d2 = f"{y}-{m+1:02d}-01" if m < 12 else f"{y+1}-01-01"

url = "postgresql+psycopg2://postgres:131105Gv@37.60.236.200:5432/Marcon"
eng = sa.create_engine(url)

sql = sa.text("""
    SELECT
        COUNT(*)                                  AS linhas,
        COUNT(DISTINCT "Order ID")                AS pedidos_unicos,
        ROUND(SUM("Total Venda")::numeric, 2)     AS receita_bruta,
        ROUND(SUM("Total Venda Pedido")::numeric, 2) AS total_pedido
    FROM bd_vendas
    WHERE "Data" >= :d1 AND "Data" < :d2
""")
with eng.connect() as c:
    r = c.execute(sql, {"d1": d1, "d2": d2}).fetchone()
    print(f"Período:         {d1} → {d2}")
    print(f"Linhas:          {r[0]:,}")
    print(f"Pedidos únicos:  {r[1]:,}")
    print(f"Receita Bruta:   R$ {r[2]:,.2f}")
    print(f"Total Pedido:    R$ {r[3]:,.2f}")
