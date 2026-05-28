import sqlalchemy as sa

url = "postgresql+psycopg2://postgres:131105Gv@37.60.236.200:5432/Marcon"
eng = sa.create_engine(url)

sql = sa.text(
    'SELECT COUNT(DISTINCT "Order ID"), ROUND(SUM("Total Venda")::numeric,2) '
    'FROM bd_vendas WHERE "Data" >= :d1 AND "Data" < :d2'
)
with eng.connect() as c:
    row = c.execute(sql, {"d1": "2025-01-01", "d2": "2025-02-01"}).fetchone()
    print("Pedidos unicos:", row[0])
    print("Receita Bruta: R$", row[1])
