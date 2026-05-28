import sqlalchemy as sa

url = "postgresql+psycopg2://postgres:131105Gv@37.60.236.200:5432/Marcon"
eng = sa.create_engine(url)

with eng.begin() as c:
    before = c.execute(sa.text('SELECT COUNT(*) FROM bd_vendas WHERE "Data" >= :d1 AND "Data" < :d2'),
                       {"d1": "2025-01-01", "d2": "2025-02-01"}).scalar()
    print(f"Registros antes: {before}")

    c.execute(sa.text('DELETE FROM bd_vendas WHERE "Data" >= :d1 AND "Data" < :d2'),
              {"d1": "2025-01-01", "d2": "2025-02-01"})
    print("DELETE executado")

with eng.connect() as c:
    after = c.execute(sa.text('SELECT COUNT(*) FROM bd_vendas WHERE "Data" >= :d1 AND "Data" < :d2'),
                      {"d1": "2025-01-01", "d2": "2025-02-01"}).scalar()
    print(f"Registros depois: {after}")
