from sqlalchemy import create_engine, text

e = create_engine("postgresql+psycopg2://postgres:131105Gv@37.60.236.200:5432/Lanzi")
with e.connect() as conn:
    total      = conn.execute(text('SELECT COUNT(*) FROM bd_vendas')).scalar()
    nulls      = conn.execute(text('SELECT COUNT(*) FROM bd_vendas WHERE "Data" IS NULL')).scalar()
    min_date   = conn.execute(text('SELECT MIN("Data") FROM bd_vendas')).scalar()
    max_date   = conn.execute(text('SELECT MAX("Data") FROM bd_vendas')).scalar()

print(f"Total de registros : {total:,}")
print(f"Datas nulas        : {nulls:,}")
print(f"Data mínima        : {min_date}")
print(f"Data máxima        : {max_date}")
