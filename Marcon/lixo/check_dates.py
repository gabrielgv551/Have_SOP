import os
from sqlalchemy import create_engine, text

e = create_engine(
    f"postgresql+psycopg2://{os.getenv('MARCON_USER', 'postgres')}:{os.getenv('MARCON_PASSWORD', '')}"
    f"@{os.getenv('MARCON_HOST', '')}:{os.getenv('MARCON_PORT', 5432)}/Marcon"
)
with e.connect() as conn:
    total      = conn.execute(text('SELECT COUNT(*) FROM bd_vendas')).scalar()
    nulls      = conn.execute(text('SELECT COUNT(*) FROM bd_vendas WHERE "Data" IS NULL')).scalar()
    min_date   = conn.execute(text('SELECT MIN("Data") FROM bd_vendas')).scalar()
    max_date   = conn.execute(text('SELECT MAX("Data") FROM bd_vendas')).scalar()

print(f"Total de registros : {total:,}")
print(f"Datas nulas        : {nulls:,}")
print(f"Data mínima        : {min_date}")
print(f"Data máxima        : {max_date}")
