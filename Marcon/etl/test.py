import pandas as pd
from sqlalchemy import create_engine
engine = create_engine('postgresql+psycopg2://postgres:Havedata2024!@35.199.117.159:5432/postgres')
query = "SELECT Status, SUM(Receita Bruta) as total FROM bd_vendas_precocerto WHERE Data >= '2026-05-01' AND Data <= '2026-05-31' GROUP BY Status"
df = pd.read_sql(query, engine)
print(df)
