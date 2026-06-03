from sqlalchemy import create_engine, text
import pandas as pd
engine = create_engine('postgresql+psycopg2://postgres:131105Gv@37.60.236.200:5432/Marcon')
q = text("SELECT chave, valor FROM configuracoes WHERE chave ILIKE '%tiny%' ORDER BY chave")
df = pd.read_sql(q, engine)
print(df.to_string())
