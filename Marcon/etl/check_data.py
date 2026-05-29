import os
from dotenv import load_dotenv
import sqlalchemy

load_dotenv()

engine = sqlalchemy.create_engine(
    f'postgresql://{os.getenv("MARCON_USER")}:{os.getenv("MARCON_PASSWORD")}@{os.getenv("MARCON_HOST")}:{os.getenv("MARCON_PORT")}/{os.getenv("MARCON_DB")}'
)

with engine.connect() as conn:
    result = conn.execute(sqlalchemy.text('SELECT COUNT(*) as total FROM bd_vendas'))
    total = result.fetchone()[0]
    print(f'Total records in bd_vendas: {total}')
    
    result = conn.execute(sqlalchemy.text('SELECT MAX("Data") as max_date FROM bd_vendas'))
    max_date = result.fetchone()[0]
    print(f'Max date in bd_vendas: {max_date}')
