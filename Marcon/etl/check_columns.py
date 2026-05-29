import sqlalchemy as sa
import os
from dotenv import load_dotenv

load_dotenv()

db_config = {
    "host": os.getenv("MARCON_HOST", "37.60.236.200"),
    "port": os.getenv("MARCON_PORT", 5432),
    "database": os.getenv("MARCON_DB", "Marcon"),
    "user": os.getenv("MARCON_USER", "postgres"),
    "password": os.getenv("MARCON_PASSWORD", ""),
}

conn_str = f'postgresql://{db_config["user"]}:{db_config["password"]}@{db_config["host"]}:{db_config["port"]}/{db_config["database"]}'
engine = sa.create_engine(conn_str)

with engine.connect() as conn:
    result = conn.execute(sa.text('SELECT column_name FROM information_schema.columns WHERE table_name = \'bd_vendas\' ORDER BY column_name'))
    cols = [row[0] for row in result]
    print('Colunas em bd_vendas:', len(cols))
    print('Total Venda Pedido existe?', 'Total Venda Pedido' in cols)
    print('Total Venda existe?', 'Total Venda' in cols)
    print('Receita Liquida existe?', 'Receita Liquida' in cols)
    print('Order ID existe?', 'Order ID' in cols)
    print('\nTodas as colunas:')
    for col in sorted(cols):
        print(f'  - {col}')
