import os
from sqlalchemy import create_engine, text

e = create_engine(
    f"postgresql+psycopg2://{os.getenv('MARCON_USER', 'postgres')}:{os.getenv('MARCON_PASSWORD', '')}"
    f"@{os.getenv('MARCON_HOST', '')}:{os.getenv('MARCON_PORT', 5432)}/Marcon"
)
with e.connect() as conn:
    conn.execute(text("DROP TABLE IF EXISTS bd_vendas"))
    conn.execute(text("DELETE FROM sync_log WHERE tabela = 'bd_vendas'"))
    conn.commit()
print("Tabela bd_vendas dropada com sucesso.")
