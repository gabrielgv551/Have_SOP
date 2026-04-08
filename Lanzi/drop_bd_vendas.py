from sqlalchemy import create_engine, text

e = create_engine("postgresql+psycopg2://postgres:131105Gv@37.60.236.200:5432/Lanzi")
with e.connect() as conn:
    conn.execute(text("DROP TABLE IF EXISTS bd_vendas"))
    conn.execute(text("DELETE FROM sync_log WHERE tabela = 'bd_vendas'"))
    conn.commit()
print("Tabela bd_vendas dropada com sucesso.")
