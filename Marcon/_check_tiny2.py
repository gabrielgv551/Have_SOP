from sqlalchemy import create_engine, text
engine = create_engine('postgresql+psycopg2://postgres:131105Gv@37.60.236.200:5432/Marcon')
with engine.connect() as conn:
    q = text("SELECT chave, LEFT(valor, 60) as valor FROM configuracoes WHERE chave ILIKE '%tiny%' ORDER BY chave")
    rows = conn.execute(q).fetchall()
    print(f"Total: {len(rows)}")
    for r in rows:
        print(f"{r[0]} = {r[1]}")
