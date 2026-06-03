from sqlalchemy import create_engine, text
engine = create_engine('postgresql+psycopg2://postgres:131105Gv@37.60.236.200:5432/Marcon')
with engine.connect() as conn:
    rows = conn.execute(text("SELECT sku_kit, sku_componente, quantidade FROM sku_kits WHERE empresa='marcon' ORDER BY sku_kit")).fetchall()
    print(f'Total registros em sku_kits: {len(rows)}')
    for r in rows:
        print(f'  {r[0]} -> {r[1]} x{r[2]}')
