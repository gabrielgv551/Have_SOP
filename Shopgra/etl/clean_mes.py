import sqlalchemy as sa, sys

mes = sys.argv[1] if len(sys.argv) > 1 else "2026-05"
d1 = f"{mes}-01"
y, m = int(mes[:4]), int(mes[5:7])
if m == 12:
    d2 = f"{y+1}-01-01"
else:
    d2 = f"{y}-{m+1:02d}-01"

url = "postgresql+psycopg2://postgres:131105Gv@37.60.236.200:5432/shopgra"
eng = sa.create_engine(url)

with eng.begin() as c:
    before = c.execute(sa.text('SELECT COUNT(*) FROM bd_vendas WHERE "Data" >= :d1 AND "Data" < :d2'),
                       {"d1": d1, "d2": d2}).scalar()
    print(f"Registros em {mes}: {before}")
    c.execute(sa.text('DELETE FROM bd_vendas WHERE "Data" >= :d1 AND "Data" < :d2'),
              {"d1": d1, "d2": d2})
    print(f"DELETE executado ({d1} → {d2})")

# Atualiza sync_log para forçar recomeço antes do mês
with eng.begin() as c:
    c.execute(sa.text("""
        INSERT INTO sync_log (tabela, registros, status, origem)
        VALUES ('bd_vendas', 0, 'RESET_MES', 'clean_mes')
    """))
    print("sync_log atualizado.")
