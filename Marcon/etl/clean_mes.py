import sqlalchemy as sa, sys, os

mes = sys.argv[1] if len(sys.argv) > 1 else "2025-02"
d1 = f"{mes}-01"
# Calcular primeiro dia do próximo mês
y, m = int(mes[:4]), int(mes[5:7])
if m == 12:
    d2 = f"{y+1}-01-01"
else:
    d2 = f"{y}-{m+1:02d}-01"

url = "postgresql+psycopg2://postgres:131105Gv@37.60.236.200:5432/Marcon"
eng = sa.create_engine(url)

with eng.begin() as c:
    before = c.execute(sa.text('SELECT COUNT(*) FROM bd_vendas WHERE "Data" >= :d1 AND "Data" < :d2'),
                       {"d1": d1, "d2": d2}).scalar()
    print(f"Registros em {mes}: {before}")
    c.execute(sa.text('DELETE FROM bd_vendas WHERE "Data" >= :d1 AND "Data" < :d2'),
              {"d1": d1, "d2": d2})
    print(f"DELETE executado ({d1} → {d2})")

# Deletar cache de orders do mês
cache_dir = os.path.join(os.path.dirname(__file__), "cache")
import calendar
last_day = calendar.monthrange(y, m)[1]
cache_file = os.path.join(cache_dir, f"orders_{d1}_{y}-{m:02d}-{last_day}.json")
if os.path.exists(cache_file):
    os.remove(cache_file)
    print(f"Cache deletado: {cache_file}")
else:
    print(f"Cache não encontrado: {cache_file}")
