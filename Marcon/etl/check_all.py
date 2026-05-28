from sqlalchemy import create_engine, text
e = create_engine("postgresql+psycopg2://postgres:131105Gv@37.60.236.200:5432/Marcon")
with e.connect() as c:
    rows = c.execute(text(
        'SELECT "Ano", "Mes", COUNT(*) n, SUM("Total Venda") receita '
        'FROM bd_vendas GROUP BY "Ano","Mes" ORDER BY 1,2'
    )).fetchall()
    total_linhas = sum(r[2] for r in rows)
    total_receita = sum(r[3] or 0 for r in rows)
    for row in rows:
        print(f"  {row[0]}-{row[1]:02d}: {row[2]:,} linhas | R$ {(row[3] or 0):,.0f}")
    print(f"\n  TOTAL: {total_linhas:,} linhas | R$ {total_receita:,.0f}")
