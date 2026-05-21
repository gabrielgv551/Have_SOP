"""Diagnóstico rápido do dashboard Supershop."""
import psycopg2, json

conn = psycopg2.connect(host='37.60.236.200', dbname='Supershop', user='postgres', password='131105Gv')
cur = conn.cursor()

print("=== bd_vendas ===")
cur.execute("SELECT COUNT(*) FROM bd_vendas")
print(f"  Total linhas: {cur.fetchone()[0]}")

cur.execute("SELECT MIN(\"Ano\"), MAX(\"Ano\"), MIN(\"Mes\"), MAX(\"Mes\") FROM bd_vendas")
row = cur.fetchone()
print(f"  Ano min/max: {row[0]} / {row[1]}")
print(f"  Mes min/max: {row[2]} / {row[3]}")

cur.execute("""
    SELECT "Ano", "Mes", COUNT(*) as pedidos, SUM("Total Venda Pedido") as receita
    FROM bd_vendas
    WHERE "Ano" IS NOT NULL AND "Mes" IS NOT NULL
    GROUP BY "Ano", "Mes"
    ORDER BY "Ano", "Mes"
    LIMIT 5
""")
rows = cur.fetchall()
print(f"\n  Primeiros meses com dados:")
for r in rows:
    print(f"    {r[0]}/{r[1]:02d}  pedidos={r[2]}  receita={r[3]}")

cur.execute("""SELECT column_name FROM information_schema.columns
              WHERE table_name='bd_vendas' AND column_name IN ('Ano','Mes','Total Venda','Margem Produto','Status','Data')""")
print(f"\n  Colunas encontradas: {[r[0] for r in cur.fetchall()]}")

# Testa exatamente a query do dashboard_kpis
print("\n=== dashboard_kpis (última data) ===")
cur.execute('SELECT MAX("Data") as max_data, COUNT(*) FILTER (WHERE "Data" IS NULL) AS data_nulls FROM bd_vendas')
r = cur.fetchone()
print(f"  MAX(Data)={r[0]}  nulos={r[1]}")

cur.execute("""
    SELECT DATE_TRUNC('month', MAX("Data"::date)) AS ultimo_mes
    FROM bd_vendas WHERE "Data" IS NOT NULL
""")
ultimo = cur.fetchone()[0]
print(f"  Último mês calculado: {ultimo}")

if ultimo:
    cur.execute("""
        SELECT COUNT(*) FROM bd_vendas
        WHERE DATE_TRUNC('month', "Data"::date) = %s
    """, (ultimo,))
    print(f"  Linhas nesse mês: {cur.fetchone()[0]}")

    cur.execute("""
        SELECT
            COUNT(*) AS total,
            COUNT("Total Venda Pedido") AS tvp_preenchidos,
            SUM("Total Venda Pedido") AS tvp_soma,
            SUM("Total Venda") AS tv_soma
        FROM bd_vendas
        WHERE DATE_TRUNC('month', "Data"::date) = %s
    """, (ultimo,))
    r = cur.fetchone()
    print(f"  Total Venda Pedido — preenchidos={r[1]}/{r[0]}  soma={r[2]}")
    print(f"  Total Venda        — soma={r[3]}")

    cur.execute("""
        SELECT "Ano", "Mes", COUNT(*) as linhas, SUM("Total Venda Pedido") as receita
        FROM bd_vendas
        WHERE "Ano" IS NOT NULL AND "Mes" IS NOT NULL
        GROUP BY "Ano","Mes"
        ORDER BY "Ano" DESC, "Mes" DESC
        LIMIT 5
    """)
    print("\n  Últimos 5 meses no banco:")
    for r in cur.fetchall():
        print(f"    {r[0]}/{r[1]:02d}  linhas={r[2]}  receita={r[3]}")

# Simula exatamente o dashboard_kpis
print("\n=== Simulação dashboard_kpis ===")
cur.execute("""
    WITH lm AS (
      SELECT DATE_TRUNC('month', MAX("Data"::date)) AS m
      FROM bd_vendas WHERE "Data" IS NOT NULL
    ),
    rb AS (
      SELECT SUM(tvp) AS receita_bruta
      FROM (
        SELECT "Order ID", MAX("Total Venda Pedido") AS tvp
        FROM bd_vendas
        WHERE DATE_TRUNC('month', "Data"::date) = (SELECT m FROM lm)
        GROUP BY "Order ID"
      ) t
    )
    SELECT
      EXTRACT(YEAR  FROM "Data"::date) AS ano,
      EXTRACT(MONTH FROM "Data"::date) AS mes,
      (SELECT receita_bruta FROM rb)   AS receita_bruta,
      SUM(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN "Total Venda" ELSE 0 END) AS receita_liquida
    FROM bd_vendas
    WHERE DATE_TRUNC('month', "Data"::date) = (SELECT m FROM lm)
    GROUP BY 1, 2
""")
r = cur.fetchone()
if r:
    print(f"  ano={r[0]} mes={r[1]} receita_bruta={r[2]} receita_liquida={r[3]}")
else:
    print("  [VAZIO] Nenhuma linha retornada!")

print("\n=== curva_abc ===")
cur.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name='curva_abc')")
existe = cur.fetchone()[0]
print(f"  Tabela existe: {existe}")
if existe:
    cur.execute("SELECT COUNT(*) FROM curva_abc")
    print(f"  Linhas: {cur.fetchone()[0]}")

print("\n=== cadastros_sku ===")
cur.execute("SELECT COUNT(*), COUNT(\"Marca\") FROM cadastros_sku")
r = cur.fetchone()
print(f"  Total={r[0]}  com Marca={r[1]}")

print("\n=== Tabelas necessárias ===")
for tabela in ['ponto_pedido', 'forecast_12m', 'ppr_sku', 'semana_pedidos', 'configuracoes']:
    cur.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name=%s)", (tabela,))
    existe = cur.fetchone()[0]
    count = 0
    if existe:
        cur.execute(f'SELECT COUNT(*) FROM {tabela}')
        count = cur.fetchone()[0]
    print(f"  {tabela}: {'OK ' + str(count) + ' linhas' if existe else 'NAO EXISTE'}")

cur.close()
conn.close()
