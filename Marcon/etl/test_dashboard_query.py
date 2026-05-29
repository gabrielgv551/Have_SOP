import os
from dotenv import load_dotenv
import sqlalchemy

load_dotenv()

engine = sqlalchemy.create_engine(
    f'postgresql://{os.getenv("MARCON_USER")}:{os.getenv("MARCON_PASSWORD")}@{os.getenv("MARCON_HOST")}:{os.getenv("MARCON_PORT")}/{os.getenv("MARCON_DB")}'
)

with engine.connect() as conn:
    # Test the dashboard_kpis query
    query = """
        WITH lm AS (SELECT DATE_TRUNC('month', MAX("Data"::date)) AS m FROM bd_vendas WHERE "Data" IS NOT NULL),
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
          (SELECT receita_bruta FROM rb) AS receita_bruta,
          SUM(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN COALESCE("Receita Liquida"::numeric, "Total Venda") ELSE 0 END) AS receita_liquida,
          SUM(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN "Quantidade Vendida"            ELSE 0 END) AS qtd_liquida,
          SUM(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN COALESCE("Margem Produto", 0)  ELSE 0 END) AS margem_bruta,
          SUM(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN COALESCE("Custo Total", 0)     ELSE 0 END) AS custo_total
        FROM bd_vendas
        WHERE DATE_TRUNC('month', "Data"::date) = (SELECT m FROM lm)
        GROUP BY 1, 2
    """
    
    try:
        result = conn.execute(sqlalchemy.text(query))
        row = result.fetchone()
        print(f'Query result: {row}')
    except Exception as e:
        print(f'Query error: {e}')
