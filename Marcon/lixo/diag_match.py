"""Verifica match de SKUs entre as tabelas do S&OP."""
import os
from sqlalchemy import create_engine, text
import pandas as pd

e = create_engine(
    f"postgresql+psycopg2://{os.getenv('MARCON_USER', 'postgres')}:{os.getenv('MARCON_PASSWORD', '')}"
    f"@{os.getenv('MARCON_HOST', '')}:{os.getenv('MARCON_PORT', 5432)}/Marcon"
)

with e.connect() as conn:
    abc  = pd.read_sql('SELECT sku FROM curva_abc LIMIT 5', conn)
    cad  = pd.read_sql('SELECT "Sku" FROM cadastros_sku LIMIT 5', conn)
    est  = pd.read_sql('SELECT "SKU" FROM estoque_consolidado LIMIT 5', conn)
    fore = pd.read_sql('SELECT "Sku" FROM forecast_12m LIMIT 5', conn)
    es_t = pd.read_sql('SELECT sku FROM estoque_seguranca LIMIT 5', conn)

    print("curva_abc:", abc["sku"].tolist())
    print("cadastros_sku:", cad["Sku"].tolist())
    print("estoque_consolidado:", est["SKU"].tolist())
    print("forecast_12m:", fore["Sku"].tolist())
    print("estoque_seguranca:", es_t["sku"].tolist())

    # Match curva_abc <-> cadastros_sku
    r1 = pd.read_sql("""
        SELECT COUNT(*) AS total,
               SUM(CASE WHEN s."Sku" IS NOT NULL THEN 1 ELSE 0 END) AS com_match
        FROM curva_abc c
        LEFT JOIN cadastros_sku s ON s."Sku" = c.sku
    """, conn)
    print("\nMatch curva_abc <-> cadastros_sku:", r1.to_dict("records"))

    # Match estoque_seguranca <-> estoque_consolidado
    r2 = pd.read_sql("""
        SELECT COUNT(*) AS total_es,
               SUM(CASE WHEN ec.sku IS NOT NULL THEN 1 ELSE 0 END) AS com_estoque
        FROM estoque_seguranca es
        LEFT JOIN (
            SELECT "SKU" AS sku, SUM("Estoque Base") AS eb
            FROM estoque_consolidado GROUP BY "SKU"
        ) ec ON ec.sku = es.sku
    """, conn)
    print("Match estoque_seguranca <-> estoque_consolidado:", r2.to_dict("records"))

    # Match forecast_12m <-> estoque_consolidado
    r3 = pd.read_sql("""
        SELECT COUNT(DISTINCT f."Sku") AS skus_forecast,
               SUM(CASE WHEN ec."SKU" IS NOT NULL THEN 1 ELSE 0 END) AS com_estoque
        FROM (SELECT DISTINCT "Sku" FROM forecast_12m) f
        LEFT JOIN (SELECT DISTINCT "SKU" FROM estoque_consolidado) ec
          ON ec."SKU" = f."Sku"
    """, conn)
    print("Match forecast_12m <-> estoque_consolidado:", r3.to_dict("records"))

    # SKUs sem match — exemplos
    r4 = pd.read_sql("""
        SELECT f."Sku" AS sku_forecast, ec."SKU" AS sku_estoque
        FROM (SELECT DISTINCT "Sku" FROM forecast_12m) f
        LEFT JOIN (SELECT DISTINCT "SKU" FROM estoque_consolidado) ec
          ON ec."SKU" = f."Sku"
        WHERE ec."SKU" IS NULL
        LIMIT 10
    """, conn)
    print("\nExemplos de SKUs no forecast sem estoque:", r4["sku_forecast"].tolist())
