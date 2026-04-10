"""Diagnóstico rápido das tabelas usadas por Estoque_Seguranca e Ponto_Pedido."""
from sqlalchemy import create_engine, text

engine = create_engine(
    "postgresql+psycopg2://postgres:131105Gv@37.60.236.200:5432/Lanzi"
)

TABELAS = [
    "bd_vendas",
    "curva_abc",
    "cadastros_sku",
    "estoque_consolidado",
    "estoque_seguranca",
    "forecast_12m",
    "po",
]

with engine.connect() as conn:
    for t in TABELAS:
        try:
            r = conn.execute(text(f'SELECT COUNT(*) FROM "{t}"'))
            n = r.scalar()
            cols = conn.execute(text(
                "SELECT column_name, data_type FROM information_schema.columns "
                f"WHERE table_name='{t}' ORDER BY ordinal_position"
            )).fetchall()
            print(f"\n{'='*50}")
            print(f"  {t}  ({n} linhas)")
            print(f"{'='*50}")
            for col, dtype in cols:
                print(f"  {col:<35} {dtype}")
        except Exception as e:
            print(f"\n[ERRO] {t}: {e}")

    # Testa queries reais
    print("\n\n=== TESTES DE QUERY ===")

    # estoque_consolidado
    try:
        r = conn.execute(text('SELECT "SKU", "Estoque Base" FROM estoque_consolidado LIMIT 2'))
        print("\n[OK] estoque_consolidado (SKU + Estoque Base):")
        for row in r:
            print(" ", dict(row._mapping))
    except Exception as e:
        print(f"\n[ERRO] estoque_consolidado: {e}")

    # po
    try:
        r = conn.execute(text('SELECT "SKU", "Quantidade", "Previsao_Entrega" FROM po LIMIT 2'))
        print("\n[OK] po (SKU + Quantidade + Previsao_Entrega):")
        for row in r:
            print(" ", dict(row._mapping))
    except Exception as e:
        print(f"\n[ERRO] po: {e}")

    # bd_vendas quantidade vendida
    try:
        r = conn.execute(text('SELECT "Sku", "Quantidade Vendida", "Status" FROM bd_vendas LIMIT 2'))
        print("\n[OK] bd_vendas (Sku + Quantidade Vendida + Status):")
        for row in r:
            print(" ", dict(row._mapping))
    except Exception as e:
        print(f"\n[ERRO] bd_vendas: {e}")

    # estoque_seguranca
    try:
        r = conn.execute(text("SELECT sku, estoque_seguranca, lead_time, media_mensal FROM estoque_seguranca LIMIT 2"))
        print("\n[OK] estoque_seguranca (sku + lead_time + media_mensal):")
        for row in r:
            print(" ", dict(row._mapping))
    except Exception as e:
        print(f"\n[ERRO] estoque_seguranca: {e}")
