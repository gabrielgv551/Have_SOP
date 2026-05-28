"""Cria a tabela contas_pagar no banco Lanzi."""
from sqlalchemy import create_engine, text

engine = create_engine("postgresql+psycopg2://postgres:131105Gv@37.60.236.200:5432/Lanzi")

DDL = """
CREATE TABLE IF NOT EXISTS contas_pagar (
    id              TEXT,
    situacao        TEXT,
    token_origem    TEXT,
    numero_doc      TEXT,
    historico       TEXT,
    fornecedor      TEXT,
    valor           NUMERIC(15, 2),
    saldo           NUMERIC(15, 2),
    data_vencimento DATE,
    data_emissao    DATE,
    atualizado_em   TIMESTAMP,
    data_calculo    DATE
);
"""

with engine.begin() as conn:
    conn.execute(text(DDL))
    print("[OK] Tabela 'contas_pagar' criada (ou ja existia).")

    r = conn.execute(text("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = 'contas_pagar'
        ORDER BY ordinal_position
    """))
    print("\nColunas:")
    for col, dtype in r:
        print(f"  {col:<25} {dtype}")
