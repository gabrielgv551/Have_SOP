"""Cria a tabela contas_pagar no banco Marcon."""
import os
from sqlalchemy import create_engine, text

engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('MARCON_USER', 'postgres')}:{os.getenv('MARCON_PASSWORD', '')}"
    f"@{os.getenv('MARCON_HOST', '')}:{os.getenv('MARCON_PORT', 5432)}/Marcon"
)

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
