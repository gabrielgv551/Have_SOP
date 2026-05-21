"""Salva as credenciais do Gefinance na tabela configuracoes do banco Supershop.

Uso:
  python setup_gefinance.py "postgresql://postgres:SENHA@37.60.236.200:5432/Supershop"
"""
import os
import sys
from sqlalchemy import create_engine, text

if len(sys.argv) > 1:
    dsn = sys.argv[1]
else:
    dsn = (
        f"postgresql+psycopg2://"
        f"{os.getenv('SUPERSHOP_USER', 'postgres')}:"
        f"{os.getenv('SUPERSHOP_PASSWORD', '131105Gv')}"
        f"@{os.getenv('SUPERSHOP_HOST', '37.60.236.200')}:"
        f"{os.getenv('SUPERSHOP_PORT', '5432')}/Supershop"
    )

engine = create_engine(dsn)

CREDENCIAIS = {
    "gefinance_email":    "financeiro@supershop.com.br",
    "gefinance_password": "1893210aB@",
}

with engine.begin() as conn:
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS configuracoes (
            empresa       VARCHAR(50)  NOT NULL,
            chave         VARCHAR(100) NOT NULL,
            valor         TEXT,
            atualizado_em TIMESTAMP DEFAULT NOW(),
            PRIMARY KEY (empresa, chave)
        )
    """))

    for chave, valor in CREDENCIAIS.items():
        conn.execute(text("""
            INSERT INTO configuracoes (empresa, chave, valor, atualizado_em)
            VALUES ('supershop', :chave, :valor, NOW())
            ON CONFLICT (empresa, chave)
            DO UPDATE SET valor = EXCLUDED.valor, atualizado_em = NOW()
        """), {"chave": chave, "valor": valor})
        print(f"  [OK] {chave} salvo")

print("\n[OK] Credenciais Gefinance configuradas para Supershop.")
