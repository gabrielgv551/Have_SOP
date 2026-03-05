import pandas as pd
from sqlalchemy import create_engine

arquivo = r"C:\Users\HAVE\Desktop\Arquivos\Have I\Lanzi\ETL Lanzi.xlsx"

usuario = "postgres"
senha = "1234"
host = "localhost"
porta = "5432"
banco = "Lanzi"

engine = create_engine(
    f"postgresql+psycopg2://{usuario}:{senha}@{host}:{porta}/{banco}",
    connect_args={"options": "-c client_encoding=utf8"}
)

abas = [
    "BD VENDAS",
    "Estoque Consolidado",
    "Estoque 1",
    "Full 1",
    "Full 2"
]

for aba in abas:
    df = pd.read_excel(arquivo, sheet_name=aba)
    nome_tabela = aba.lower().replace(" ", "_")
    df.to_sql(nome_tabela, engine, if_exists="replace", index=False)

print("\n[OK] Todas as abas foram enviadas com sucesso!")