import pandas as pd
from sqlalchemy import create_engine

arquivo = r"C:\Users\HAVE\Desktop\Arquivos\Have I\Lanzi\ETL Lanzi.xlsx"

DB_CONFIG = {
    "host"    : "37.60.236.200",
    "port"    : 5432,
    "database": "Lanzi",
    "user"    : "postgres",
    "password": "131105Gv",
}

engine = create_engine(
    f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
    f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}",
    connect_args={"options": "-c client_encoding=utf8"}
)

print("Lendo arquivo Excel...")
todas_abas = pd.read_excel(arquivo, sheet_name=None)
total = len(todas_abas)
print(f"{total} abas encontradas.\n")

print("Iniciando upload...\n")

for i, (aba, df) in enumerate(todas_abas.items(), start=1):
    nome_tabela = aba.lower().replace(" ", "_")
    print(f"[{i}/{total}] Enviando '{aba}' → tabela '{nome_tabela}' ({len(df)} linhas)...")
    df.to_sql(nome_tabela, engine, if_exists="replace", index=False)
    print(f"           Salva com sucesso!\n")

print("[OK] Todas as abas foram enviadas com sucesso!")