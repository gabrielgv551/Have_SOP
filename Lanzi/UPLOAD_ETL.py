import pandas as pd
from sqlalchemy import create_engine
import sys
import os

# Se um caminho for passado como argumento, use-o. Senão, use o padrão.
if len(sys.argv) > 1:
    arquivo = sys.argv[1]
else:
    arquivo = r"C:\Users\HAVE\Desktop\Arquivos\Have I\Lanzi\ETL Lanzi.xlsx"

DB_CONFIG = { # Recomenda-se usar variáveis de ambiente aqui também
    "host"    : os.getenv("LANZI_HOST", "37.60.236.200"),
    "port"    : os.getenv("LANZI_PORT", 5432),
    "database": os.getenv("LANZI_DB", "Lanzi"),
    "user"    : os.getenv("LANZI_USER", "postgres"),
    "password": os.getenv("LANZI_PASSWORD", "131105Gv"), # A senha deve vir do ambiente
}

# Abas que NÃO devem ser sobrescritas por este script
# (bd_vendas agora vem do GEFINANCE_ETL.py)
ABAS_IGNORADAS = {"bd_vendas", "bd vendas"}

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

ignoradas = 0
for i, (aba, df) in enumerate(todas_abas.items(), start=1):
    nome_tabela = aba.lower().replace(" ", "_")
    if nome_tabela in ABAS_IGNORADAS or aba.lower() in ABAS_IGNORADAS:
        print(f"[{i}/{total}] Ignorando '{aba}' (gerenciada pelo GEFINANCE_ETL)")
        ignoradas += 1
        continue
    print(f"[{i}/{total}] Enviando '{aba}' → tabela '{nome_tabela}' ({len(df)} linhas)...")
    df.to_sql(nome_tabela, engine, if_exists="replace", index=False)
    print(f"           Salva com sucesso!\n")

print(f"[OK] Upload concluído — {total - ignoradas} abas enviadas, {ignoradas} ignorada(s).")