"""Verifica e padroniza colunas da tabela cadastros_sku para o padrão esperado pelo dashboard."""
import psycopg2

conn = psycopg2.connect(host='37.60.236.200', dbname='Supershop', user='postgres', password='131105Gv')
cur = conn.cursor()

# Ver colunas atuais
cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='cadastros_sku' ORDER BY ordinal_position")
colunas = [r[0] for r in cur.fetchall()]
print("Colunas atuais:", colunas)

# Mapeamento: nome atual → nome esperado pelo dashboard
ESPERADO = {
    'sku': 'Sku', 'SKU': 'Sku',
    'marca': 'Marca', 'MARCA': 'Marca',
    'categoria': 'Categoria', 'CATEGORIA': 'Categoria',
    'descricao': 'Descricao', 'DESCRICAO': 'Descricao',
    'nome': 'Nome', 'NOME': 'Nome',
}

for col in colunas:
    if col in ESPERADO:
        novo = ESPERADO[col]
        if col != novo:
            print(f'  Renomeando "{col}" → "{novo}"')
            cur.execute(f'ALTER TABLE cadastros_sku RENAME COLUMN "{col}" TO "{novo}"')

conn.commit()
print("\nColunas após fix:")
cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='cadastros_sku' ORDER BY ordinal_position")
print([r[0] for r in cur.fetchall()])

cur.close()
conn.close()
print("\n[OK] Pronto.")
