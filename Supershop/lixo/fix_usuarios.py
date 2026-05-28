import psycopg2
conn = psycopg2.connect(host='37.60.236.200', dbname='Supershop', user='postgres', password='131105Gv')
cur = conn.cursor()

cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='usuarios' ORDER BY ordinal_position")
colunas = [r[0] for r in cur.fetchall()]
print("Colunas de usuarios:", colunas)

# Adiciona nav_permissoes se não existir
if 'nav_permissoes' not in colunas:
    cur.execute("ALTER TABLE usuarios ADD COLUMN nav_permissoes JSONB")
    conn.commit()
    print("[OK] Coluna nav_permissoes adicionada.")
else:
    print("[SKIP] nav_permissoes já existe.")

conn.close()
