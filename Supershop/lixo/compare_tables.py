import psycopg2

def get_tables(host, dbname, password):
    conn = psycopg2.connect(host=host, dbname=dbname, user='postgres', password=password)
    cur = conn.cursor()
    cur.execute("""SELECT table_name FROM information_schema.tables
                   WHERE table_schema='public' AND table_type='BASE TABLE'
                   ORDER BY table_name""")
    tables = {r[0] for r in cur.fetchall()}
    conn.close()
    return tables

lanzi     = get_tables('37.60.236.200', 'Lanzi',     '131105Gv')
supershop = get_tables('37.60.236.200', 'Supershop', '131105Gv')

faltando = sorted(lanzi - supershop)
extras   = sorted(supershop - lanzi)

print(f"Lanzi tem {len(lanzi)} tabelas | Supershop tem {len(supershop)} tabelas")
print(f"\n=== Faltando na Supershop ({len(faltando)}) ===")
for t in faltando:
    print(f"  - {t}")
print(f"\n=== Só na Supershop ({len(extras)}) ===")
for t in extras:
    print(f"  + {t}")
