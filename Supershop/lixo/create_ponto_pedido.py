import psycopg2
conn = psycopg2.connect(host='37.60.236.200', dbname='Supershop', user='postgres', password='131105Gv')
cur = conn.cursor()
cur.execute("""
    CREATE TABLE IF NOT EXISTS ponto_pedido (
        sku             TEXT PRIMARY KEY,
        estoque_atual   NUMERIC DEFAULT 0,
        ponto_pedido    NUMERIC DEFAULT 0,
        alerta          TEXT    DEFAULT 'SEM DADOS'
    )
""")
cur.execute("""
    CREATE TABLE IF NOT EXISTS estoque_seguranca (
        sku          TEXT PRIMARY KEY,
        media_mensal NUMERIC DEFAULT 0
    )
""")
conn.commit()
print('[OK] Tabelas ponto_pedido e estoque_seguranca criadas.')
conn.close()
