import psycopg2, os

try:
    conn = psycopg2.connect(
        host=os.getenv('AMJLS_HOST', '37.60.236.200'), 
        port=os.getenv('AMJLS_PORT', 5432), 
        dbname=os.getenv('AMJLS_DB', 'amjls'), 
        user=os.getenv('AMJLS_USER', 'postgres'), 
        password=os.getenv('AMJLS_PASSWORD', '131105Gv')
    )
    cur = conn.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public';")
    tables = cur.fetchall()
    print("Tables in public schema:")
    for t in tables:
        print(t[0])
except Exception as e:
    print("Error:", e)
