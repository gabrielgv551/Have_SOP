import psycopg2, psycopg2.extras

conn = psycopg2.connect(host='37.60.236.200', port=5432, database='extratos', user='postgres', password='131105Gv')
cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
cur.execute("""
  SELECT type, amount, description
  FROM transactions
  WHERE client_id = '98d138b9-d8dc-4ec2-ba90-1e025bde158b'
  ORDER BY date DESC LIMIT 10
""")
for r in cur.fetchall():
    print(f"  type={r['type']}  amount={r['amount']}  desc={r['description'][:50]}")
conn.close()
