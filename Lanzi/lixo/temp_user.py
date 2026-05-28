import psycopg2
import bcrypt

conn = psycopg2.connect(host='37.60.236.200', dbname='Lanzi', user='postgres', password='131105Gv')
cur = conn.cursor()

try:
    cur.execute("SELECT id, nome, email, role, ativo FROM usuarios")
    users = cur.fetchall()
    print("Usuarios encontrados na tabela:", users)
    
    if not users:
        # Criar admin se nao existir
        password = b"lanzi2024"
        hashed = bcrypt.hashpw(password, bcrypt.gensalt()).decode('utf-8')
        cur.execute("""
            INSERT INTO usuarios (nome, email, senha_hash, role, ativo) 
            VALUES ('Administrador', 'admin', %s, 'admin', TRUE)
        """, (hashed,))
        conn.commit()
        print("Usuario 'admin' com senha 'lanzi2024' criado com sucesso.")

except Exception as e:
    print("Erro:", e)
finally:
    cur.close()
    conn.close()
