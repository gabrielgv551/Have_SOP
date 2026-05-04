import os
import psycopg2
import bcrypt

conn = psycopg2.connect(
    host=os.getenv('MARCON_HOST', ''),
    dbname='Marcon',
    user=os.getenv('MARCON_USER', 'postgres'),
    password=os.getenv('MARCON_PASSWORD', '')
)
cur = conn.cursor()

try:
    cur.execute("SELECT id, nome, email, role, ativo FROM usuarios")
    users = cur.fetchall()
    print("Usuarios encontrados na tabela:", users)
    
    if not users:
        # Criar admin se nao existir
        password = b"marcon2024"
        hashed = bcrypt.hashpw(password, bcrypt.gensalt()).decode('utf-8')
        cur.execute("""
            INSERT INTO usuarios (nome, email, senha_hash, role, ativo) 
            VALUES ('Administrador', 'admin', %s, 'admin', TRUE)
        """, (hashed,))
        conn.commit()
        print("Usuario 'admin' com senha 'marcon2024' criado com sucesso.")

except Exception as e:
    print("Erro:", e)
finally:
    cur.close()
    conn.close()
