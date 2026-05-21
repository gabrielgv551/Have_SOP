import psycopg2
import bcrypt

conn = psycopg2.connect(host='37.60.236.200', dbname='Supershop', user='postgres', password='131105Gv')
cur = conn.cursor()

USUARIOS = [
    ('Administrador', 'admin',  'supershop2024', 'admin'),
    ('Gestor',        'gestor', 'supershop2024', 'gestor'),
    ('Have',          'have',   'supershop2024', 'have'),
]

try:
    cur.execute("SELECT usuario, perfil FROM usuarios WHERE empresa = 'supershop'")
    existentes = cur.fetchall()
    print("Usuários existentes:", existentes)

    for nome, usuario, senha, perfil in USUARIOS:
        hashed = bcrypt.hashpw(senha.encode(), bcrypt.gensalt()).decode('utf-8')
        cur.execute("""
            INSERT INTO usuarios (empresa, nome, usuario, senha_hash, perfil, ativo)
            VALUES ('supershop', %s, %s, %s, %s, TRUE)
            ON CONFLICT (empresa, usuario) DO UPDATE SET senha_hash = EXCLUDED.senha_hash
        """, (nome, usuario, hashed, perfil))
        print(f"  [OK] {usuario} ({perfil}) criado/atualizado — senha: {senha}")

    conn.commit()
    print("\n[OK] Usuários da Supershop prontos.")

except Exception as e:
    print("Erro:", e)
finally:
    cur.close()
    conn.close()
