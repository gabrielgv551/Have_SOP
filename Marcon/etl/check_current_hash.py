import os
from dotenv import load_dotenv
import sqlalchemy

load_dotenv()

engine = sqlalchemy.create_engine(
    f'postgresql://{os.getenv("MARCON_USER")}:{os.getenv("MARCON_PASSWORD")}@{os.getenv("MARCON_HOST")}:{os.getenv("MARCON_PORT")}/{os.getenv("MARCON_DB")}'
)

with engine.connect() as conn:
    result = conn.execute(sqlalchemy.text("""
        SELECT usuario, senha_hash, perfil, ativo 
        FROM usuarios 
        WHERE usuario = 'admin' AND empresa = 'marcon'
    """))
    user = result.fetchone()
    if user:
        print(f'Usuário: {user[0]}')
        print(f'Hash atual: {user[1]}')
        print(f'Perfil: {user[2]}')
        print(f'Ativo: {user[3]}')
    else:
        print('Usuário admin não encontrado')
