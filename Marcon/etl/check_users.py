import os
from dotenv import load_dotenv
import sqlalchemy

load_dotenv()

engine = sqlalchemy.create_engine(
    f'postgresql://{os.getenv("MARCON_USER")}:{os.getenv("MARCON_PASSWORD")}@{os.getenv("MARCON_HOST")}:{os.getenv("MARCON_PORT")}/{os.getenv("MARCON_DB")}'
)

with engine.connect() as conn:
    # Check if usuarios table exists
    result = conn.execute(sqlalchemy.text("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = 'usuarios'
        )
    """))
    table_exists = result.fetchone()[0]
    print(f'Tabela usuarios existe: {table_exists}')
    
    if table_exists:
        # Check users in the table
        result = conn.execute(sqlalchemy.text('SELECT usuario, perfil, ativo FROM usuarios'))
        users = result.fetchall()
        print(f'Usuários na tabela: {len(users)}')
        for user in users:
            print(f'  - {user[0]} | {user[1]} | ativo: {user[2]}')
    else:
        print('Tabela usuarios não existe - precisa rodar migration marcon_initial_setup.sql')
