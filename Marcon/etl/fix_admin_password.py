import os
from dotenv import load_dotenv
import sqlalchemy
import bcrypt

load_dotenv()

engine = sqlalchemy.create_engine(
    f'postgresql://{os.getenv("MARCON_USER")}:{os.getenv("MARCON_PASSWORD")}@{os.getenv("MARCON_HOST")}:{os.getenv("MARCON_PORT")}/{os.getenv("MARCON_DB")}'
)

print('=' * 60)
print('  CORREÇÃO DE SENHA DO USUÁRIO ADMIN - MARCON')
print('=' * 60)
print()

# Usar senha padrão "admin"
new_password = 'admin'

# Gerar hash bcrypt
password_hash = bcrypt.hashpw(new_password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

print(f'Nova senha: {new_password}')
print(f'Hash bcrypt: {password_hash}')
print()

# Atualizar no banco
with engine.connect() as conn:
    conn.execute(sqlalchemy.text("""
        UPDATE usuarios 
        SET senha_hash = :hash, atualizado_em = NOW()
        WHERE usuario = 'admin' AND empresa = 'marcon'
    """), {'hash': password_hash})
    conn.commit()
    print('✅ Hash atualizado no banco de dados com sucesso!')
    print()

print('=' * 60)
print('  PRÓXIMO PASSO OBRIGATÓRIO')
print('=' * 60)
print()
print('Você PRECISA atualizar a variável de ambiente no Vercel:')
print()
print('  Variável: MARCON_PASS_ADMIN')
print(f'  Valor: {new_password}')
print()
print('Passos:')
print('1. Acesse: https://vercel.com/dashboard')
print('2. Vá para o projeto have-gestor-api')
print('3. Settings → Environment Variables')
print('4. Edite MARCON_PASS_ADMIN')
print('5. Cole o valor:', new_password)
print('6. Salve e faça redeploy')
print()
print('=' * 60)
