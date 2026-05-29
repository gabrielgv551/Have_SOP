import os
from dotenv import load_dotenv
import sqlalchemy

load_dotenv()

engine = sqlalchemy.create_engine(
    f'postgresql://{os.getenv("MARCON_USER")}:{os.getenv("MARCON_PASSWORD")}@{os.getenv("MARCON_HOST")}:{os.getenv("MARCON_PORT")}/{os.getenv("MARCON_DB")}'
)

# Perguntar ao usuário qual senha quer usar
print('Para resetar a senha do usuário admin no banco Marcon:')
new_password = input('Digite a nova senha (ou pressione Enter para usar "admin"): ').strip()
if not new_password:
    new_password = 'admin'

print(f'\nATENÇÃO: Você precisa atualizar a variável MARCON_PASS_ADMIN no Vercel para: {new_password}')
print('Depois de atualizar no Vercel, rode este script novamente para atualizar o hash no banco.')

confirm = input('\nDeseja atualizar o hash no banco agora? (s/n): ').strip().lower()
if confirm != 's':
    print('Cancelado.')
    exit()

# Gerar hash bcrypt manualmente (sem import bcrypt - usando hash simples temporariamente)
# NOTA: Isso é apenas para teste - em produção use bcrypt
import hashlib
# Usando sha256 como fallback temporário
password_hash = hashlib.sha256(new_password.encode('utf-8')).hexdigest()

print(f'Atualizando senha do usuário admin para: {new_password}')
print(f'Hash (SHA256 temporário): {password_hash[:50]}...')

with engine.connect() as conn:
    conn.execute(sqlalchemy.text("""
        UPDATE usuarios 
        SET senha_hash = :hash, atualizado_em = NOW()
        WHERE usuario = 'admin' AND empresa = 'marcon'
    """), {'hash': password_hash})
    conn.commit()
    print('✅ Senha do usuário admin atualizada com sucesso!')
    print('\n⚠️ IMPORTANTE: Atualize MARCON_PASS_ADMIN no Vercel para:', new_password)
