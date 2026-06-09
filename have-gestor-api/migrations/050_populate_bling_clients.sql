-- Script SQL: Popular tabela de clientes Bling
-- 
-- INSTRUÇÕES:
-- 1. Substitua os placeholders (CLIENT_ID_1, CLIENT_SECRET_1, etc.) pelos valores REAIS do seu Bling
-- 2. Execute este script contra o banco de dados 'bling'
-- 
-- Você encontra as credenciais Bling em:
-- - https://www.bling.com.br/integracao/api/v3 (Suas aplicações registradas)
-- - Environment Variables do Vercel / seu servidor local
-- 

-- Limpar tabela de teste (OPCIONAL - descomente se quiser limpar os placeholders)
-- DELETE FROM clientes WHERE nome LIKE 'cliente_%';

-- Inserir/Atualizar CLIENTE 1 (Autoequip)
INSERT INTO clientes (
  nome, 
  empresa, 
  client_id, 
  client_secret, 
  access_token, 
  refresh_token, 
  expires_at
)
VALUES (
  'Autoequip Store',
  'autoequip',
  'COLE_AQUI_O_CLIENT_ID_AUTOEQUIP',
  'COLE_AQUI_O_CLIENT_SECRET_AUTOEQUIP',
  'COLE_AQUI_O_ACCESS_TOKEN_AUTOEQUIP',
  'COLE_AQUI_O_REFRESH_TOKEN_AUTOEQUIP',
  NOW() + INTERVAL '30 minutes'
)
ON CONFLICT (empresa, nome) 
DO UPDATE SET 
  client_id = EXCLUDED.client_id,
  client_secret = EXCLUDED.client_secret,
  access_token = EXCLUDED.access_token,
  refresh_token = EXCLUDED.refresh_token,
  expires_at = EXCLUDED.expires_at,
  atualizado_em = NOW();

-- Inserir/Atualizar CLIENTE 2 (Outro cliente)
INSERT INTO clientes (
  nome, 
  empresa, 
  client_id, 
  client_secret, 
  access_token, 
  refresh_token, 
  expires_at
)
VALUES (
  'Segundo Cliente',
  'autoequip',
  'COLE_AQUI_O_CLIENT_ID_CLIENTE_2',
  'COLE_AQUI_O_CLIENT_SECRET_CLIENTE_2',
  'COLE_AQUI_O_ACCESS_TOKEN_CLIENTE_2',
  'COLE_AQUI_O_REFRESH_TOKEN_CLIENTE_2',
  NOW() + INTERVAL '30 minutes'
)
ON CONFLICT (empresa, nome) 
DO UPDATE SET 
  client_id = EXCLUDED.client_id,
  client_secret = EXCLUDED.client_secret,
  access_token = EXCLUDED.access_token,
  refresh_token = EXCLUDED.refresh_token,
  expires_at = EXCLUDED.expires_at,
  atualizado_em = NOW();

-- Verificar inserção
SELECT id, nome, empresa, client_id, expires_at, atualizado_em 
FROM clientes 
WHERE empresa = 'autoequip'
ORDER BY nome;
