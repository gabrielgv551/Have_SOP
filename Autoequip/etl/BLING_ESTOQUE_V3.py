#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════╗
║  Bling Estoque — API v3 (OAuth2) Centralizado                ║
║  Extrai estoque do Bling ERP via API v3 e salva no PG        ║
║  Lê e renova os tokens a partir do banco central [bling]     ║
║  e grava os dados de estoque no banco da empresa selecionada.║
╚══════════════════════════════════════════════════════════════╝

Dependências:
  pip install requests pandas sqlalchemy psycopg2-binary

Uso:
  # Sincroniza uma conta específica para a empresa Autoequip:
  python BLING_ESTOQUE_V3.py --empresa autoequip --account cliente_1

  # Sincroniza todas as contas cadastradas para a empresa Autoequip:
  python BLING_ESTOQUE_V3.py --empresa autoequip
"""

import os
import sys
import time
import argparse
from datetime import datetime, timedelta
import requests
import pandas as pd
from sqlalchemy import create_engine, text

# ─── Config ───────────────────────────────────────────────
BLING_API_BASE = "https://api.bling.com.br/Api/v3"
BLING_TOKEN_URL = "https://api.bling.com.br/Api/v3/oauth/token"
PAGE_SIZE = 100
DELAY_MS = 400  # Respeita o rate limit do Bling (máx 3 requisições por segundo)

def get_central_engine():
  """Retorna a engine de conexão para o banco de dados central 'bling'."""
  host = os.getenv("AUTOEQUIP_HOST", "37.60.236.200")
  port = os.getenv("AUTOEQUIP_PORT", 5432)
  user = os.getenv("AUTOEQUIP_USER", "postgres")
  password = os.getenv("AUTOEQUIP_PASSWORD", "131105Gv")
  url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/bling"
  return create_engine(url, connect_args={"options": "-c client_encoding=utf8"}, pool_pre_ping=True)

def get_company_engine(empresa):
  """Retorna a engine de conexão para o banco de dados da empresa."""
  host = os.getenv("AUTOEQUIP_HOST", "37.60.236.200")
  port = os.getenv("AUTOEQUIP_PORT", 5432)
  user = os.getenv("AUTOEQUIP_USER", "postgres")
  password = os.getenv("AUTOEQUIP_PASSWORD", "131105Gv")
  
  # Mapeia a empresa para o nome correto do banco
  db_name = empresa.lower()
  if db_name == 'lanzi':
    db_name = 'Lanzi'
  elif db_name == 'supershop':
    db_name = 'Supershop'
  elif db_name == 'marcon':
    db_name = 'Marcon'
  elif db_name == 'autoequip':
    db_name = 'Autoequip'
  elif db_name == 'amjls':
    db_name = 'amjls'
  
  url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db_name}"
  return create_engine(url, connect_args={"options": "-c client_encoding=utf8"}, pool_pre_ping=True)

def list_clients(central_engine, empresa):
  """Lista todos os clientes Bling cadastrados para uma empresa no banco central."""
  with central_engine.connect() as conn:
    res = conn.execute(
      text("SELECT nome, expires_at, last_sync FROM clientes WHERE empresa = :empresa"),
      {"empresa": empresa.lower()}
    )
    return res.fetchall()

def get_client_data(central_engine, empresa, account_name):
  """Busca credenciais e tokens de um cliente Bling no banco central."""
  with central_engine.connect() as conn:
    res = conn.execute(
      text("SELECT * FROM clientes WHERE empresa = :empresa AND nome = :nome"),
      {"empresa": empresa.lower(), "nome": account_name}
    )
    row = res.fetchone()
    if not row:
      return None
    return dict(row._mapping)

def refresh_token(central_engine, client_data):
  """Renova o access_token utilizando o refresh_token rotativo do Bling."""
  account_name = client_data['nome']
  refresh = client_data['refresh_token']
  client_id = (client_data['client_id'] or '7d24d3e4ab13c4e803b0441f52170ddc261395b7').strip()
  client_secret = (client_data['client_secret'] or 'ebdf4f1c63020852537cef1e4bdd117175fe104b72a3ed3d9ac7aa66bb83').strip()

  if not refresh:
    raise RuntimeError(f"Refresh token nao encontrado para a conta: {account_name}")

  print(f"[INFO] Renovando token para {account_name}...")
  
  import base64
  credentials = f"{client_id}:{client_secret}".encode('utf-8')
  base64_creds = base64.b64encode(credentials).decode('utf-8')
  headers = {
    'Authorization': f'Basic {base64_creds}',
    'Content-Type': 'application/x-www-form-urlencoded',
    'Accept': '1.0'
  }
  payload = {
    'grant_type': 'refresh_token',
    'refresh_token': refresh
  }
  
  r = requests.post(BLING_TOKEN_URL, data=payload, headers=headers, timeout=15)
  if not r.ok:
    raise RuntimeError(f"Falha ao renovar token Bling ({r.status_code}): {r.text}")

  data = r.json()
  access_token = data['access_token']
  new_refresh = data.get('refresh_token', refresh)
  expires_in = data.get('expires_in', 1800)
  expires_at = (datetime.utcnow() + timedelta(seconds=expires_in)).isoformat()

  # Salva os novos tokens atualizados no banco central 'bling'
  with central_engine.begin() as conn:
    conn.execute(
      text("""
        UPDATE clientes SET 
          access_token = :access,
          refresh_token = :refresh,
          expires_at = :exp,
          atualizado_em = NOW()
        WHERE id = :id
      """),
      {
        "access": access_token,
        "refresh": new_refresh,
        "exp": expires_at,
        "id": client_data['id']
      }
    )
  print(f"[OK] Token renovado e atualizado no banco central para {account_name}.")
  return access_token

def fetch_all_produtos(token):
  """Busca cadastro de produtos Bling para mapear IDs para SKUs (Codigos) e Nomes."""
  produtos_map = {}
  pagina = 1
  while True:
    headers = {'Authorization': f'Bearer {token}'}
    url = f"{BLING_API_BASE}/produtos"
    params = {'pagina': pagina, 'limite': PAGE_SIZE}
    
    r = requests.get(url, params=params, headers=headers, timeout=15)
    if r.status_code == 429:
      print("[AVISO] Rate limit atingido em /produtos. Aguardando 5 segundos...")
      time.sleep(5)
      continue
    r.raise_for_status()
    
    data = r.json().get('data', [])
    if not data:
      break
      
    for item in data:
      produtos_map[item['id']] = {
        'sku': item.get('codigo', 'Sem SKU'),
        'nome': item.get('nome', 'Sem Nome')
      }
      
    if len(data) < PAGE_SIZE:
      break
    pagina += 1
    time.sleep(DELAY_MS / 1000.0)
    
  return produtos_map

def fetch_all_estoque(token, produtos_map):
  """Busca saldos de estoque e mescla com os SKUs correspondentes."""
  rows = []
  pagina = 1
  while True:
    headers = {'Authorization': f'Bearer {token}'}
    url = f"{BLING_API_BASE}/estoques/saldos"
    params = {'pagina': pagina, 'limite': PAGE_SIZE}
    
    r = requests.get(url, params=params, headers=headers, timeout=15)
    if r.status_code == 429:
      print("[AVISO] Rate limit atingido em /estoques/saldos. Aguardando 5 segundos...")
      time.sleep(5)
      continue
    r.raise_for_status()
    
    data = r.json().get('data', [])
    if not data:
      break
      
    for item in data:
      prod_id = item['produto']['id']
      p_info = produtos_map.get(prod_id, {'sku': 'Desconhecido', 'nome': 'Produto nao mapeado'})
      
      rows.append({
        'id_bling': str(prod_id),
        'sku': p_info['sku'],
        'nome': p_info['nome'],
        'estoque_atual': float(item.get('saldoFisicoTotal', 0)),
        'estoque_virtual': float(item.get('saldoVirtualTotal', 0))
      })
      
    if len(data) < PAGE_SIZE:
      break
    pagina += 1
    time.sleep(DELAY_MS / 1000.0)
    
  return rows

def salvar_estoque(company_engine, account, rows):
  """Salva a lista de estoque no banco especifico da empresa."""
  safe_name = ''.join(c if c.isalnum() or c == '_' else '_' for c in account).lower()
  table_name = f"bd_estoque_bling_{safe_name}"
  
  ddl = f"""
  CREATE TABLE IF NOT EXISTS {table_name} (
    id_bling TEXT PRIMARY KEY,
    sku TEXT,
    nome TEXT,
    estoque_atual NUMERIC DEFAULT 0,
    estoque_virtual NUMERIC DEFAULT 0,
    atualizado_em TIMESTAMP DEFAULT NOW()
  );
  """
  
  with company_engine.begin() as conn:
    conn.execute(text(ddl))
    
    # Executa o upsert em lote
    conn.execute(
      text(f"""
        INSERT INTO {table_name} (id_bling, sku, nome, estoque_atual, estoque_virtual, atualizado_em)
        VALUES (:id_bling, :sku, :nome, :estoque_atual, :estoque_virtual, NOW())
        ON CONFLICT (id_bling) DO UPDATE SET
          sku = EXCLUDED.sku,
          nome = EXCLUDED.nome,
          estoque_atual = EXCLUDED.estoque_atual,
          estoque_virtual = EXCLUDED.estoque_virtual,
          atualizado_em = NOW()
      """),
      rows
    )
  print(f"[OK] {len(rows)} produtos salvos na tabela '{table_name}' no banco da empresa.")

def update_last_sync(central_engine, client_id):
  """Salva a data de ultima sincronizacao bem-sucedida no banco central."""
  with central_engine.begin() as conn:
    conn.execute(
      text("UPDATE clientes SET last_sync = NOW() WHERE id = :id"),
      {"id": client_id}
    )

def sync_account(central_engine, company_engine, empresa, account_name):
  """Processo completo de sincronizacao para uma conta especifica."""
  print(f"\n[INFO] Iniciando sincronizacao: [{account_name}] para a empresa [{empresa}]")
  
  client_data = get_client_data(central_engine, empresa, account_name)
  if not client_data:
    print(f"[ERRO] Conta '{account_name}' nao encontrada no banco central para a empresa '{empresa}'.")
    return False

  # Verifica e renova token se expirado
  access_token = client_data.get('access_token')
  expires_at_str = client_data.get('expires_at')
  
  needs_refresh = False
  if not access_token or not expires_at_str:
    needs_refresh = True
  else:
    try:
      exp_dt = datetime.fromisoformat(str(expires_at_str).replace('Z', '+00:00'))
      if datetime.now(exp_dt.tzinfo) >= exp_dt - timedelta(minutes=2):
        needs_refresh = True
    except Exception as e:
      needs_refresh = True

  if needs_refresh:
    try:
      access_token = refresh_token(central_engine, client_data)
    except Exception as err:
      print(f"[ERRO] Falha ao renovar token de {account_name}: {err}")
      return False

  # 1. Busca produtos Bling
  print("[INFO] Buscando produtos cadastrados no Bling...")
  produtos_map = fetch_all_produtos(access_token)
  print(f"[INFO] {len(produtos_map)} produtos carregados para mapeamento.")

  # 2. Busca estoque Bling
  print("[INFO] Buscando saldos de estoque do Bling...")
  estoque_rows = fetch_all_estoque(access_token, produtos_map)
  print(f"[INFO] {len(estoque_rows)} registros de saldo de estoque obtidos.")

  if not estoque_rows:
    print("[AVISO] Nenhum registro de estoque para salvar.")
    return False

  # 3. Salva no banco de dados da empresa
  salvar_estoque(company_engine, account_name, estoque_rows)
  
  # 4. Registra ultima sincronizacao no banco central
  update_last_sync(central_engine, client_data['id'])
  print(f"[OK] Sincronizacao de [{account_name}] concluida com sucesso!")
  return True

def main():
  parser = argparse.ArgumentParser(description="ETL de Estoque do Bling V3")
  parser.add_argument('--empresa', default='autoequip', help='Nome da empresa proprietaria (ex: autoequip)')
  parser.add_argument('--account', default=None, help='Nome da conta Bling no banco central (ex: cliente_1). Se omitido, sincroniza todas da empresa.')
  args = parser.parse_args()

  empresa = args.empresa.lower()
  account = args.account

  print("="*60)
  print("S&OP Intelligence - Bling V3 Estoque ETL (Centralizado)")
  print("="*60)

  try:
    central_engine = get_central_engine()
    company_engine = get_company_engine(empresa)
  except Exception as e:
    print(f"[ERRO] Erro ao conectar ao banco de dados: {e}")
    sys.exit(1)

  if account:
    success = sync_account(central_engine, company_engine, empresa, account)
    sys.exit(0 if success else 1)
  else:
    clients = list_clients(central_engine, empresa)
    if not clients:
      print(f"[AVISO] Nenhuma conta Bling V3 cadastrada para a empresa '{empresa}' no banco central.")
      print("Cadastre as contas na tabela 'clientes' do banco de dados 'bling'.")
      sys.exit(0)

    print(f"[INFO] Encontrado(s) {len(clients)} cliente(s) para sincronizar.")
    success_count = 0
    for row in clients:
      acc_name = row[0]
      if sync_account(central_engine, company_engine, empresa, acc_name):
        success_count += 1
      time.sleep(1)
      
    print("\n" + "="*60)
    print(f"Resumo Final: Sincronizadas {success_count} de {len(clients)} contas.")
    print("="*60)

if __name__ == '__main__':
  main()
