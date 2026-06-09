import os
import csv
import time
import requests
import psycopg2
import base64
from datetime import datetime

# =========================================================================
# CONFIGURAÇÕES
# =========================================================================
SALVAR_EM = os.getcwd()

# Banco Central (Onde guardamos os Tokens do Bling)
DB_HOST = "37.60.236.200"
DB_NAME = "bling"
DB_USER = "postgres"
DB_PASS = "131105Gv"
DB_PORT = 5432

def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        port=DB_PORT
    )

def rotate_token(client_id, client_secret, refresh_token):
    """Troca o Refresh Token atual por um novo Access Token e atualiza no banco."""
    print("      [+] Rotacionando token OAuth no Bling...")
    credentials = f"{client_id}:{client_secret}"
    b64_credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')
    
    headers = {
        'Authorization': f'Basic {b64_credentials}',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Accept': '1.0'
    }
    
    data = {
        'grant_type': 'refresh_token',
        'refresh_token': refresh_token
    }
    
    response = requests.post('https://www.bling.com.br/Api/v3/oauth/token', headers=headers, data=data)
    
    if response.status_code == 200:
        token_data = response.json()
        new_access = token_data['access_token']
        new_refresh = token_data['refresh_token']
        
        # Atualiza o banco
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            UPDATE clientes 
            SET access_token = %s, refresh_token = %s, atualizado_em = NOW()
            WHERE client_id = %s
        """, (new_access, new_refresh, client_id))
        conn.commit()
        cur.close()
        conn.close()
        
        print("      [+] Token atualizado com sucesso no banco de dados!")
        return new_access
    else:
        raise Exception(f"Erro ao rotacionar token: {response.status_code} - {response.text}")


def fetch_produtos_estoque(access_token):
    """Busca produtos na API V3 do Bling. Aqui você pode adaptar se quiser pegar /estoques/saldos"""
    produtos = []
    pagina = 1
    limite = 100
    
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Accept': 'application/json'
    }
    
    print("      [+] Iniciando extração via API V3...")
    while True:
        # Busca produtos (pode alterar para /estoques/saldos conforme necessidade)
        url = f"https://www.bling.com.br/Api/v3/produtos?pagina={pagina}&limite={limite}"
        
        response = requests.get(url, headers=headers)
        
        if response.status_code == 429:
            print("      [!] Rate Limit atingido. Aguardando 1 segundo...")
            time.sleep(1)
            continue
            
        if response.status_code != 200:
            print(f"      [!] Erro na API: {response.status_code} - {response.text}")
            break
            
        data = response.json().get('data', [])
        if not data:
            break
            
        for p in data:
            # O Bling V3 retorna estoque no objeto /produtos? Você pode precisar bater em /estoques/saldos
            produtos.append({
                'id': p.get('id'),
                'codigo': p.get('codigo', ''),
                'nome': p.get('nome', ''),
                'preco': p.get('preco', 0),
                'situacao': p.get('situacao', '')
                # Para saldo físico exato, seria necessário fazer GET /estoques/saldos
            })
            
        print(f"      [+] Página {pagina} processada. Total até agora: {len(produtos)}")
        pagina += 1
        time.sleep(0.35) # Respeitar rate limit de 3 req/segundo
        
    return produtos


def export_to_csv(data):
    if not data:
        print("      [!] Nenhum dado para exportar.")
        return
        
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    filepath = os.path.join(SALVAR_EM, f"bling_estoque_api_{ts}.csv")
    
    keys = data[0].keys()
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        dict_writer = csv.DictWriter(f, fieldnames=keys, delimiter=';')
        dict_writer.writeheader()
        dict_writer.writerows(data)
        
    size_kb = os.path.getsize(filepath) / 1024
    print(f"\n✅ Arquivo de estoque API salvo: {filepath}")
    print(f"   Tamanho: {size_kb:.1f} KB")


def main():
    print("[1/3] Conectando ao Banco Central [bling] para buscar credenciais...")
    conn = get_db_connection()
    cur = conn.cursor()
    # Busca o cliente de ID 1 (Autoequip) ou mude o filtro para WHERE empresa = 'autoequip'
    cur.execute("SELECT client_id, client_secret, refresh_token FROM clientes WHERE id = 1;")
    row = cur.fetchone()
    cur.close()
    conn.close()
    
    if not row or not row[0] or not row[2]:
        print("\n❌ ERRO: Credenciais ou refresh_token não encontrados no banco de dados!")
        print("Por favor, rode o script 'BLING_AUTH_HELPER.py' primeiro para autorizar a API.")
        return
        
    client_id, client_secret, refresh_token = row
    
    print("[2/3] Autenticando com a API V3 do Bling...")
    try:
        access_token = rotate_token(client_id, client_secret, refresh_token)
    except Exception as e:
        print(f"\n❌ ERRO ao Autenticar: {e}")
        return

    print("[3/3] Baixando dados de produtos...")
    produtos = fetch_produtos_estoque(access_token)
    
    print("[FINAL] Exportando para CSV...")
    export_to_csv(produtos)

if __name__ == "__main__":
    main()
