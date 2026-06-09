import psycopg2
import requests
import base64
import urllib.parse
from datetime import datetime, timedelta

# Configurações do Banco de Dados
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

def main():
    print("="*60)
    print("   ASSISTENTE DE AUTORIZAÇÃO - API BLING V3 (Autoequip)   ")
    print("="*60)
    print("Este script vai configurar o seu aplicativo Bling no banco de dados.")
    
    client_id = input("\n1. Qual é o seu Client ID? (ex: 7d24d3...): ").strip()
    client_secret = input("2. Qual é o seu Client Secret? (ex: ebdf4f...): ").strip()
    
    print("\nPara a API do Bling V3, você precisa autorizar o app e obter o 'code'.")
    redirect_uri = input("3. Qual Redirect URI você cadastrou no Bling? (ex: https://google.com): ").strip()
    
    auth_url = f"https://www.bling.com.br/Api/v3/oauth/authorize?response_type=code&client_id={client_id}&state=12345"
    print("\n---------------------------------------------------------")
    print(" PASSO 1: Copie e cole a URL abaixo no seu navegador:")
    print(f" -> {auth_url}")
    print(" PASSO 2: Faça login no Bling e clique em 'Autorizar'.")
    print(f" PASSO 3: Você será redirecionado para o seu redirect_uri ({redirect_uri}).")
    print(" PASSO 4: Na barra de endereços, copie APENAS a parte depois de 'code='.")
    print("---------------------------------------------------------")
    
    code = input("\nCole o 'code' que você copiou da URL: ").strip()
    
    if not code:
        print("Erro: Nenhum código fornecido. Abortando.")
        return
        
    print("\nTrocando o 'code' pelos tokens definitivos...")
    
    credentials = f"{client_id}:{client_secret}"
    b64_credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')
    
    headers = {
        'Authorization': f'Basic {b64_credentials}',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Accept': '1.0'
    }
    
    data = {
        'grant_type': 'authorization_code',
        'code': code
    }
    
    try:
        response = requests.post('https://www.bling.com.br/Api/v3/oauth/token', headers=headers, data=data)
        
        if response.status_code == 200:
            token_data = response.json()
            access_token = token_data['access_token']
            refresh_token = token_data['refresh_token']
            expires_in = token_data['expires_in'] # Geralmente 28800 (8 horas)
            
            expires_at = datetime.now() + timedelta(seconds=expires_in)
            
            # Salvando no banco de dados
            conn = get_db_connection()
            cur = conn.cursor()
            
            # Atualizamos o cliente 1 (Autoequip) ou inserimos um novo se não existir
            cur.execute("""
                UPDATE clientes 
                SET client_id = %s, 
                    client_secret = %s, 
                    access_token = %s, 
                    refresh_token = %s, 
                    expires_at = %s, 
                    atualizado_em = NOW()
                WHERE id = 1
            """, (client_id, client_secret, access_token, refresh_token, expires_at))
            
            conn.commit()
            cur.close()
            conn.close()
            
            print("\n✅ SUCESSO! Tokens gerados e salvos no banco de dados (ID 1).")
            print("Você já pode rodar o extrator de estoque: BLING_ESTOQUE_API_ETL.py")
        else:
            print(f"\n❌ ERRO da API: {response.status_code} - {response.text}")
            print("O código pode ter expirado (ele dura só 1 minuto). Tente gerar um novo código.")
            
    except Exception as e:
        print(f"\n❌ Erro na requisição: {e}")

if __name__ == "__main__":
    main()
