"""
Explorador da API do sistema precocerto.co
Objetivo: descobrir endpoints e estrutura de dados de pedidos de venda da Marcon
"""
import requests
import json

BASE = "https://sys.precocerto.co"
EMAIL = "comercial@casaeletromarcon.com.br"
SENHA = "eletro123"

session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "pt-BR,pt;q=0.9",
    "Referer": "https://sys.precocerto.co/",
    "Origin": "https://sys.precocerto.co",
})

def tentar_login():
    """Tenta diferentes endpoints de login"""
    endpoints_login = [
        "/api/auth/login/",
        "/api/v1/auth/login/",
        "/api/token/",
        "/api/v1/token/",
        "/auth/login/",
        "/api/users/login/",
        "/api/login/",
        "/api/v1/login/",
        "/api/auth/token/",
    ]
    
    payloads = [
        {"email": EMAIL, "password": SENHA},
        {"username": EMAIL, "password": SENHA},
    ]
    
    for endpoint in endpoints_login:
        for payload in payloads:
            try:
                r = session.post(BASE + endpoint, json=payload, timeout=10)
                print(f"POST {endpoint} [{list(payload.keys())[0]}] → {r.status_code}")
                if r.status_code in (200, 201):
                    print(f"  ✅ SUCESSO! Resposta: {r.text[:300]}")
                    return r
                elif r.status_code == 400:
                    print(f"  ⚠️  400: {r.text[:200]}")
                elif r.status_code not in (404, 405):
                    print(f"  → {r.text[:150]}")
            except Exception as e:
                print(f"  ❌ Erro: {e}")
    return None

def explorar_api():
    """Lista endpoints da API raiz"""
    endpoints = [
        "/api/",
        "/api/v1/",
        "/api/v2/",
        "/gerenciar/api/",
    ]
    for ep in endpoints:
        try:
            r = session.get(BASE + ep, timeout=10)
            print(f"GET {ep} → {r.status_code} | Content-Type: {r.headers.get('Content-Type','')}")
            if r.status_code == 200 and "json" in r.headers.get("Content-Type",""):
                print(f"  {r.text[:400]}")
        except Exception as e:
            print(f"  ❌ {e}")

def buscar_pedidos(token=None):
    """Tenta buscar pedidos-de-venda"""
    headers_extra = {}
    if token:
        headers_extra["Authorization"] = f"Bearer {token}"
        headers_extra["Authorization"] = f"Token {token}"
    
    endpoints_pedidos = [
        "/api/pedidos-de-venda/",
        "/api/v1/pedidos-de-venda/",
        "/api/orders/",
        "/api/v1/orders/",
        "/api/sale-orders/",
        "/api/sales-orders/",
        "/api/v1/sales/",
        "/api/sales/",
    ]
    
    params = {
        "limit": 5,
        "offset": 0,
        "date_after": "2026-05-01",
        "date_before": "2026-05-19",
    }
    
    for ep in endpoints_pedidos:
        try:
            r = session.get(BASE + ep, params=params, headers=headers_extra, timeout=10)
            print(f"GET {ep} → {r.status_code} | Content-Type: {r.headers.get('Content-Type','')}")
            if r.status_code == 200:
                print(f"  ✅ {r.text[:500]}")
            elif r.status_code not in (404,):
                print(f"  → {r.text[:200]}")
        except Exception as e:
            print(f"  ❌ {e}")

if __name__ == "__main__":
    print("=" * 60)
    print("PASSO 1: Explorando raiz da API")
    print("=" * 60)
    explorar_api()
    
    print("\n" + "=" * 60)
    print("PASSO 2: Tentando login")
    print("=" * 60)
    resp_login = tentar_login()
    
    token = None
    if resp_login:
        try:
            data = resp_login.json()
            token = data.get("token") or data.get("access") or data.get("key") or data.get("auth_token")
            print(f"\nToken encontrado: {token}")
            print(f"Cookies após login: {dict(session.cookies)}")
        except:
            pass
    
    print("\n" + "=" * 60)
    print("PASSO 3: Buscando pedidos")
    print("=" * 60)
    buscar_pedidos(token)
    
    print("\n" + "=" * 60)
    print("COOKIES DA SESSÃO")
    print("=" * 60)
    print(dict(session.cookies))
