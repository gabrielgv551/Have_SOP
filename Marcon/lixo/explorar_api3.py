"""
Testa order/facts com parâmetros exatos do frontend e verifica estrutura
"""
import requests
import json

BASE = "https://sys.precocerto.co"
EMAIL = "comercial@casaeletromarcon.com.br"
SENHA = "eletro123"

session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "pt-BR,pt;q=0.9",
    "Origin": "https://sys.precocerto.co",
    "Referer": "https://sys.precocerto.co/gerenciar/pedidos-de-venda/",
})

# Login
r = session.post(BASE + "/api/token/", json={"username": EMAIL, "password": SENHA}, timeout=10)
access = r.json()["access"]
session.headers["Authorization"] = f"Bearer {access}"
print(f"✅ Autenticado\n")

# Testar order/facts com parâmetros do frontend
print("=== Testando /api/order/facts com parâmetros do frontend ===")
combinacoes = [
    # Exatamente como a URL do front
    {"source_created": "04/05/2026 - 19/05/2026", "date_before": "2026-05-19", "date_after": "2026-05-04", "ordering": "-source_created"},
    # Sem filtros de data
    {"ordering": "-source_created", "limit": 10},
    # Só limit
    {"limit": 10},
    # Sem nada
    {},
    # Com format=json
    {"format": "json", "limit": 10},
    # Com page
    {"page": 1, "limit": 10},
    # com page_size
    {"page_size": 10, "page": 1},
    # Com offset
    {"limit": 5, "offset": 0, "source_created": "04/05/2026 - 19/05/2026"},
]

for params in combinacoes:
    r = session.get(BASE + "/api/order/facts", params=params, timeout=15)
    ct = r.headers.get("Content-Type", "")
    print(f"\nParams: {params}")
    print(f"Status: {r.status_code} | CT: {ct[:50]}")
    if r.status_code == 200:
        try:
            data = r.json()
            print(f"✅ SUCESSO! Keys: {list(data.keys()) if isinstance(data, dict) else 'lista'}")
            if isinstance(data, dict):
                print(f"   count={data.get('count')}, results={len(data.get('results',[]))}")
                if data.get("results"):
                    print(f"   Primeiro item keys: {list(data['results'][0].keys())}")
                    print(f"   Primeiro item:\n{json.dumps(data['results'][0], indent=2, ensure_ascii=False)[:800]}")
        except:
            print(f"   Resposta: {r.text[:400]}")
        break
    elif r.status_code != 500:
        print(f"   {r.text[:200]}")
    else:
        # 500 — mostrar mais detalhe
        if "json" in ct:
            print(f"   500 JSON: {r.text[:200]}")

# Testar também com trailing slash
print("\n=== Com trailing slash /api/order/facts/ ===")
r = session.get(BASE + "/api/order/facts/", params={"source_created": "04/05/2026 - 19/05/2026", "limit": 5}, timeout=15)
print(f"Status: {r.status_code}")
if r.status_code == 200:
    print(r.text[:600])
elif r.status_code != 500:
    print(r.text[:200])

# Verificar company/facts - pode ter info útil
print("\n=== Testando /api/company/facts ===")
r = session.get(BASE + "/api/company/facts", timeout=10)
print(f"Status: {r.status_code}")
if r.status_code == 200:
    print(r.text[:800])

# Verificar dashboard/filters - pode mostrar o nome do endpoint certo
print("\n=== Testando /api/dashboard/filters ===")
r = session.get(BASE + "/api/dashboard/filters", timeout=10)
print(f"Status: {r.status_code}")
if r.status_code == 200:
    print(r.text[:800])

# Verificar channel 
print("\n=== Testando /api/channel com parâmetros ===")
for params in [{"limit": 5}, {}]:
    r = session.get(BASE + "/api/channel", params=params, timeout=10)
    print(f"Params {params} → Status: {r.status_code}")
    if r.status_code == 200:
        print(r.text[:400])
        break
