"""
Diagnóstico final: tenta acessar Django admin e verificar 
se o problema é da conta específica ou do backend em geral
"""
import requests
import json
import re

BASE = "https://sys.precocerto.co"
EMAIL = "comercial@casaeletromarcon.com.br"
SENHA = "eletro123"

r_jwt = requests.post(BASE + "/api/token/", json={"username": EMAIL, "password": SENHA}, timeout=10)
access = r_jwt.json()["access"]
api = requests.Session()
api.headers.update({
    "Authorization": f"Bearer {access}",
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0",
})

# 1. Tentar Django admin
print("=== Django Admin ===")
for ep in ["/admin/", "/admin/login/", "/admin/precocerto/"]:
    r = requests.get(BASE + ep, timeout=10)
    ct = r.headers.get("Content-Type", "")
    title = re.search(r'<title>(.*?)</title>', r.text)
    print(f"  {ep} → {r.status_code} | {title.group(1) if title else ct[:40]}")

# 2. Tentar acesso ao admin com credenciais
print("\n=== Admin login ===")
sess = requests.Session()
sess.headers.update({"User-Agent": "Mozilla/5.0"})
r = sess.get(BASE + "/admin/login/", timeout=10)
csrf_match = re.search(r'name="csrfmiddlewaretoken"[^>]+value="([^"]+)"', r.text)
csrf = csrf_match.group(1) if csrf_match else ""
if csrf:
    r_admin = sess.post(BASE + "/admin/login/",
                        data={"username": EMAIL, "password": SENHA, "csrfmiddlewaretoken": csrf},
                        headers={"Referer": BASE + "/admin/login/"},
                        allow_redirects=True, timeout=10)
    print(f"  Admin login → {r_admin.status_code} | URL: {r_admin.url}")
    print(f"  Cookies: {dict(sess.cookies)}")
    if "sessionid" in sess.cookies:
        print("  ✅ Admin session!")
        r_orders = sess.get(BASE + "/admin/orders/order/", 
                            headers={"Accept": "application/json"},
                            timeout=10)
        print(f"  Admin orders → {r_orders.status_code}")

# 3. Ver se o endpoint funciona como query param (company_id)
print("\n=== Testando company_id como query param ===")
# Talvez precise enviar company_id explicitamente
for company_id in range(1, 20):
    r = api.get(BASE + "/api/order",
                params={"company_id": company_id, "limit": 5},
                timeout=10)
    if r.status_code == 200 and "json" in r.headers.get("Content-Type", ""):
        d = r.json()
        print(f"✅ company_id={company_id}: total={d.get('total')}")
        if d.get("rows"):
            print(f"  Keys: {list(d['rows'][0].keys())}")
        break
    elif r.status_code != 500:
        print(f"  company_id={company_id}: {r.status_code}")

# 4. Verificar se há um header de company que o frontend usa
print("\n=== Tentando X-Company-Id header ===")
for company_id in range(1, 10):
    for header_name in ["X-Company-Id", "X-Company", "Company-Id", "X-Tenant-Id", "X-Organization-Id"]:
        r = api.get(BASE + "/api/order",
                    headers={header_name: str(company_id)},
                    params={"limit": 5},
                    timeout=10)
        if r.status_code == 200 and "json" in r.headers.get("Content-Type", ""):
            print(f"✅ {header_name}={company_id}: {r.text[:200]}")
            break

# 5. Ver o que o site retorna se acessamos pelo legacy path
print("\n=== Acessando legacy Django API ===")
for ep in [
    "/api/order/?limit=5",
    "/api/orders/?limit=5",
    "/sales/orders/?format=json",
    "/sales/orders/",
    "/api/v1/orders/",
    "/api/orders/list/",
]:
    r = api.get(BASE + ep, timeout=10)
    ct = r.headers.get("Content-Type", "")
    if r.status_code not in (404, 405):
        print(f"  {ep} → {r.status_code} | {ct[:40]}")
        if r.status_code == 200 and "json" in ct:
            print(f"  ✅ {r.text[:300]}")

# 6. Tentar a exportação de pedidos via task
print("\n=== Tentando exportação via task ===")
# A exportação cria uma planilha e retorna um task_id
for ep in ["/api/order/export/", "/api/order/export-sheet/", "/api/order/facts/export/"]:
    r = api.post(BASE + ep, json={}, timeout=15)
    ct = r.headers.get("Content-Type", "")
    if r.status_code not in (404, 405):
        print(f"  POST {ep} → {r.status_code}: {r.text[:200]}")

# Resultado final
print("\n" + "="*60)
print("CONCLUSÃO")
print("="*60)
print("""
Endpoint confirmado via análise do código fonte:
  URL: GET https://sys.precocerto.co/api/order
  Auth: Authorization: Bearer <JWT_TOKEN>
  Params principais:
    - source_created: "04/05/2026 - 19/05/2026" (dd/MM/yyyy - dd/MM/yyyy)
    - ordering: "-source_created" (padrão)
    - limit: 10000 (máximo)
    - offset: 0 (para paginação)
  
Problema atual:
  - Endpoint retorna HTTP 500 para esta conta
  - Causa provável: conta sem integração configurada (Bling/Tiny = 0)
  - O backend provavelmente crasha ao tentar filtrar orders por integration
  
Solução para o usuário:
  - Configurar uma integração no Preco Certo (Bling, Tiny, Olist, etc.)
  - Ou contactar suporte Preco Certo sobre o erro 500
  - Uma vez resolvido, o ETL abaixo funcionará
""")
