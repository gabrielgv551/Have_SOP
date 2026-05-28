"""
Analisa o formulário de login e tenta autenticação via sessão Django corretamente
"""
import requests
import json
import re

BASE = "https://sys.precocerto.co"
EMAIL = "comercial@casaeletromarcon.com.br"
SENHA = "eletro123"

sess = requests.Session()
sess.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124",
})

# Ver o formulário de login do site legado Django
print("=== Inspecionando /login/ ===")
r = sess.get(BASE + "/login/", timeout=10)
# Extrair fields do formulário
fields = re.findall(r'<input[^>]+>', r.text)
print("Campos do formulário de login:")
for f in fields[:20]:
    name = re.search(r'name=["\']([^"\']+)["\']', f)
    itype = re.search(r'type=["\']([^"\']+)["\']', f)
    if name:
        print(f"  name={name.group(1)}, type={itype.group(1) if itype else 'text'}")

csrf_in_html = re.search(r'name=["\']csrfmiddlewaretoken["\'][^>]+value=["\']([^"\']+)["\']', r.text)
if csrf_in_html:
    csrf = csrf_in_html.group(1)
    print(f"\nCSRF: {csrf[:20]}...")
elif "csrftoken" in sess.cookies:
    csrf = sess.cookies["csrftoken"]
    print(f"\nCSRF do cookie: {csrf[:20]}...")
else:
    csrf = ""

# Tentar diferentes variações do login via sessão
print("\n=== Tentativas de login ===")
login_variants = [
    ("/login/", {"username": EMAIL, "password": SENHA, "csrfmiddlewaretoken": csrf}),
    ("/login/", {"email": EMAIL, "password": SENHA, "csrfmiddlewaretoken": csrf}),
]
for url, data in login_variants:
    r = sess.post(BASE + url, data=data,
                  headers={"Referer": BASE + url, "X-CSRFToken": csrf},
                  allow_redirects=True, timeout=15)
    print(f"POST {url} ({list(data.keys())}) → {r.status_code} | URL={r.url}")
    print(f"  Cookies: {dict(sess.cookies)}")
    if "sessionid" in sess.cookies:
        print("  ✅ Sessão Django estabelecida!")
        break

# Testar via Nuxt auth route diretamente
print("\n=== Tentando via /api/token/ + active-company da API ===")
r_jwt = requests.post(BASE + "/api/token/", json={"username": EMAIL, "password": SENHA}, timeout=10)
data = r_jwt.json()
access = data["access"]
refresh = data["refresh"]
print(f"Access token: {access[:40]}...")
print(f"Refresh token: {refresh[:40]}...")

# O Nuxt auth-next pode usar o refresh token de forma diferente
# Tentar com o refresh token
sess_jwt = requests.Session()
sess_jwt.headers.update({
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json",
    "Authorization": f"Bearer {access}",
})

# Ver /api/company/facts com Accept header diferente
print("\n=== Tentando Accept: application/json explícito em /api/company/facts ===")
r = sess_jwt.get(BASE + "/api/company/facts", 
                 headers={"Accept": "application/json"},
                 timeout=10)
print(f"Status: {r.status_code}, CT: {r.headers.get('Content-Type')}")
if r.status_code == 200:
    print(r.text[:500])

# Tentar endpoint de exportação CSV
print("\n=== Tentando exportação CSV de pedidos ===")
for ep in [
    "/api/order/facts?format=csv",
    "/api/order/export/?format=csv",
]:
    r = sess_jwt.get(BASE + ep.split("?")[0], params={"format": "csv"}, timeout=15)
    print(f"{ep} → {r.status_code}, CT: {r.headers.get('Content-Type', '')[:50]}")

# Verificar se há um endpoint de companies para pegar o company_id
print("\n=== Endpoints para descobrir company_id ===")
for ep in ["/api/user/detail/", "/api/profile/", "/api/me/"]:
    r = sess_jwt.get(BASE + ep, timeout=10)
    if r.status_code == 200 and "json" in r.headers.get("Content-Type", ""):
        print(f"✅ {ep} → {r.text[:400]}")
    elif r.status_code not in (404, 405):
        print(f"⚠️  {ep} → {r.status_code}")

# Verificar o que /api/ retorna com mais detalhes — há campos de "company"?
print("\n=== Re-verificando /api/ root ===")
r = sess_jwt.get(BASE + "/api/", timeout=10)
if r.status_code == 200:
    data = r.json()
    print("Endpoints com 'company' no nome:")
    for k, v in data.items():
        if "company" in k.lower() or "order" in k.lower() or "sale" in k.lower():
            print(f"  {k}: {v}")

# Tentar acessar o endpoint de companies diretamente (legacy Django)
print("\n=== Tentando /account/user_management/ ===")
r = sess_jwt.get(BASE + "/account/user_management/", 
                 headers={"Accept": "application/json"},
                 timeout=10)
print(f"Status: {r.status_code}")

# Tentar via API v2 que pode ter a listagem de empresas
print("\n=== Testando padrões alternativos de order/facts ===")
for params in [
    {"source_created": "04/05/2026 - 19/05/2026", "page_size": 10},
    {"date_after": "2026-05-04", "date_before": "2026-05-19", "page_size": 10},
    {"ordering": "-source_created", "page_size": 10, "page": 1},
    {"start_date": "2026-05-04", "end_date": "2026-05-19"},
    {"month": "2026-05"},
    {"company_id": "1", "source_created": "04/05/2026 - 19/05/2026"},
]:
    r = sess_jwt.get(BASE + "/api/order/facts", params=params, timeout=10)
    ct = r.headers.get("Content-Type", "")
    if r.status_code == 200 and "json" in ct:
        print(f"✅ Params {params} → 200!")
        print(f"  {r.text[:400]}")
        break
    elif r.status_code != 500:
        print(f"Params {params}: {r.status_code}")
