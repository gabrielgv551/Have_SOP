"""
Tenta login via sessão Django (cookie) para contornar o 500 em /api/order/facts
"""
import requests
import json
import re

BASE = "https://sys.precocerto.co"
EMAIL = "comercial@casaeletromarcon.com.br"
SENHA = "eletro123"

# ===== SESSÃO COOKIE (abordagem browser) =====
sess = requests.Session()
sess.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "pt-BR,pt;q=0.9",
    "Referer": BASE + "/",
})

# 1. Pegar página de login para obter CSRF token
print("=== Obtendo CSRF token ===")
r = sess.get(BASE + "/login/", timeout=10)
print(f"GET /login/ → {r.status_code}")
csrf = None
# Procurar no cookie
if "csrftoken" in sess.cookies:
    csrf = sess.cookies["csrftoken"]
    print(f"CSRF do cookie: {csrf[:20]}...")
# Procurar no HTML
if not csrf:
    match = re.search(r'csrfmiddlewaretoken["\s]+value="([^"]+)"', r.text)
    if match:
        csrf = match.group(1)
        print(f"CSRF do HTML: {csrf[:20]}...")
print(f"Cookies após GET /login/: {dict(sess.cookies)}")

# 2. Fazer POST de login com credenciais + CSRF
print("\n=== POST de login ===")
login_data = {
    "email": EMAIL,
    "password": SENHA,
    "username": EMAIL,
}
if csrf:
    login_data["csrfmiddlewaretoken"] = csrf

r_login = sess.post(BASE + "/login/", 
    data=login_data,
    headers={
        "Content-Type": "application/x-www-form-urlencoded",
        "Referer": BASE + "/login/",
        "X-CSRFToken": csrf or "",
    },
    allow_redirects=True,
    timeout=15)
print(f"POST /login/ → {r_login.status_code}")
print(f"Cookies após login: {dict(sess.cookies)}")
print(f"URL final: {r_login.url}")

# 3. Testar endpoints com sessão + JWT
print("\n=== Testando com sessão apenas ===")
for ep in ["/api/order/facts", "/api/company/facts", "/api/company/"]:
    r = sess.get(BASE + ep, 
                 headers={"Accept": "application/json"},
                 params={"source_created": "04/05/2026 - 19/05/2026"},
                 timeout=15)
    ct = r.headers.get("Content-Type", "")
    print(f"{ep} → {r.status_code} | {ct[:50]}")
    if r.status_code == 200 and "json" in ct:
        print(f"  ✅ {r.text[:400]}")

# 4. Tentar combinar sessão + JWT Bearer
print("\n=== Sessão + JWT combinados ===")
r_jwt = requests.Session()
r_jwt.cookies = sess.cookies.copy()
r_jwt.headers.update(sess.headers)
# Obter JWT
r_token = requests.post(BASE + "/api/token/", json={"username": EMAIL, "password": SENHA}, timeout=10)
access = r_token.json()["access"]
r_jwt.headers["Authorization"] = f"Bearer {access}"
r_jwt.headers["Accept"] = "application/json"

for ep in ["/api/order/facts", "/api/company/facts"]:
    r = r_jwt.get(BASE + ep, 
                  params={"source_created": "04/05/2026 - 19/05/2026"},
                  timeout=15)
    ct = r.headers.get("Content-Type", "")
    print(f"{ep} → {r.status_code} | {ct[:50]}")
    if r.status_code == 200 and "json" in ct:
        print(f"  ✅ {r.text[:600]}")

# 5. Verificar se o endpoint de orders é via GET com filtro de empresa na URL
print("\n=== Busca em empresa específica ===")
for ep in [
    "/api/company/1/orders/",
    "/api/company/1/order/facts/",
    "/api/1/order/facts/",
    "/api/order/facts/?format=json",
]:
    r = requests.get(BASE + ep, 
                     headers={"Authorization": f"Bearer {access}", "Accept": "application/json"},
                     timeout=10)
    if r.status_code not in (404, 405):
        ct = r.headers.get("Content-Type", "")
        print(f"⚠️  {ep} → {r.status_code} | {ct[:40]} | {r.text[:100]}")

# 6. Verificar /api/channel para entender a estrutura
print("\n=== GET /api/channel (sem params) ===")
r = requests.get(BASE + "/api/channel", 
                 headers={"Authorization": f"Bearer {access}", "Accept": "application/json"},
                 timeout=10)
print(f"Status: {r.status_code}")
ct = r.headers.get("Content-Type", "")
print(f"Content-Type: {ct}")
if "json" in ct and r.status_code == 200:
    print(r.text[:600])
