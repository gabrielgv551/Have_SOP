"""
Testa endpoints de empresa e tenta encontrar company_id associado ao usuário
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
    "Origin": "https://sys.precocerto.co",
    "Referer": "https://sys.precocerto.co/gerenciar/pedidos-de-venda/",
})

r = session.post(BASE + "/api/token/", json={"username": EMAIL, "password": SENHA}, timeout=10)
access = r.json()["access"]
session.headers["Authorization"] = f"Bearer {access}"
print(f"✅ Token JWT\n")

# Tentar endpoints de empresa
print("=== Endpoints de empresa ===")
for ep in [
    "/api/company/", "/api/companies/", "/api/user/company/",
    "/api/account/", "/api/account/company/",
    "/api/company", "/api/companies",
    "/api/me/", "/api/me",
    "/api/user/me/", "/api/profile/",
    "/api/user/profile/", "/api/user/",
]:
    r = session.get(BASE + ep, timeout=10)
    ct = r.headers.get("Content-Type", "")
    if r.status_code == 200:
        print(f"✅ {ep} → 200 | {ct[:40]}")
        if "json" in ct:
            d = r.json()
            print(f"   {json.dumps(d, ensure_ascii=False)[:400]}")
    elif r.status_code not in (404, 405):
        print(f"⚠️ {ep} → {r.status_code}: {r.text[:80]}")

# Ver detalhes do 500 em /api/order/facts (cabeçalho da resposta)
print("\n=== Detalhes do erro 500 em /api/order/facts ===")
r = session.get(BASE + "/api/order/facts", timeout=10)
print(f"Status: {r.status_code}")
print(f"Headers: {dict(r.headers)}")
print(f"Content-Type: {r.headers.get('Content-Type')}")
if "json" in r.headers.get("Content-Type", ""):
    print(f"Body: {r.text[:500]}")

# Tentar login via sessão (forma do browser) em vez de JWT
print("\n=== Tentando login via sessão (cookies) ===")
sess2 = requests.Session()
sess2.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124",
    "Accept": "application/json, text/plain, */*",
    "Origin": "https://sys.precocerto.co",
    "Referer": "https://sys.precocerto.co/",
})

# Primeiro pegar o CSRF do site legado
r_home = sess2.get(BASE + "/login/", timeout=10)
print(f"GET /login/ → {r_home.status_code}")

# Tentar login de sessão Django
r_login = sess2.post(BASE + "/api-auth/login/", 
    data={"username": EMAIL, "password": SENHA}, 
    headers={"Referer": BASE + "/api-auth/login/"},
    timeout=10)
print(f"POST /api-auth/login/ → {r_login.status_code}")

# Tentar login direto no endpoint do site legado
for login_url in ["/login/", "/accounts/login/", "/auth/login"]:
    r_l = sess2.post(BASE + login_url,
        data={"email": EMAIL, "password": SENHA, "username": EMAIL},
        timeout=10, allow_redirects=True)
    if r_l.status_code == 200 and "logout" in r_l.text.lower():
        print(f"✅ Login por sessão em {login_url}!")
        print(f"Cookies: {dict(sess2.cookies)}")
        # Agora testar order/facts com sessão
        r_of = sess2.get(BASE + "/api/order/facts", 
                         params={"source_created": "04/05/2026 - 19/05/2026"}, timeout=15)
        print(f"order/facts com sessão → {r_of.status_code}")
        if r_of.status_code == 200:
            print(r_of.text[:600])
        break
    print(f"{login_url} → {r_l.status_code}")

# Verificar se há endpoint de "active company" no estilo do bundle
print("\n=== Endpoints do Vue store de companies ===")
for ep in [
    "/api/active-company/", 
    "/api/active-company",
    "/api/user/active-company/",
    "/api/companies/active/",
]:
    r = session.get(BASE + ep, timeout=10)
    if r.status_code not in (404, 405):
        print(f"⚠️  {ep} → {r.status_code}: {r.text[:100]}")

# Ver se o 500 do order/facts tem mensagem de erro visível na resposta HTML
print("\n=== Tentando extrair mensagem de erro do 500 ===")
r = session.get(BASE + "/api/order/facts", timeout=10)
# Pegar apenas o <title> e primeiros parágrafos do HTML de erro
html = r.text
import re
title_match = re.search(r'<title>(.*?)</title>', html, re.IGNORECASE)
error_match = re.search(r'<pre[^>]*>(.*?)</pre>', html, re.IGNORECASE | re.DOTALL)
value_error = re.search(r'(ValueError|AttributeError|DoesNotExist|Exception)[^\n]*', html)
if title_match:
    print(f"Title: {title_match.group(1)}")
if error_match:
    print(f"Pre tag: {error_match.group(1)[:300]}")
if value_error:
    print(f"Erro: {value_error.group(0)[:200]}")
print(f"Primeiros 500 chars do corpo: {html[:500]}")
