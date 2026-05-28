"""
Tenta autenticação com token como cookie (Nuxt SSR mode)
e examina outros métodos de auth para contornar o 500
"""
import requests
import json
import urllib.parse

BASE = "https://sys.precocerto.co"
EMAIL = "comercial@casaeletromarcon.com.br"
SENHA = "eletro123"

# Obter JWT
r_jwt = requests.post(BASE + "/api/token/", json={"username": EMAIL, "password": SENHA}, timeout=10)
tokens = r_jwt.json()
access = tokens["access"]
refresh = tokens["refresh"]
bearer = f"Bearer {access}"
encoded = urllib.parse.quote(bearer)

print(f"Token obtido: {access[:40]}...\n")

# Tentar cookie auth (como o Nuxt SSR configuraria)
print("=== Tentando cookie auth ===")
cookie_names = [
    "auth._token.local",
    "auth.token",
    "_auth_token",
    "token",
    "jwt",
    "access_token",
    "authorization",
    "auth._token.jwt",
    "auth",
]

for cname in cookie_names:
    for value in [bearer, access, encoded]:
        cookies = {cname: value}
        r = requests.get(BASE + "/api/order",
                         cookies=cookies,
                         headers={"Accept": "application/json",
                                  "Referer": BASE + "/gerenciar/pedidos-de-venda/"},
                         timeout=10)
        ct = r.headers.get("Content-Type", "")
        if r.status_code == 200 and "json" in ct:
            print(f"✅ Cookie {cname}={value[:30]}: {r.text[:400]}")
            break
        elif r.status_code != 500:
            print(f"  Cookie {cname}: {r.status_code}")

# Tentar com AMBOS cookie + Authorization header
print("\n=== Bearer header + cookie combinado ===")
for cname in cookie_names:
    r = requests.get(BASE + "/api/order",
                     cookies={cname: bearer},
                     headers={"Authorization": f"Bearer {access}",
                               "Accept": "application/json"},
                     timeout=10)
    if r.status_code == 200:
        print(f"✅ {cname}: {r.text[:200]}")

# Ver o que o site retorna no login com Nuxt (tentando via /auth)
print("\n=== Tentando login Nuxt auth (/auth/local) ===")
for ep in ["/auth/local", "/auth/token", "/auth/login", "/nuxt/auth"]:
    r = requests.post(BASE + ep, json={"username": EMAIL, "password": SENHA}, timeout=10)
    if r.status_code not in (404, 405):
        print(f"  {ep} → {r.status_code}: {r.text[:200]}")

# Ver se a página de login define cookies especiais
print("\n=== Cookies do login ===")
sess = requests.Session()
sess.headers.update({"User-Agent": "Mozilla/5.0"})
r_home = sess.get(BASE + "/", timeout=10)
r_login = sess.get(BASE + "/gerenciar/pedidos-de-venda/", timeout=10)
print(f"Cookies após navegar: {dict(sess.cookies)}")

# Tentar via Bearer token mas com cookie csrf
sess.cookies.set("auth._token.local", urllib.parse.quote(bearer))
r = sess.get(BASE + "/api/order",
             headers={"Authorization": f"Bearer {access}", "Accept": "application/json"},
             timeout=10)
print(f"Com cookie auth._token.local: {r.status_code}")

# Tentar acesso ao endpoint de companies via Nuxt proxy
print("\n=== Tentando endpoints internos Nuxt ===")
for ep in [
    "/__nuxt__/",
    "/api/__nuxt__/order",
    "/_api/order",
    "/nuxt/api/order",
]:
    r = requests.get(BASE + ep, headers={"Authorization": f"Bearer {access}"}, timeout=10)
    if r.status_code not in (404, 405):
        print(f"  {ep} → {r.status_code}: {r.text[:100]}")

# Exibir sumário do que sabemos
print("\n" + "="*60)
print("SUMÁRIO DA INVESTIGAÇÃO")
print("="*60)
print("""
✅ CONFIRMADO:
  - Endpoint correto: GET /api/order (data-url da tabela)
  - Parâmetros: source_created, ordering, limit, offset, date_before, date_after
  - Auth funciona (JWT): /api/user/detail retorna 200
  - Outros endpoints simples: /api/product-warehouse, /api/payment-method retornam 200 (total=0)

❌ PROBLEMA:
  - GET /api/order retorna 500 (HTTP 500 → CloudFront serving Nuxt SPA)
  - GET /api/order/facts retorna 500
  - GET /api/company/facts retorna 500
  - Conta tem 0 integrações (Bling, Tiny, etc. = 0)
  - Login via sessão Django falha (credenciais JWT funcionam mas sessão não)

🔍 HIPÓTESES:
  1. A conta tem empresa sem integração configurada → backend crasha ao tentar 
     fazer queryset de orders que requer integration_id
  2. Há um bug no backend para contas sem dados (DivisionByZero, NoneType error)
  3. O usuário vê dados no browser via uma conta diferente (empresa pai/admin)

💡 PRÓXIMOS PASSOS:
  - Verificar com o usuário se há dados visíveis no browser com ESTE login
  - Tentar obter token de sessão Django via API legacy
  - Implementar ETL com retry/fallback para quando o endpoint ficar disponível
""")
