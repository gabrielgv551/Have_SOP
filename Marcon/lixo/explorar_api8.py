"""
1. Login correto com username_login/password_login
2. Testar purchase-order/orders e endpoints relacionados
3. Testar order/facts com format=json
"""
import requests
import json
import re

BASE = "https://sys.precocerto.co"
EMAIL = "comercial@casaeletromarcon.com.br"
SENHA = "eletro123"

# ========== SESSÃO COM CAMPOS CORRETOS ==========
sess = requests.Session()
sess.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
})

# Obter CSRF
r = sess.get(BASE + "/login/", timeout=10)
csrf_match = re.search(r'name=["\']csrfmiddlewaretoken["\'][^>]+value=["\']([^"\']+)["\']', r.text)
csrf = csrf_match.group(1) if csrf_match else sess.cookies.get("csrftoken", "")
print(f"CSRF: {csrf[:30]}...")

# Login com campos corretos
r_login = sess.post(BASE + "/login/",
    data={
        "username_login": EMAIL,
        "password_login": SENHA,
        "csrfmiddlewaretoken": csrf,
    },
    headers={"Referer": BASE + "/login/", "X-CSRFToken": csrf,
             "Content-Type": "application/x-www-form-urlencoded",
             "Accept": "text/html,application/xhtml+xml,*/*"},
    allow_redirects=True,
    timeout=15)

print(f"Login → {r_login.status_code} | URL: {r_login.url}")
print(f"Cookies: {dict(sess.cookies)}")
if "sessionid" in sess.cookies:
    print("✅ Sessão Django estabelecida!")
    has_session = True
else:
    has_session = False
    print("❌ Sem sessionid ainda")

# ========== JWT ==========
r_jwt = requests.post(BASE + "/api/token/", json={"username": EMAIL, "password": SENHA}, timeout=10)
access = r_jwt.json()["access"]

# Criar sessão que combina cookies de sessão + JWT
api = requests.Session()
api.cookies = sess.cookies.copy()
api.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124",
    "Authorization": f"Bearer {access}",
    "Accept": "application/json",
    "Referer": BASE + "/gerenciar/pedidos-de-venda/",
})

# ========== TESTAR purchase-order/orders ==========
print("\n=== Testando purchase-order/* ===")
for ep in [
    "/api/purchase-order/orders",
    "/api/purchase-order/lines",
    "/api/purchase-order/stats",
]:
    r = api.get(BASE + ep, timeout=15)
    ct = r.headers.get("Content-Type", "")
    if r.status_code == 200 and "json" in ct:
        d = r.json()
        print(f"✅ {ep} → 200")
        print(f"  Keys: {list(d.keys()) if isinstance(d, dict) else 'lista'}")
        if isinstance(d, dict) and "rows" in d:
            print(f"  total={d.get('total', '?')}, rows={len(d['rows'])} items")
            if d['rows']:
                print(f"  Primeiro row keys: {list(d['rows'][0].keys())[:15]}")
        elif isinstance(d, list):
            print(f"  {len(d)} items, primeiro: {d[0] if d else 'vazio'}")
        else:
            print(f"  {json.dumps(d, ensure_ascii=False)[:400]}")
    else:
        print(f"  {ep} → {r.status_code} | {ct[:50]}")

# ========== order/facts com format=json ==========
print("\n=== order/facts com format=json ===")
r = api.get(BASE + "/api/order/facts", params={"format": "json"}, timeout=15)
print(f"Status: {r.status_code} | CT: {r.headers.get('Content-Type', '')[:50]}")
if r.status_code == 200:
    print(r.text[:500])

# ========== Testar todos os endpoints do API root que não são 404/500 ==========
print("\n=== Testando todos endpoints da API root ===")
r_root = api.get(BASE + "/api/", timeout=10)
all_endpoints = r_root.json()

working = []
failed = []
for key, url in all_endpoints.items():
    r = api.get(url, timeout=10)
    ct = r.headers.get("Content-Type", "")
    if r.status_code == 200 and "json" in ct:
        d = r.json()
        total = d.get("total", d.get("count", "?")) if isinstance(d, dict) else "list"
        working.append((key, url, total))
    elif r.status_code == 200:
        working.append((key, url, "200-html"))
    else:
        failed.append((key, r.status_code))

print(f"\n✅ Endpoints que funcionaram ({len(working)}):")
for k, u, t in working:
    print(f"  {k}: total={t}")

print(f"\n❌ Endpoints com erro ({len(failed)}):")
for k, s in failed:
    print(f"  {k}: {s}")
