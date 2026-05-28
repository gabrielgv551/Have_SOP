"""Descobre os endpoints da API do Upseller para Supershop."""
import requests, json

EMAIL    = "financeiro@supershop.com.br"
PASSWORD = "1893210aB@"

s = requests.Session()
s.headers.update({
    "User-Agent": "Mozilla/5.0 Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Origin": "https://app.upseller.com",
    "Referer": "https://app.upseller.com/pt/login",
})

# ── 1. Tentar login via API JSON com vários formatos de payload ──────────────
LOGIN_URL = "https://app.upseller.com/api/auth/login"

PAYLOADS = [
    {"email": EMAIL, "password": PASSWORD},
    {"username": EMAIL, "password": PASSWORD},
    {"login": EMAIL, "password": PASSWORD},
    {"email": EMAIL, "senha": PASSWORD},
    {"user": EMAIL, "pass": PASSWORD},
    {"email": EMAIL, "password": PASSWORD, "rememberMe": False},
]

auth_resp = None
for payload in PAYLOADS:
    try:
        r = s.post(LOGIN_URL, json=payload, timeout=10)
        data = r.json()
        code = data.get("code")
        print(f"  payload={list(payload.keys())} → {r.status_code}  code={code}  msg={data.get('msg')}")
        if r.status_code in (200, 201) and code != 2001:
            print("  ✔ Login OK!")
            print(json.dumps(data, indent=2, ensure_ascii=False)[:1500])
            auth_resp = r
            break
    except Exception as e:
        print(f"  Erro: {e}")

if not auth_resp:
    # ── 2. Tentar login como form ────────────────────────────────────────────
    print("\n--- Tentando form login ---")
    FORM_URLS = [
        "https://app.upseller.com/pt/login",
        "https://app.upseller.com/en/login",
    ]
    for url in FORM_URLS:
        try:
            r = s.post(url, data={"email": EMAIL, "password": PASSWORD}, timeout=10,
                       allow_redirects=True)
            print(f"POST {url} → {r.status_code}  url_final={r.url}")
            if r.status_code == 200 and "login" not in r.url:
                print("  ✔ Login por form OK!")
                auth_resp = r
                break
        except Exception as e:
            print(f"  Erro: {e}")

if auth_resp:
    # ── 3. Tentar endpoints de inventário ────────────────────────────────────
    print("\n--- Tentando endpoints de inventário ---")
    INV_URLS = [
        "https://app.upseller.com/api/inventory/list",
        "https://app.upseller.com/api/v1/inventory",
        "https://app.upseller.com/api/inventory",
        "https://app.upseller.com/api/products/inventory",
        "https://app.upseller.com/api/stock",
    ]
    for url in INV_URLS:
        try:
            r = s.get(url, timeout=10, params={"page": 1, "pageSize": 10})
            print(f"GET {url} → {r.status_code}")
            if r.status_code == 200:
                print("  ✔ Endpoint encontrado!")
                print(json.dumps(r.json(), indent=2, ensure_ascii=False)[:1500])
                break
            elif r.status_code not in (404, 405):
                print(f"  Resposta: {r.text[:200]}")
        except Exception as e:
            print(f"  Erro: {e}")
else:
    print("\n[!] Login não funcionou. Verifique credenciais.")
