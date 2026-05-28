"""Testa endpoints candidatos de export no Preço Certo."""
import requests, json

# Pegar auth do ETL
import sys, os
sys.path.insert(0, os.path.dirname(__file__))

PC_URL   = "https://sys.precocerto.co"
PC_EMAIL = "comercial@casaeletromarcon.com.br"
PC_SENHA = "eletro123"

# Login via requests para pegar sessionid
s = requests.Session()
s.get(f"{PC_URL}/login/")
csrf = s.cookies.get("csrftoken", "")
r = s.post(f"{PC_URL}/login/", data={
    "username_login": PC_EMAIL,
    "password_login": PC_SENHA,
    "csrfmiddlewaretoken": csrf,
}, headers={"Referer": f"{PC_URL}/login/"}, allow_redirects=True)
print(f"Login: {r.status_code} | URL: {r.url}")

sessionid = s.cookies.get("sessionid", "")
csrf2 = s.cookies.get("csrftoken", csrf)
print(f"sessionid: {sessionid[:20]}... | csrf: {csrf2[:20]}...")

headers = {
    "Cookie": f"sessionid={sessionid}; csrftoken={csrf2}",
    "x-csrftoken": csrf2,
    "Referer": f"{PC_URL}/v2/orders/",
}

params = {
    "source_created": "01/02/2025 - 28/02/2025",
    "date_after": "2025-02-01",
    "date_before": "2025-02-28",
}

# Endpoints candidatos
candidates = [
    ("GET",  "/api/order/export-by-product/"),
    ("GET",  "/api/order/export/"),
    ("GET",  "/api/orderline/export/"),
    ("GET",  "/gerenciar/pedidos-de-venda/exportar/"),
    ("POST", "/api/order/export-by-product/"),
    ("POST", "/api/order/export/"),
    ("GET",  "/api/order/sheet/"),
    ("GET",  "/api/order/download/"),
]

for method, path in candidates:
    url = f"{PC_URL}{path}"
    try:
        if method == "GET":
            resp = requests.get(url, params=params, headers=headers, timeout=10, allow_redirects=False)
        else:
            resp = requests.post(url, json=params, headers=headers, timeout=10, allow_redirects=False)
        ct = resp.headers.get("content-type", "")
        print(f"{method} {path} -> {resp.status_code} | {ct[:60]}")
        if resp.status_code not in (404, 403, 302) or "sheet" in ct or "excel" in ct or "json" in ct:
            print(f"  >>> CANDIDATO! Body[:200]: {resp.text[:200]}")
    except Exception as e:
        print(f"{method} {path} -> ERRO: {e}")
