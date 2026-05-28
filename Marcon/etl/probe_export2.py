import requests, json

PC_URL = "https://sys.precocerto.co"
s = requests.Session()
r = s.get(f"{PC_URL}/login/")
csrf = s.cookies.get("csrftoken", "")
print(f"csrf: {csrf[:20]}...")
login_r = s.post(f"{PC_URL}/authenticate_user_ajax/",
       data={"username_login": "comercial@casaeletromarcon.com.br",
             "password_login": "eletro123",
             "csrfmiddlewaretoken": csrf},
       headers={"Referer": f"{PC_URL}/login/", "X-Requested-With": "XMLHttpRequest"})
print(f"Login: {login_r.status_code} | body: {login_r.text[:200]}")
sessionid = s.cookies.get("sessionid", "")
csrf2 = s.cookies.get("csrftoken", csrf)
print(f"sessionid: {sessionid[:20]}... | csrf2: {csrf2[:20]}...")
h = {"Cookie": f"sessionid={sessionid}; csrftoken={csrf2}", "x-csrftoken": csrf2,
     "Accept": "application/json"}

# Testar o endpoint de polling com o job ID conhecido
r = s.get(f"{PC_URL}/api/integration-task/last/21673", headers=h, timeout=10)
print(f"GET /api/integration-task/last/21673 -> {r.status_code}")
print(f"Body: {r.text[:500] if r.text else '(empty)'}")

# Testar o endpoint de integration-task sem ID especifico
r2 = s.get(f"{PC_URL}/api/integration-task/", headers=h, timeout=10)
print(f"\nGET /api/integration-task/ -> {r2.status_code}")
print(f"Body: {r2.text[:500] if r2.text else '(empty)'}")

print()
# Tentar o POST que dispara o export - candidatos
post_candidates = [
    "/api/integration-task/",
    "/api/order/export-sheet/",
    "/api/order/sheet/export/",
    "/api/orderline/export-sheet/",
    "/api/order/export-by-product-sheet/",
]
params = {
    "source_created": "01/02/2025 - 28/02/2025",
    "date_after": "2025-02-01",
    "date_before": "2025-02-28",
    "ordering": "-source_created",
}
for path in post_candidates:
    r2 = s.post(f"{PC_URL}{path}", json=params, headers=h, timeout=10)
    ct = r2.headers.get("content-type", "")
    print(f"POST {path} -> {r2.status_code} | {ct[:50]}")
    if r2.status_code not in (404, 405, 403):
        print(f"  Body: {r2.text[:300]}")

# Tambem tentar GET nos candidatos
for path in ["/api/order/export-by-product-sheet/", "/api/order/export-sheet/", "/api/orderline/export-sheet/"]:
    r3 = s.get(f"{PC_URL}{path}", params=params, headers=h, timeout=10)
    print(f"GET {path} -> {r3.status_code} | {r3.headers.get('content-type','')[:50]}")
    if r3.status_code not in (404, 405, 403):
        print(f"  Body: {r3.text[:300]}")
