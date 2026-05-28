import requests

PC_URL = "https://sys.precocerto.co"
s = requests.Session()
s.get(f"{PC_URL}/login/")
csrf = s.cookies.get("csrftoken", "")
s.post(f"{PC_URL}/authenticate_user_ajax/",
    data={"username_login": "comercial@casaeletromarcon.com.br",
          "password_login": "eletro123",
          "csrfmiddlewaretoken": csrf},
    headers={"Referer": f"{PC_URL}/login/", "X-Requested-With": "XMLHttpRequest"})
sessionid = s.cookies.get("sessionid", "")
csrf2 = s.cookies.get("csrftoken", csrf)
h = {"Cookie": f"sessionid={sessionid}; csrftoken={csrf2}",
     "x-csrftoken": csrf2, "Accept": "application/json"}

params = {"source_created": "01/02/2025 - 28/02/2025",
          "date_after": "2025-02-01", "date_before": "2025-02-28",
          "ordering": "-source_created", "limit": 1}

# Confirmar que /api/order funciona (sem trailing slash)
r = s.get(f"{PC_URL}/api/order", params=params, headers=h, timeout=15)
print(f"GET /api/order -> {r.status_code} | {r.headers.get('content-type','')[:40]}")
if r.status_code == 200:
    import json
    d = r.json()
    print(f"  total: {d.get('total')}")

# Tentar todas as variações sem trailing slash
for path in ["/api/order/export", "/api/order/sheet",
             "/api/orderline/export", "/api/order/export_by_product_sheet",
             "/api/order/export-by-product-sheet",
             "/api/order/by_product_sheet", "/api/order/by-product-sheet"]:
    r2 = s.get(f"{PC_URL}{path}", params=params, headers=h, timeout=10, allow_redirects=False)
    ct = r2.headers.get("content-type", "")
    loc = r2.headers.get("location", "")
    print(f"GET {path} -> {r2.status_code} | {ct[:40]} {loc[:50]}")
    if r2.status_code not in (404, 405, 403) and "html" not in ct:
        print(f"  {r2.text[:200]}")
