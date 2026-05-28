import requests

PC_URL = "https://sys.precocerto.co"
s = requests.Session()
s.get(f"{PC_URL}/login/")
csrf = s.cookies.get("csrftoken", "")
login_r = s.post(f"{PC_URL}/authenticate_user_ajax/",
    data={"username_login": "comercial@casaeletromarcon.com.br",
          "password_login": "eletro123",
          "csrfmiddlewaretoken": csrf},
    headers={"Referer": f"{PC_URL}/login/", "X-Requested-With": "XMLHttpRequest"})
sessionid = s.cookies.get("sessionid", "")
csrf2 = s.cookies.get("csrftoken", csrf)
h = {"Cookie": f"sessionid={sessionid}; csrftoken={csrf2}",
     "x-csrftoken": csrf2, "Accept": "application/json",
     "Referer": f"{PC_URL}/gerenciar/pedidos-de-venda/"}

params = {
    "source_created": "01/02/2025 - 28/02/2025",
    "date_after": "2025-02-01",
    "date_before": "2025-02-28",
    "ordering": "-source_created",
}

# Baseado no S3 path: orders-by-product-sheet-exported
candidates = [
    ("GET",  "/api/orders-by-product-sheet-exported/"),
    ("POST", "/api/orders-by-product-sheet-exported/"),
    ("GET",  "/api/order/export-orders-by-product-sheet/"),
    ("POST", "/api/order/export-orders-by-product-sheet/"),
    ("GET",  "/api/order/orders-by-product-sheet-export/"),
    ("POST", "/api/order/orders-by-product-sheet-export/"),
    ("GET",  "/gerenciar/pedidos-de-venda/exportar-por-produto/"),
    ("GET",  "/gerenciar/pedidos-de-venda/export-por-produto/"),
    # Tentar com format param
    ("GET",  "/api/order/"),
    ("GET",  "/api/orderline/"),
]

for method, path in candidates:
    try:
        extra = {}
        if path == "/api/order/" or path == "/api/orderline/":
            extra = {"format": "xlsx"}
        p = {**params, **extra}
        if method == "GET":
            r = s.get(f"{PC_URL}{path}", params=p, headers=h, timeout=10, allow_redirects=False)
        else:
            r = s.post(f"{PC_URL}{path}", json=params, headers=h, timeout=10, allow_redirects=False)
        ct = r.headers.get("content-type", "")
        loc = r.headers.get("location", "")
        print(f"{method} {path} -> {r.status_code} | {ct[:40]} {loc[:60]}")
        if r.status_code not in (404, 405, 403) and "html" not in ct:
            print(f"  Body: {r.text[:300]}")
    except Exception as e:
        print(f"{method} {path} -> ERRO: {e}")
