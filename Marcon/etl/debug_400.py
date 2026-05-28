import requests

PC_URL = "https://sys.precocerto.co"
s = requests.Session()
s.get(PC_URL + "/login/")
csrf = s.cookies.get("csrftoken", "")
s.post(PC_URL + "/authenticate_user_ajax/",
    data={"username_login": "comercial@casaeletromarcon.com.br",
          "password_login": "eletro123", "csrfmiddlewaretoken": csrf},
    headers={"Referer": PC_URL + "/login/", "X-Requested-With": "XMLHttpRequest"})
csrf2 = s.cookies.get("csrftoken", csrf)
h = {"x-csrftoken": csrf2, "Accept": "application/json",
     "Referer": PC_URL + "/gerenciar/pedidos-de-venda/"}

# Testar períodos: um que funciona (out/2025) e um que falha (nov/2025)
tests = [
    ("2025-10-01", "2025-10-31", "01/10/2025 - 31/10/2025"),
    ("2025-11-01", "2025-11-30", "01/11/2025 - 30/11/2025"),
    ("2025-11-01", "2025-11-15", "01/11/2025 - 15/11/2025"),  # quinzena
    ("2026-01-01", "2026-01-15", "01/01/2026 - 15/01/2026"),  # quinzena jan
]

for da, db, sc in tests:
    r = s.get(PC_URL + "/api/order/export-orders-by-line",
        params={"source_created": sc, "date_after": da, "date_before": db, "id__notin": ""},
        headers=h, timeout=30)
    print(f"{da} → {db}: status={r.status_code}")
    if r.status_code != 200:
        print(f"  Body: {r.text[:300]}")
    else:
        print(f"  task_id: {r.json().get('task_id','?')[:20]}...")
