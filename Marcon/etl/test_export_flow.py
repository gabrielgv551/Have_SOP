import requests, json, time

PC_URL = "https://sys.precocerto.co"
s = requests.Session()
s.get(PC_URL + "/login/")
csrf = s.cookies.get("csrftoken", "")
s.post(PC_URL + "/authenticate_user_ajax/",
    data={"username_login": "comercial@casaeletromarcon.com.br",
          "password_login": "eletro123",
          "csrfmiddlewaretoken": csrf},
    headers={"Referer": PC_URL + "/login/", "X-Requested-With": "XMLHttpRequest"})
sessionid = s.cookies.get("sessionid", "")
csrf2 = s.cookies.get("csrftoken", csrf)
h = {"Cookie": f"sessionid={sessionid}; csrftoken={csrf2}",
     "x-csrftoken": csrf2, "Accept": "application/json"}

# 1. Disparar export
r = s.get(PC_URL + "/api/order/export-orders-by-line",
    params={"source_created": "01/02/2025 - 28/02/2025",
            "date_before": "2025-02-28",
            "date_after": "2025-02-01",
            "id__notin": ""},
    headers=h, timeout=30)
task_id = r.json()["task_id"]
print(f"task_id: {task_id}")

# 2. Polling
download_url = None
for i in range(40):
    time.sleep(4)
    tr = s.get(f"{PC_URL}/api/task/result/{task_id}", headers=h, timeout=10)
    d = tr.json()
    status = d.get("status", "?")
    print(f"  [{i}] status={status}")
    if status == "SUCCESS":
        download_url = json.loads(d["result"])
        print(f"URL: {download_url[:100]}...")
        break

# 3. Baixar e verificar
if download_url:
    import pandas as pd
    resp = requests.get(download_url, timeout=60)
    print(f"\nDownload: {resp.status_code} | {len(resp.content):,} bytes")
    df = pd.read_excel(resp.content)
    print(f"Shape: {df.shape}")
    print(f"Colunas: {list(df.columns[:10])}")
    print(f"Linhas: {len(df):,}")
    print(f"Pedidos unicos: {df['Número do pedido'].nunique():,}")
    print(f"Total do pedido sum (unique): {df.drop_duplicates('Número do pedido')['Total do pedido'].sum():,.2f}")
