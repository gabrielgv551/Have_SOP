from sqlalchemy import create_engine, text
import requests
import json
from datetime import datetime

# DB config
engine = create_engine("postgresql+psycopg2://postgres:131105Gv@37.60.236.200:5432/amjls")
with engine.connect() as conn:
    rows = conn.execute(text(
        "SELECT chave, valor FROM configuracoes "
        "WHERE empresa = 'amjls' "
        "AND chave IN ('gefinance_email', 'gefinance_password')"
    ))
    cfg = {r[0]: r[1] for r in rows}

email = cfg.get("gefinance_email")
password = cfg.get("gefinance_password")

# Login
login_url = "https://gateway-web.ge.finance/api/Auth/login"
headers_login = {
    "Content-Type" : "application/json",
    "Accept"       : "application/json, text/plain, */*",
    "Origin"       : "https://app.ge.finance",
    "Referer"      : "https://app.ge.finance/",
    "language"     : "pt-BR",
    "User-Agent"   : "Mozilla/5.0 Chrome/146.0.0.0 Safari/537.36",
}
resp = requests.post(login_url, json={"username": email, "password": password}, headers=headers_login, timeout=20)
resp.raise_for_status()
data = resp.json()

token = (
    data.get("token")
    or data.get("access_token")
    or data.get("accessToken")
    or (data.get("data") or {}).get("token")
)
customer_id = str(data.get("customerId") or (data.get("data") or {}).get("customerId") or "")

import base64
parts = token.split(".")
payload = json.loads(base64.b64decode(parts[1] + "==").decode("utf-8"))
plan_ids_str = payload.get("customerPlanIds", "")
import re
plan_ids = re.findall(r'\d+', str(plan_ids_str))
if plan_ids:
    plan_id = plan_ids[0]
else:
    plan_id = str(payload.get("customerPlanId") or "507")

# Headers
headers = {
    "Authorization"  : f"Bearer {token}",
    "customerid"     : customer_id,
    "customerplanid" : plan_id,
    "language"       : "pt-BR",
    "Accept"         : "application/json, text/plain, */*",
    "Origin"         : "https://app.ge.finance",
    "Referer"        : "https://app.ge.finance/",
    "User-Agent"     : "Mozilla/5.0 Chrome/146.0.0.0 Safari/537.36",
}

# Params
params = {
    "pageSize"       : "10",
    "firstDate"      : "2026-06-01",
    "endDate"        : "2026-06-08",
    "refreshDate"    : datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000"),
    "sortColumn"     : "date",
    "sortType"       : "2",
    "currentPage"    : "1",
    "customerId"     : customer_id,
    "customerPlanId" : plan_id,
    "isTrial"        : "false",
}

api_url = "https://gateway-web.ge.finance/api/SpreadSheet"
resp = requests.get(api_url, headers=headers, params=params, timeout=20)
results = resp.json().get("result", [])

print("--- Raw Date Examples from Gefinance API ---")
for p in results[:10]:
    print(f"Order ID: {p.get('id')} | Date: {repr(p.get('date'))} | Status: {p.get('status')} | Client: {p.get('client')}")
