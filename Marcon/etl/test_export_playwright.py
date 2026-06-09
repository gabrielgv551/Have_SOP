import sys
import os
import requests
from dotenv import load_dotenv

sys.path.append(r"c:\Users\HAVE\Desktop\Have_SOP\Marcon\etl")
import PRECOCERTO_ETL as pc

load_dotenv(r"c:\Users\HAVE\Desktop\Have_SOP\.env")

print("Autenticando via Playwright...")
auth = pc.autenticar_playwright()
print("Sessão:", auth['sessionid'])

date_after = "2026-05-01"
date_before = "2026-05-31"

h = pc._api_headers(auth)
h["Accept"] = "application/json"

session = requests.Session()

print("Disparando export...")
r = session.get(
    f"{pc.PC_URL}/api/order/export-orders-by-line",
    params={
        "source_created": pc._fmt_date_range(date_after, date_before),
        "date_after":     date_after,
        "date_before":    date_before,
        "id__notin":      "",
    },
    headers=h, timeout=30, allow_redirects=False
)

print("Status:", r.status_code)
print("Headers:", r.headers)
print("Text:", r.text[:500])
if r.status_code in (301, 302):
    print("Location:", r.headers.get("Location"))
