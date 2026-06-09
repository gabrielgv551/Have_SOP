import sys
import os
import requests
from dotenv import load_dotenv

sys.path.append(r"c:\Users\HAVE\Desktop\Have_SOP\Marcon\etl")
import PRECOCERTO_ETL as pc

load_dotenv(r"c:\Users\HAVE\Desktop\Have_SOP\.env")

print("Autenticando...")
session = pc.autenticar_requests()
print("Cookies:", session.cookies.get_dict())

date_after = "2026-05-01"
date_before = "2026-05-31"

csrf = session.cookies.get("csrftoken", "")
h = {
    "x-csrftoken": csrf,
    "Accept": "application/json",
    "Referer": f"{pc.PC_URL}/gerenciar/pedidos-de-venda/",
}

print("Disparando export...")
r = session.get(
    f"{pc.PC_URL}/api/order/export-orders-by-line",
    params={
        "source_created": pc._fmt_date_range(date_after, date_before),
        "date_after":     date_after,
        "date_before":    date_before,
        "id__notin":      "",
    },
    headers=h, timeout=30,
)

print("Status:", r.status_code)
print("Headers:", r.headers)
print("Text:", r.text[:500])
