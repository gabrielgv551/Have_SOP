import sys
import os
import requests
from dotenv import load_dotenv

sys.path.append(r"c:\Users\HAVE\Desktop\Have_SOP\Marcon\etl")
import PRECOCERTO_ETL as pc

load_dotenv(r"c:\Users\HAVE\Desktop\Have_SOP\.env")

print("Autenticando via requests...")
session = pc.autenticar_requests()

print("Tentando /api/orderline com a propria sessao autenticada...")
r = session.get(
    f"{pc.PC_URL}/api/orderline",
    params={"limit": 10, "offset": 0},
    headers={"Accept": "application/json", "x-csrftoken": session.cookies.get("csrftoken")},
    allow_redirects=False
)

print("Status:", r.status_code)
if r.status_code in (301, 302):
    print("Redirect Location:", r.headers.get("Location"))
print("Content-Type:", r.headers.get("Content-Type"))
if 'application/json' in r.headers.get("Content-Type", ""):
    print(r.json())
else:
    print(r.text[:500])
