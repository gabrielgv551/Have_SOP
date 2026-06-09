import sys
import os
import requests
from dotenv import load_dotenv

sys.path.append(r"c:\Users\HAVE\Desktop\Have_SOP\Marcon\etl")
import PRECOCERTO_ETL as pc

load_dotenv(r"c:\Users\HAVE\Desktop\Have_SOP\.env")

PC_URL = pc.PC_URL
PC_EMAIL = pc.PC_EMAIL
PC_SENHA = pc.PC_SENHA

print("Obtendo JWT...")
session = requests.Session()
r = session.post(
    f"{PC_URL}/api/token/",
    json={"username": PC_EMAIL, "password": PC_SENHA},
    timeout=10,
)
if r.status_code == 200:
    token = r.json()["access"]
    session.headers["Authorization"] = f"Bearer {token}"
    print("JWT obtido!")
else:
    print("Erro no JWT", r.status_code, r.text)

print("Tentando /api/orderline...")
r2 = session.get(f"{PC_URL}/api/orderline", params={"limit": 1})
print("Status:", r2.status_code)
print("Text:", r2.text[:200])
