"""Testa a autenticação Playwright e chama /api/order com session auth"""
import sys, json
sys.path.insert(0, ".")
from PRECOCERTO_ETL import autenticar_playwright, _api_headers, _fmt_date_range, PC_URL, baixar_todos
import requests

print("=== Autenticando via Playwright ===")
auth = autenticar_playwright()
print(f"sessionid: {auth['sessionid'][:20]}...")
print(f"csrftoken: {auth['csrftoken'][:20]}...")

print("\n=== GET /api/order — período 04/05 a 19/05/2026 ===")
r = requests.get(
    PC_URL + "/api/order",
    headers=_api_headers(auth),
    params={
        "source_created": _fmt_date_range("2026-05-04", "2026-05-19"),
        "date_after": "2026-05-04",
        "date_before": "2026-05-19",
        "ordering": "-source_created",
        "limit": 10,
    },
    timeout=30,
)
print(f"Status: {r.status_code}")
if r.status_code == 200 and "json" in r.headers.get("Content-Type", ""):
    data = r.json()
    print(f"✅ total={data.get('total')}, rows={len(data.get('rows', []))}")
    if data.get("rows"):
        row = data["rows"][0]
        print(f"Campos ({len(row)}): {list(row.keys())}")
        print(f"Primeiro pedido: number={row.get('number')}, status={row.get('status')}, total={row.get('total')}")
else:
    print(f"Erro: {r.text[:400]}")

print("\n=== Testando paginação (baixar_todos) ===")
try:
    todos = baixar_todos(auth, "2026-05-04", "2026-05-19")
    print(f"✅ Total baixado: {len(todos)} pedidos")
    if todos:
        print(f"Campos do primeiro pedido: {list(todos[0].keys())}")
except Exception as e:
    print(f"Erro: {e}")
