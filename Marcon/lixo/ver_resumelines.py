"""Inspeciona campos reais de /api/order e /api/orderline"""
import sys, json
sys.path.insert(0, ".")
from PRECOCERTO_ETL import autenticar_playwright, _api_headers, PC_URL
import requests

auth   = autenticar_playwright()
params = {"date_after": "2026-05-15", "date_before": "2026-05-16",
          "ordering": "-source_created", "limit": 3, "offset": 0}

SEP = "\n" + "─" * 60

# ── /api/order ────────────────────────────────────────────────
print(SEP)
print("  /api/order — campos do 1º pedido")
print(SEP)
r = requests.get(PC_URL + "/api/order", headers=_api_headers(auth), params=params, timeout=60)
d = r.json()
print(f"  total: {d.get('total')}  |  rows recebidas: {len(d.get('rows', []))}")
if d.get("rows"):
    first = d["rows"][0]
    for k, v in first.items():
        if k == "resume_lines":
            print(f"\n  resume_lines ({len(v or [])} itens):")
            if v:
                print("    1º item — campos:")
                for rk, rv in v[0].items():
                    print(f"      {rk}: {rv}")
        else:
            print(f"  {k}: {v}")

# ── /api/orderline ────────────────────────────────────────────
print(SEP)
print("  /api/orderline — campos do 1º item")
print(SEP)
r2 = requests.get(PC_URL + "/api/orderline", headers=_api_headers(auth), params=params, timeout=60)
d2 = r2.json()
print(f"  total: {d2.get('total')}  |  rows recebidas: {len(d2.get('rows', []))}")
if d2.get("rows"):
    for k, v in d2["rows"][0].items():
        print(f"  {k}: {v}")
