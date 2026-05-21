"""
Testa endpoints descobertos no bundle:
- api/order/facts/total
- /api/order (sem trailing slash)
- /api/orderline
- /api/order/export-orders-by-line
- source_created como parâmetro de data
"""
import requests
import json
import re

BASE = "https://sys.precocerto.co"
EMAIL = "comercial@casaeletromarcon.com.br"
SENHA = "eletro123"

r_jwt = requests.post(BASE + "/api/token/", json={"username": EMAIL, "password": SENHA}, timeout=10)
access = r_jwt.json()["access"]
api = requests.Session()
api.headers.update({
    "Authorization": f"Bearer {access}",
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124",
    "Referer": BASE + "/gerenciar/pedidos-de-venda/",
    "X-Requested-With": "XMLHttpRequest",
})
print(f"✅ Autenticado (user_id=16082)\n")

# Parâmetros usados pela URL original do usuário
date_params = {
    "source_created": "04/05/2026 - 19/05/2026",
    "date_before": "2026-05-19",
    "date_after": "2026-05-04",
    "ordering": "-source_created",
}

# 1. Testar api/order/facts/total (sem trailing ?)
print("=== api/order/facts/total ===")
for params in [
    date_params,
    {"source_created": "04/05/2026 - 19/05/2026"},
    {"date_before": "2026-05-19", "date_after": "2026-05-04"},
    {},
]:
    r = api.get(BASE + "/api/order/facts/total", params=params, timeout=15)
    ct = r.headers.get("Content-Type", "")
    if r.status_code == 200 and "json" in ct:
        print(f"  ✅ {params}: {r.text[:400]}")
        break
    else:
        print(f"  {params} → {r.status_code} | {ct[:40]}")

# 2. Testar /api/order (sem slash) com params
print("\n=== /api/order com vários params ===")
test_params = [
    date_params,
    {"source_created": "04/05/2026 - 19/05/2026", "limit": 10},
    {"ordering": "-source_created", "limit": 10, "offset": 0},
    {"limit": 10},
    {"date_before": "2026-05-19", "date_after": "2026-05-04", "limit": 10},
]
for params in test_params:
    r = api.get(BASE + "/api/order", params=params, timeout=15)
    ct = r.headers.get("Content-Type", "")
    if r.status_code == 200 and "json" in ct:
        d = r.json()
        print(f"  ✅ Params {params}:")
        print(f"     Keys: {list(d.keys()) if isinstance(d, dict) else 'lista'}")
        if isinstance(d, dict) and ("rows" in d or "count" in d or "results" in d):
            total = d.get("total", d.get("count", "?"))
            print(f"     total={total}")
            rows = d.get("rows", d.get("results", []))
            if rows:
                print(f"     Primeiro row keys: {list(rows[0].keys())[:20]}")
        break
    elif r.status_code not in (404, 405, 500):
        print(f"  ⚠️ {params} → {r.status_code}: {r.text[:100]}")

# 3. Testar /api/orderline com params de order
print("\n=== /api/orderline ===")
for params in [{"limit": 10}, {"order_id__in": "1", "limit": 5}, date_params]:
    r = api.get(BASE + "/api/orderline", params=params, timeout=15)
    ct = r.headers.get("Content-Type", "")
    if r.status_code == 200 and "json" in ct:
        d = r.json()
        print(f"  ✅ Params {params}: total={d.get('total', '?')}")
        if d.get("rows"):
            print(f"     Primeiro row: {list(d['rows'][0].keys())[:10]}")
        break
    elif r.status_code not in (404, 405, 500):
        print(f"  ⚠️ {params} → {r.status_code}: {r.text[:100]}")

# 4. Buscar no bundle como a tabela busca dados
print("\n=== Analisando bundle para encontrar getData da tabela ===")
# Procurar mais padrões no bundle salvo
with open("bundle_60611d8.js", encoding="utf-8") as f:
    bundle = f.read()

# Procurar a função getData e seu contexto
getData_pattern = r'getData.{0,50}regeneratorRuntime.{0,500}'
matches = re.findall(getData_pattern, bundle)
for m in matches[:3]:
    print(f"  getData: ...{m[:300]}...")

# Procurar o $api plugin (onde os endpoints são definidos)
api_plugin = re.findall(r'\$api\.[a-z_]+\.[a-z]+\([^)]{0,200}\)', bundle, re.IGNORECASE)
print("\n  Todas as chamadas $api encontradas:")
for a in set(api_plugin):
    print(f"    {a[:150]}")

# Procurar axios com "order"
axios_order = re.findall(r'axios[^;]{0,20}(?:get|post)\(["\'](?:api/)?order[^"\']*["\'][^)]{0,100}\)', bundle)
print("\n  Chamadas axios para order:")
for a in set(axios_order):
    print(f"    {a[:200]}")

# Procurar source_created em contexto maior
sc_ctx = []
for m in re.finditer(r'source_created', bundle):
    start = max(0, m.start()-200)
    end = min(len(bundle), m.end()+300)
    sc_ctx.append(bundle[start:end].replace('\n', ' '))
print("\n  Contextos de source_created:")
for ctx in sc_ctx[:5]:
    print(f"    ...{ctx}...")

# 5. Buscar no chunk específico da página (159 = d91121a.js)
print("\n=== Baixando chunk específico da página de pedidos (d91121a.js) ===")
r_chunk = requests.get(BASE + "/_nuxt/d91121a.js", timeout=30)
chunk_content = r_chunk.text
print(f"Tamanho: {len(chunk_content):,} bytes")

for label, pattern in [
    ("order/facts", r'order/facts'),
    ("source_created", r'source_created'),
    ("$axios.$get", r'\$axios\.\$get'),
    ("$axios.get", r'\$axios\.get\(["\'](?:api/)?order'),
    ("$api.order", r'\$api\.order'),
    ("getData", r'getData'),
    ("filtersCard", r'filtersCard'),
    ("localQuery", r'localQuery'),
]:
    matches = list(re.finditer(pattern, chunk_content, re.IGNORECASE))
    if matches:
        print(f"\n  [{label}] — {len(matches)} ocorrências:")
        for m in matches[:2]:
            start = max(0, m.start()-100)
            end = min(len(chunk_content), m.end()+300)
            print(f"    ...{chunk_content[start:end].replace(chr(10),' ')}...")
