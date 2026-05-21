"""
Analisa o chunk d91121a.js (página de pedidos) para encontrar o dataUrl e chamadas de API
"""
import requests
import re
import json

BASE = "https://sys.precocerto.co"

# Baixar chunks relevantes para pedidos
chunks = {
    "d91121a": "chunk específico da página pedidos",
    "4e057e8": "chunk 175 (carregado junto com pedidos)",
    "c731c24": "chunk 8",
}

all_content = {}
for fname, desc in chunks.items():
    url = f"{BASE}/_nuxt/{fname}.js"
    r = requests.get(url, timeout=30)
    all_content[fname] = r.text
    print(f"Baixado {fname}.js: {len(r.text):,} bytes ({desc})")

# Análise focada em d91121a.js (chunk 159 - específico de pedidos)
content = all_content["d91121a"]

print("\n" + "="*60)
print("ANÁLISE d91121a.js (chunk específico de pedidos)")
print("="*60)

# 1. dataUrl
print("\n=== dataUrl ===")
for m in re.finditer(r'dataUrl[^,;]{0,200}', content):
    ctx = content[max(0,m.start()-50):min(len(content),m.end()+100)]
    print(f"  {ctx.replace(chr(10),' ')[:300]}")

# 2. $api calls
print("\n=== Chamadas $api ===")
for m in re.finditer(r'\$api\.[a-z._]+\([^)]{0,300}\)', content, re.IGNORECASE):
    print(f"  {m.group()[:250]}")

# 3. axios calls
print("\n=== Chamadas $axios ===")
for m in re.finditer(r'\$axios[^;]{0,300}', content, re.IGNORECASE):
    ctx = m.group()
    if any(kw in ctx for kw in ['order', 'fact', 'get', 'post']):
        print(f"  {ctx[:300]}")

# 4. source_created context
print("\n=== Contexto source_created ===")
for m in re.finditer(r'source_created', content, re.IGNORECASE):
    start = max(0, m.start()-300)
    end = min(len(content), m.end()+300)
    ctx = content[start:end].replace('\n', ' ')
    print(f"  ...{ctx}...")

# 5. Procurar o componente da tabela de pedidos
print("\n=== Procurando URL da tabela ===")
# Padrão: dataUrl ou url com order
url_patterns = re.findall(r'(?:dataUrl|url)[^=]{0,5}=[^,;]{0,200}(?:order|fact)[^,;]{0,100}', content, re.IGNORECASE)
for p in url_patterns[:10]:
    print(f"  {p[:300]}")

# 6. Procurar em 4e057e8.js também
content2 = all_content["4e057e8"]
print("\n" + "="*60)
print("ANÁLISE 4e057e8.js (chunk 175 - pedidos)")
print("="*60)

print("\n=== order/facts em 4e057e8 ===")
for m in re.finditer(r'order.{0,5}facts', content2, re.IGNORECASE):
    start = max(0, m.start()-200)
    end = min(len(content2), m.end()+400)
    print(f"  ...{content2[start:end].replace(chr(10),' ')}...")

print("\n=== dataUrl em 4e057e8 ===")
for m in re.finditer(r'dataUrl', content2, re.IGNORECASE):
    start = max(0, m.start()-100)
    end = min(len(content2), m.end()+300)
    print(f"  ...{content2[start:end].replace(chr(10),' ')}...")

print("\n=== Todos endpoints em 4e057e8 ===")
endpoints = re.findall(r'["\'](?:/api/|api/)[a-z/_-]{3,50}["\']', content2)
for e in set(endpoints):
    print(f"  {e}")

# Agora tentar chamar a API de orders com os parâmetros encontrados
print("\n" + "="*60)
print("TESTANDO ENDPOINTS DESCOBERTOS")
print("="*60)

r_jwt = requests.post(BASE + "/api/token/", json={"username": "comercial@casaeletromarcon.com.br", "password": "eletro123"}, timeout=10)
access = r_jwt.json()["access"]
api = requests.Session()
api.headers.update({
    "Authorization": f"Bearer {access}",
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0",
    "Referer": BASE + "/gerenciar/pedidos-de-venda/",
})

test_endpoints = [
    "/api/order/facts/total",
    "/api/order/margin-summary",
    "/api/order/detail/",
    "/api/orderline",
]
for ep in test_endpoints:
    r = api.get(BASE + ep, params={"limit": 5}, timeout=15)
    ct = r.headers.get("Content-Type", "")
    if r.status_code == 200 and "json" in ct:
        d = r.json()
        print(f"✅ {ep}: {json.dumps(d, ensure_ascii=False)[:300]}")
    else:
        print(f"  {ep} → {r.status_code} | {ct[:40]}")
