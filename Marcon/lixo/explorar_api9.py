"""
1. Decodifica o JWT para ver company_id
2. Inspeciona o corpo da resposta 500 detalhadamente
3. Testa o login com abordagem do browser (simulando Nuxt auth)
"""
import requests
import json
import re
import base64

BASE = "https://sys.precocerto.co"
EMAIL = "comercial@casaeletromarcon.com.br"
SENHA = "eletro123"

# Obter JWT
r_jwt = requests.post(BASE + "/api/token/", json={"username": EMAIL, "password": SENHA}, timeout=10)
tokens = r_jwt.json()
access = tokens["access"]
refresh = tokens["refresh"]

# Decodificar o payload do JWT (sem verificar assinatura)
print("=== Payload do JWT de acesso ===")
payload_b64 = access.split(".")[1]
# Adicionar padding
payload_b64 += "=" * (4 - len(payload_b64) % 4)
payload = json.loads(base64.b64decode(payload_b64))
print(json.dumps(payload, indent=2, ensure_ascii=False))

# Campos importantes
user_id = payload.get("user_id")
company_id = payload.get("company_id") or payload.get("org_id") or payload.get("organization_id")
print(f"\nuser_id: {user_id}")
print(f"company_id no JWT: {company_id}")

# ========== Tentar acesso direto à empresa via endpoint legado Django ==========
sess = requests.Session()
sess.headers.update({"User-Agent": "Mozilla/5.0", "Accept": "application/json"})
sess.headers["Authorization"] = f"Bearer {access}"

print("\n=== Tentar /api/user/{user_id}/ ===")
r = sess.get(BASE + f"/api/user/{user_id}/", timeout=10)
print(f"Status: {r.status_code}, CT: {r.headers.get('Content-Type', '')[:50]}")
if r.status_code == 200 and "json" in r.headers.get("Content-Type", ""):
    print(r.text[:500])

# ========== Pegar o erro 500 completo ==========
print("\n=== Corpo COMPLETO do 500 em /api/order/facts ===")
r = sess.get(BASE + "/api/order/facts", timeout=15)
# Salvar a resposta de erro em arquivo para inspeção
with open("erro_500_order_facts.html", "w", encoding="utf-8") as f:
    f.write(r.text)
print(f"Resposta salva em erro_500_order_facts.html ({len(r.text)} bytes)")
# Analisar o que é o HTML
if "Preço Certo" in r.text and "nuxt" in r.text.lower():
    print("→ Parece SPA do Nuxt (custom error page do CloudFront)")
elif "Internal Server Error" in r.text or "Traceback" in r.text:
    print("→ Parece erro real do Django")
    # Extrair traceback
    tb = re.search(r'Traceback.*?(\w+Error[^\n]*)', r.text, re.DOTALL)
    if tb:
        print(f"Erro: {tb.group(0)[-300:]}")

# ========== Tentar com X-Requested-With: XMLHttpRequest ==========
print("\n=== Tentando com XMLHttpRequest header ===")
xhr_headers = {
    "Authorization": f"Bearer {access}",
    "Accept": "application/json",
    "X-Requested-With": "XMLHttpRequest",
    "Content-Type": "application/json",
}
r = requests.get(BASE + "/api/order/facts", headers=xhr_headers, timeout=15)
print(f"Status: {r.status_code}, CT: {r.headers.get('Content-Type', '')}")
if "json" in r.headers.get("Content-Type", ""):
    print(r.text[:500])
else:
    print(r.text[:300])

# ========== Verificar o endpoint de pedidos no site legado ==========
print("\n=== Verificando endpoints do site legado ==========")
for ep in [
    "/orders/",
    "/orders/api/",
    "/orders/json/",
    "/sales-orders/",
    "/api/v1/order/facts",
    "/api/v2/order/facts",
    "/api/order/sales/",
    "/api/sales/",
]:
    r = requests.get(BASE + ep, 
                     headers={"Authorization": f"Bearer {access}", "Accept": "application/json"},
                     timeout=10)
    if r.status_code not in (404, 405):
        ct = r.headers.get("Content-Type", "")
        print(f"⚠️ {ep} → {r.status_code} | {ct[:40]}")
        if r.status_code == 200 and "json" in ct:
            print(f"  ✅ {r.text[:300]}")

# ========== Tentar endpoint de "facts" via GraphQL ou alternativa ==========
print("\n=== Verificar se há endpoint GraphQL ===")
for ep in ["/graphql", "/api/graphql", "/graphql/", "/api/graphql/"]:
    r = requests.post(BASE + ep,
                      json={"query": "{ __typename }"},
                      headers={"Authorization": f"Bearer {access}", "Content-Type": "application/json"},
                      timeout=10)
    if r.status_code not in (404, 405):
        print(f"⚠️ {ep} → {r.status_code}: {r.text[:100]}")
