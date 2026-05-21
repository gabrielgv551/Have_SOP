"""
Baixa o bundle 60611d8.js e busca por strings relacionadas a order/facts
para encontrar os parâmetros exatos usados pela página de pedidos
"""
import requests
import re

url = "https://sys.precocerto.co/_nuxt/60611d8.js"
print(f"Baixando {url}...")
r = requests.get(url, timeout=60)
content = r.text
print(f"Tamanho: {len(content):,} bytes\n")

# Salvar para análise
with open("bundle_60611d8.js", "w", encoding="utf-8") as f:
    f.write(content)
print("Salvo em bundle_60611d8.js\n")

# Buscar strings relevantes
patterns = {
    "order/facts": r'order/facts',
    "source_created": r'source_created',
    "pedidos": r'pedidos',
    "date_before": r'date_before',
    "date_after": r'date_after',
    "ordering": r'ordering',
    "fetchOrders": r'fetchOrder|getOrder|loadOrder',
    "company_id param": r'company_id|companyId|company-id',
    "axios order": r'\$axios[^;]{0,80}order',
    "api order": r'\$api[^;]{0,80}order',
    "facts endpoint": r'"[^"]*facts"',
    "gerenciar": r'gerenciar',
}

for label, pattern in patterns.items():
    matches = list(re.finditer(pattern, content))
    if matches:
        print(f"=== {label} ({len(matches)} ocorrências) ===")
        for m in matches[:5]:
            start = max(0, m.start() - 100)
            end = min(len(content), m.end() + 200)
            ctx = content[start:end].replace('\n', ' ')
            print(f"  ...{ctx}...")
            print()
    else:
        print(f"--- {label}: NÃO ENCONTRADO ---\n")
