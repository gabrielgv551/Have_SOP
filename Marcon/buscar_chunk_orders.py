"""
Baixa os chunks específicos da página pedidos-de-venda e extrai as chamadas de API
Chunk IDs para /gerenciar/pedidos-de-venda: 8, 175, 1, 3, 159
Mapeamento de chunk IDs para nomes:
  8  → c731c24.js
  175 → 4e057e8.js
  1  → e33d7b3.js
  3  → e48b4e3.js
  159 → d91121a.js
"""
import requests
import re

BASE = "https://sys.precocerto.co/_nuxt/"

chunk_map = {
    8: "c731c24",
    175: "4e057e8",
    1: "e33d7b3",
    3: "e48b4e3",
    159: "d91121a",
}

def search_chunk(chunk_id, filename):
    url = f"{BASE}{filename}.js"
    print(f"\n{'='*60}")
    print(f"Chunk {chunk_id}: {url}")
    r = requests.get(url, timeout=30)
    content = r.text
    print(f"Tamanho: {len(content):,} bytes")
    
    found = False
    patterns = [
        (r'order/facts', 'order/facts'),
        (r'source_created', 'source_created'),
        (r'date_before|date_after', 'date_before/after'),
        (r'\$axios[^;]{0,150}order', 'axios+order'),
        (r'\$api[^;]{0,150}order', 'api+order'),
        (r'ordering', 'ordering'),
        (r'fetchOrders|getOrders|loadOrders|buscarPedidos', 'fetch orders func'),
        (r'orderFacts|order_facts', 'orderFacts'),
        (r'"order"', '"order" string'),
        (r'company_id|companyId', 'company_id'),
        (r'facts', 'facts'),
    ]
    
    for pattern, label in patterns:
        matches = list(re.finditer(pattern, content, re.IGNORECASE))
        if matches:
            found = True
            print(f"\n  [{label}] — {len(matches)} ocorrências:")
            for m in matches[:3]:
                start = max(0, m.start() - 120)
                end = min(len(content), m.end() + 250)
                ctx = content[start:end].replace('\n', ' ')
                print(f"    ...{ctx}...")
    
    if not found:
        print("  Nada relevante encontrado neste chunk")
    
    return content

# Baixar todos os chunks relevantes
all_content = ""
for cid, fname in chunk_map.items():
    content = search_chunk(cid, fname)
    all_content += content

# Busca global em todo o conteúdo combinado
print("\n" + "="*60)
print("BUSCA GLOBAL em todos os chunks:")
print("="*60)

# Procurar chamadas de API de pedidos
api_calls = re.findall(r'["\'](?:api/|/api/)(?:[^"\']{0,60}order[^"\']{0,60})["\']', all_content)
print(f"\nEndpoints de order encontrados:")
for a in set(api_calls):
    print(f"  {a}")

# Procurar params de data
date_params = re.findall(r'["\'][a-z_]{0,20}(?:date|created|period)[a-z_]{0,20}["\']', all_content, re.IGNORECASE)
print(f"\nParâmetros de data encontrados:")
for d in set(date_params):
    print(f"  {d}")
