"""
Diagnóstico: como a API v2 do Tiny retorna kits da conta Marcon.
"""
import requests, json

TOKEN = "adad5861e5d6cc4e25f4e0d6e2d17eafd87e7c90a2a535d3690a885761fd644e"
BASE = "https://api.tiny.com.br/api2"

def tiny_post(endpoint, params):
    r = requests.post(f"{BASE}/{endpoint}", data={"token": TOKEN, "formato": "JSON", **params}, timeout=30)
    return r.json()

# 1. Buscar alguns produtos e ver quais têm tipo != 'P'
print("=== Buscando produtos para ver tipos ===")
d = tiny_post("produtos.pesquisa.php", {"pagina": "1"})
prods = (d.get("retorno") or {}).get("produtos") or []

# Ver detalhes dos primeiros 10 produtos
for item in prods[:10]:
    p = item.get("produto") or item
    pid = p.get("id")
    sku = p.get("codigo", "")
    det = tiny_post("produto.obter.php", {"id": pid})
    prod = (det.get("retorno") or {}).get("produto") or {}
    tipo = prod.get("tipo", "?")
    kit_data = prod.get("kit")
    tem_kit = "SIM" if kit_data else "NAO"
    print(f"  {sku} | tipo={tipo} | tem_kit={tem_kit}")

# 2. Buscar especificamente um produto que parece ser kit (DF-KIT10LX5/0HTODOS)
print("\n=== Detalhes de DF-KIT10LX5/0HTODOS ===")
d2 = tiny_post("produtos.pesquisa.php", {"pesquisa": "DF-KIT10LX5/0HTODOS"})
prods2 = (d2.get("retorno") or {}).get("produtos") or []
if prods2:
    p2 = prods2[0].get("produto") or prods2[0]
    det2 = tiny_post("produto.obter.php", {"id": p2["id"]})
    print(json.dumps(det2, indent=2, ensure_ascii=False))
else:
    print("Não encontrado")
