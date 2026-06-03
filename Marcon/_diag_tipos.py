"""
Verificar tipos dos produtos na API v2 do Tiny da Marcon.
"""
import requests

TOKEN = "adad5861e5d6cc4e25f4e0d6e2d17eafd87e7c90a2a535d3690a885761fd644e"
BASE = "https://api.tiny.com.br/api2"

def tiny_post(endpoint, params):
    r = requests.post(f"{BASE}/{endpoint}", data={"token": TOKEN, "formato": "JSON", **params}, timeout=30)
    return r.json()

# Buscar 20 produtos e ver tipos
print("=== Tipos dos primeiros produtos ===")
d = tiny_post("produtos.pesquisa.php", {"pagina": "1"})
prods = (d.get("retorno") or {}).get("produtos") or []

kits_encontrados = []
for item in prods[:30]:
    p = item.get("produto") or item
    pid = p.get("id")
    sku = p.get("codigo", "")
    det = tiny_post("produto.obter.php", {"id": pid})
    prod = (det.get("retorno") or {}).get("produto") or {}
    tipo = prod.get("tipo", "?")
    tem_kit = "SIM" if prod.get("kit") else "NAO"
    tem_grade = "SIM" if prod.get("grade") else "NAO"
    print(f"  {sku:25s} | tipo={tipo} | kit={tem_kit} | grade={tem_grade}")
    if tipo == "K" and prod.get("kit"):
        kits_encontrados.append(prod)

print(f"\n=== Kits encontrados: {len(kits_encontrados)} ===")
for k in kits_encontrados:
    print(f"  SKU: {k.get('codigo')} | Nome: {k.get('nome')}")
    for entry in k.get("kit", []):
        item = entry.get("item") or entry
        print(f"    -> {item.get('codigo')} x{item.get('quantidade')}")
