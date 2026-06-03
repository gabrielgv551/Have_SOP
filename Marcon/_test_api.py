import requests
TOKEN = "adad5861e5d6cc4e25f4e0d6e2d17eafd87e7c90a2a535d3690a885761fd644e"
r = requests.post("https://api.tiny.com.br/api2/produtos.pesquisa.php", 
    data={"token": TOKEN, "formato": "JSON", "pagina": "1"}, timeout=30)
print(f"Status: {r.status_code}")
d = r.json()
ret = d.get("retorno") or {}
print(f"Status API: {ret.get('status')}")
prods = ret.get("produtos") or []
print(f"Produtos: {len(prods)}")
if prods:
    p = prods[0].get("produto") or prods[0]
    print(f"Primeiro: {p.get('codigo')} - {p.get('nome')[:40]}")
