import requests

token = "adad5861e5d6cc4e25f4e0d6e2d17eafd87e7c90a2a535d3690a885761fd644e"
r = requests.post("https://api.tiny.com.br/api2/pedidos.pesquisa.php", data={
    "token": token,
    "formato": "JSON",
    "dataInicial": "01/04/2025",
    "dataFinal": "05/04/2025",
    "pagina": "1"
})

pedidos = r.json()["retorno"]["pedidos"]
primeiro = pedidos[0]["pedido"]
print("Tem itens?", "itens" in primeiro)
print("Chaves disponíveis:", list(primeiro.keys()))