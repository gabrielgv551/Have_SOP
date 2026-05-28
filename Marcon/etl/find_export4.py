import requests, re

PC_URL = "https://sys.precocerto.co"
s = requests.Session()
s.get(f"{PC_URL}/login/")
csrf = s.cookies.get("csrftoken", "")
s.post(f"{PC_URL}/authenticate_user_ajax/",
    data={"username_login": "comercial@casaeletromarcon.com.br",
          "password_login": "eletro123",
          "csrfmiddlewaretoken": csrf},
    headers={"Referer": f"{PC_URL}/login/", "X-Requested-With": "XMLHttpRequest"})
sessionid = s.cookies.get("sessionid", "")
csrf2 = s.cookies.get("csrftoken", csrf)
h = {"Cookie": f"sessionid={sessionid}; csrftoken={csrf2}"}

# Buscar a pagina gerenciar/pedidos-de-venda
page = s.get(f"{PC_URL}/gerenciar/pedidos-de-venda/", headers=h, timeout=15)
print(f"Status: {page.status_code}")

scripts = re.findall(r'src=["\']([^"\']+\.js)["\']', page.text)
print(f"Scripts: {scripts}")

# Salvar HTML para inspecao
with open("pedidos_page.html", "w", encoding="utf-8") as f:
    f.write(page.text)
print("HTML salvo em pedidos_page.html")

# Buscar diretamente no HTML por "export" ou "planilha"
export_hits = re.findall(r'.{0,100}(?:export|planilha|sheet|download)[^<]{0,100}', page.text, re.IGNORECASE)
print(f"\nHits no HTML ({len(export_hits)}):")
for h2 in export_hits[:20]:
    print(f"  {h2.strip()}")
