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

js = s.get("https://sys.precocerto.co/static/bundles/app.a3c5d9f649c0699b3428.js", timeout=30).text
print(f"Bundle size: {len(js):,} chars")

# Buscar todos os paths de API que mencionam "order" e "export"/"sheet"
api_paths = re.findall(r'["`\'](/(?:api|gerenciar)[^"`\'\s]{3,80})["`\']', js)
relevant = sorted(set(p for p in api_paths if any(x in p.lower() for x in
    ['export', 'sheet', 'download', 'xlsx', 'pedidos', 'order'])))
print(f"\nPaths de API com order/export/sheet ({len(relevant)} encontrados):")
for p in relevant:
    print(f"  {p}")

# Buscar o contexto ao redor de "export" + "order"
print("\nContexto 'export' perto de 'order':")
matches = [(m.start(), m.group()) for m in re.finditer(r'.{0,60}export.{0,60}', js, re.IGNORECASE)]
for pos, m in matches[:20]:
    if 'order' in m.lower() or 'pedido' in m.lower() or 'sheet' in m.lower() or 'planilha' in m.lower():
        print(f"  ...{m}...")
