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

js = s.get(f"{PC_URL}/_nuxt/4dcf3a1.js", timeout=30).text
print(f"Chunk size: {len(js):,}")

# Buscar contexto ao redor de SET_IS_EXPORTING_ORDERS
for m in re.finditer(r'.{0,300}EXPORTING_ORDERS.{0,300}', js):
    print(f"\n--- EXPORTING_ORDERS context ---")
    print(m.group()[:500])

# Buscar todos os axios/fetch calls que contenham "order" 
print("\n\n--- axios/fetch calls com 'order' ---")
for m in re.finditer(r'(?:axios|fetch|get|post)\s*\(\s*["`\']([^"`\']+order[^"`\']*)["`\']', js, re.IGNORECASE):
    print(f"  {m.group(1)}")

# Buscar urls próximas de "export"
print("\n\n--- URLs proximas de 'export' ---")
for m in re.finditer(r'.{0,150}export.{0,150}', js, re.IGNORECASE):
    ctx = m.group()
    if '/api/' in ctx or 'url' in ctx.lower() or 'path' in ctx.lower() or 'axios' in ctx.lower():
        print(f"  {ctx[:200]}")
