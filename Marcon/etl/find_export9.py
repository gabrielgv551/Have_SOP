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
hdr = {"Cookie": f"sessionid={sessionid}; csrftoken={csrf2}"}

for chunk in ["e1b0c1b.js", "cb7f898.js", "3ef0f1f.js"]:
    js = s.get(f"{PC_URL}/_nuxt/{chunk}", timeout=30).text
    # Buscar axios calls perto de export/order
    hits = []
    for m in re.finditer(r'this\.\$axios\.[a-z]+\([^)]{0,400}\)', js):
        ctx = m.group()
        if any(x in ctx.lower() for x in ['order', 'export', 'sheet', 'planilha', 'pedido']):
            hits.append(ctx[:300])
    
    # Buscar task_id perto de axios
    for m in re.finditer(r'.{0,300}task_id.{0,300}', js):
        ctx = m.group()
        if 'axios' in ctx or '$axios' in ctx:
            hits.append(f"[task_id] {ctx[:300]}")
    
    if hits:
        print(f"\n=== {chunk} ===")
        for h in hits[:10]:
            print(f"  {h}")
