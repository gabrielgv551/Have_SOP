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

# Buscar toda chamada axios perto de "trackExportSheetTaskOrder" ou "EXPORTING_ORDERS"
print("=== Chamadas axios/get/post perto de exportação de orders ===")
for m in re.finditer(r'.{0,500}trackExportSheetTaskOrder.{0,500}', js):
    print(m.group()[:800])
    print("---")

print("\n=== axios calls com task_id ===")
for m in re.finditer(r'axios\.[a-z]+\([^)]{0,200}task_id[^)]{0,200}\)', js, re.IGNORECASE):
    print(f"  {m.group()[:300]}")

print("\n=== this.\$axios perto de order/export ===")
for m in re.finditer(r'this\.\$axios\.[a-z]+\([^)]{0,300}\)', js):
    ctx = m.group()
    if any(x in ctx.lower() for x in ['order', 'export', 'sheet', 'planilha']):
        print(f"  {ctx[:300]}")
