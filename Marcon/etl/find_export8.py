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

# Buscar onde o task_id é passado para trackExportSheetTaskOrder
print("=== dispatch trackExportSheetTaskOrder ===")
for m in re.finditer(r'.{0,400}trackExportSheetTaskOrder[^}]{0,400}', js):
    ctx = m.group()
    if 'task_id' in ctx or 'dispatch' in ctx or 'axios' in ctx:
        print(ctx[:600])
        print("---")

# Buscar qualquer axios call que esteja na vizinhança de planilha/export
print("\n=== axios calls 500 chars antes de trackExportSheetTaskOrder ===")
pos = 0
while True:
    idx = js.find("trackExportSheetTaskOrder", pos)
    if idx < 0:
        break
    snippet = js[max(0, idx-600):idx+200]
    if '$axios' in snippet or 'axios.get' in snippet or 'axios.post' in snippet:
        print(snippet)
        print("====")
    pos = idx + 1
