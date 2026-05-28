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

# Buscar todos os hashes de chunks no bundle principal
main_js = s.get(f"{PC_URL}/static/bundles/app.a3c5d9f649c0699b3428.js", timeout=30).text
chunks = list(set(re.findall(r'"([a-f0-9]{7})\.js"', main_js)))
print(f"Chunks encontrados no bundle: {len(chunks)}")

# Buscar em TODOS os chunks por "export" + ("order" ou "task_id") perto de axios
print("Buscando endpoint de export em todos os chunks...")
found_endpoint = []
for i, chunk in enumerate(chunks):
    try:
        url = f"{PC_URL}/_nuxt/{chunk}.js"
        r = s.get(url, timeout=8)
        if r.status_code != 200:
            continue
        js = r.text
        # Buscar axios calls com order/export
        for m in re.finditer(r'axios\.[a-z]+\([`"\']([^`"\']+)[`"\']', js, re.IGNORECASE):
            path = m.group(1)
            if any(x in path.lower() for x in ['order', 'export', 'sheet', 'planilha']):
                ctx = js[max(0, m.start()-50):m.end()+200]
                found_endpoint.append(f"{chunk}: {path} | ctx: {ctx[:200]}")
        # Buscar $axios perto de order/task_id
        for m in re.finditer(r'\$axios\.[a-z]+\([`"\']([^`"\']+)[`"\']', js):
            path = m.group(1)
            if any(x in path.lower() for x in ['order', 'export', 'sheet', 'planilha']):
                ctx = js[max(0, m.start()-50):m.end()+200]
                found_endpoint.append(f"{chunk}: {path} | ctx: {ctx[:200]}")
    except:
        pass
    if i % 20 == 0:
        print(f"  {i}/{len(chunks)} chunks verificados...")

print(f"\nEndpoints encontrados: {len(found_endpoint)}")
for ep in found_endpoint:
    print(f"  {ep}")
