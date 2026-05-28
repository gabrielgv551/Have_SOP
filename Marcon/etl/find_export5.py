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

chunks = ["e1b0c1b.js", "cb7f898.js", "3ef0f1f.js", "4dcf3a1.js"]
for chunk in chunks:
    url = f"{PC_URL}/_nuxt/{chunk}"
    js = s.get(url, timeout=15).text
    print(f"\n=== {chunk} ({len(js):,} chars) ===")
    
    # API paths
    api_paths = re.findall(r'["`\'](/(?:api|gerenciar)[^"`\'\s\\]{3,100})["`\']', js)
    if api_paths:
        print("  API paths:")
        for p in sorted(set(api_paths)):
            print(f"    {p}")
    
    # Contexto export/sheet/planilha
    for pat in [r'export', r'sheet', r'planilha', r'xlsx', r'download']:
        hits = [m.group() for m in re.finditer(rf'.{{0,80}}{pat}.{{0,80}}', js, re.IGNORECASE)]
        if hits:
            print(f"  [{pat}] {len(hits)} hits:")
            for h2 in hits[:5]:
                cleaned = h2.replace('\n', ' ').strip()
                print(f"    {cleaned}")
