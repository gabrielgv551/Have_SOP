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

# Buscar a pagina de pedidos para ver quais chunks carrega
page_html = s.get(f"{PC_URL}/v2/orders/", headers=h, timeout=15).text

# Salvar HTML para inspecao
with open("orders_page.html", "w", encoding="utf-8") as f:
    f.write(page_html)
print("HTML salvo em orders_page.html")

# Buscar todos scripts referenciados
scripts = re.findall(r'src=["\']([^"\']+\.js)["\']', page_html)
nuxt_scripts = [u for u in scripts if '_nuxt' in u or 'static' in u or 'bundles' in u]
print(f"\nScripts encontrados: {len(nuxt_scripts)}")
for sc in nuxt_scripts[:10]:
    print(f"  {sc}")

# Buscar em TODOS os chunks por "export" ou "sheet" ou "amazonaws"
print("\nBuscando em chunks...")
all_found = []
for sc in nuxt_scripts:
    url = sc if sc.startswith("http") else f"{PC_URL}{sc}"
    try:
        js = s.get(url, timeout=15).text
        # Buscar patterns de API endpoint
        hits = re.findall(r'["`\']([^"`\' ]{5,80}(?:export|sheet|download|xlsx|s3)[^"`\' ]{0,40})["`\']', js, re.IGNORECASE)
        if hits:
            print(f"\n  {sc.split('/')[-1]}:")
            for h2 in sorted(set(hits))[:10]:
                print(f"    {h2}")
            all_found.extend(hits)
    except:
        pass

print(f"\nTotal hits: {len(set(all_found))}")
