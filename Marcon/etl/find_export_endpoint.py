import requests, re

s = requests.Session()
r = s.get('https://sys.precocerto.co/login/')
csrf = s.cookies.get('csrftoken', '')
s.post('https://sys.precocerto.co/login/',
       data={'username_login': 'comercial@casaeletromarcon.com.br',
             'password_login': 'eletro123',
             'csrfmiddlewaretoken': csrf},
       headers={'Referer': 'https://sys.precocerto.co/login/'})
sessionid = s.cookies.get('sessionid', '')
csrf2 = s.cookies.get('csrftoken', csrf)
h = {'Cookie': f'sessionid={sessionid}; csrftoken={csrf2}'}

# Buscar chunks Nuxt da pagina de orders
page = s.get('https://sys.precocerto.co/v2/orders/', headers=h, timeout=15)
chunks = list(set(re.findall(r'/_nuxt/([a-f0-9]+\.js)', page.text)))
print(f'Chunks: {len(chunks)}')

found = []
for chunk in chunks:
    url = f'https://sys.precocerto.co/_nuxt/{chunk}'
    try:
        js = s.get(url, headers=h, timeout=10).text
        hits = re.findall(r'["\'/]([^"\'/ ]*(?:export|sheet|download|xlsx)[^"\'/ ]*)["\'/]', js, re.IGNORECASE)
        if hits:
            print(f'  {chunk}: {hits[:5]}')
            found.extend(hits)
    except Exception as e:
        print(f'  {chunk}: ERRO {e}')

print('\nTodos hits:')
for h2 in sorted(set(found)):
    print(f'  {h2}')
