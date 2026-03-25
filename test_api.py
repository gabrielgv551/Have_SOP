import urllib.request
import json
import urllib.error

url = 'https://have-gestor-api.vercel.app/api/login'
data = json.dumps({'email': 'admin', 'password': 'lanzi2024'}).encode('utf-8')
req = urllib.request.Request(url, data=data, headers={'Content-Type': 'application/json'})

try:
    with urllib.request.urlopen(req) as response:
        result = json.loads(response.read().decode('utf-8'))
        print("LOGIN SUCESSO:")
        print(result)
        
        # Test admin/usuarios
        if 'token' in result:
            token = result['token']
            req2 = urllib.request.Request('https://have-gestor-api.vercel.app/api/admin/usuarios', headers={'Authorization': f'Bearer {token}'})
            try:
                with urllib.request.urlopen(req2) as res2:
                    print("USUARIOS:")
                    print(res2.read().decode('utf-8'))
            except urllib.error.HTTPError as e:
                print("ERRO USUARIOS:", e.code, e.read().decode('utf-8'))
                
except urllib.error.HTTPError as e:
    print("ERRO LOGIN:", e.code, e.read().decode('utf-8'))
