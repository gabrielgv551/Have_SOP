import os
from dotenv import load_dotenv
import requests

load_dotenv()

# Test the API endpoint directly
API_BASE = 'https://have-gestor-api.vercel.app/api'

# Try with different users
test_users = [
    ('admin', os.getenv('MARCON_PASS_ADMIN', 'admin')),
    ('gestor', os.getenv('MARCON_PASS_GESTOR', 'gestor')),
    ('have', os.getenv('MARCON_PASS_HAVE', 'have')),
]

for username, password in test_users:
    print(f'\n--- Tentando login com usuário: {username} ---')
    login_res = requests.post(f'{API_BASE}/login', json={
        'company': 'marcon',
        'email': username,
        'password': password
    })
    
    print(f'Status: {login_res.status_code}')
    if login_res.status_code == 200:
        data = login_res.json()
        token = data.get('token')
        print(f'✅ Login bem-sucedido! Token: {token[:20] if token else "None"}...')
        
        # Test dashboard_kpis
        kpi_res = requests.get(f'{API_BASE}/data?tabela=dashboard_kpis', headers={
            'Authorization': f'Bearer {token}'
        })
        print(f'KPI status: {kpi_res.status_code}')
        if kpi_res.status_code == 200:
            print(f'✅ KPI response: {kpi_res.json()}')
        else:
            print(f'❌ KPI error: {kpi_res.text}')
        break
    else:
        print(f'❌ Login failed: {login_res.text}')
