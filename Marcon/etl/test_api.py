import os
from dotenv import load_dotenv
import requests

load_dotenv()

# Test the API endpoint directly
API_BASE = 'https://have-gestor-api.vercel.app/api'

# First, try to login to get a token
password = os.getenv('MARCON_PASS_ADMIN')
if not password:
    print('MARCON_PASS_ADMIN not set in .env')
    # Try common passwords
    password = 'admin'  # fallback

login_res = requests.post(f'{API_BASE}/login', json={
    'company': 'marcon',
    'email': 'admin',
    'password': password
})

print(f'Login status: {login_res.status_code}')
if login_res.status_code == 200:
    data = login_res.json()
    token = data.get('token')
    print(f'Got token: {token[:20] if token else "None"}...')
    
    # Now test dashboard_kpis
    kpi_res = requests.get(f'{API_BASE}/data?tabela=dashboard_kpis', headers={
        'Authorization': f'Bearer {token}'
    })
    print(f'KPI status: {kpi_res.status_code}')
    print(f'KPI response: {kpi_res.text[:1000]}')
else:
    print(f'Login failed: {login_res.text}')
