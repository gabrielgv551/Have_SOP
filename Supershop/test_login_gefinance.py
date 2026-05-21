"""Testa login no Gefinance e mostra a resposta completa."""
import requests, json

EMAIL    = "financeiro@supershop.com.br"
PASSWORD = "1893210aB@"

resp = requests.post(
    "https://gateway-web.ge.finance/api/Auth/login",
    json={"username": EMAIL, "password": PASSWORD},
    headers={
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Origin": "https://app.ge.finance",
        "language": "pt-BR",
    },
    timeout=20,
)

print(f"Status: {resp.status_code}")
print(json.dumps(resp.json(), indent=2, ensure_ascii=False))
