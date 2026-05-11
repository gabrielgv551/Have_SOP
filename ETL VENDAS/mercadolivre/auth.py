"""
  Mercado Livre — Autenticação OAuth 2.0 (server-side)
=======================================================
USO:
  1. Rode: python mercadolivre/auth.py
  2. Abrirá o browser no link de autorização do ML
  3. Logue com a conta do vendedor e autorize
  4. O ML redireciona para a URL de callback com ?code=...
  5. Copie o valor do parâmetro "code" da URL e cole aqui
  6. Os tokens serão salvos em tokens/ml_tokens.json

RENOVAÇÃO AUTOMÁTICA:
  O arquivo tokens/ml_tokens.json é atualizado automaticamente
  sempre que o access_token expirar (a cada 6h).
"""

import os
import json
import time
import webbrowser
import urllib.parse
import requests
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

CLIENT_ID     = os.getenv("ML_CLIENT_ID")
CLIENT_SECRET = os.getenv("ML_CLIENT_SECRET")
REDIRECT_URI  = os.getenv("ML_REDIRECT_URI", "https://have-gestor-frontend.vercel.app/ml-callback")
TOKENS_FILE   = Path(__file__).parent.parent / "tokens" / "ml_tokens.json"

ML_AUTH_URL  = "https://auth.mercadolivre.com.br/authorization"
ML_TOKEN_URL = "https://api.mercadolivre.com/oauth/token"


# ─────────────────────────────────────────────────────────────
# SALVAR / CARREGAR TOKENS
# ─────────────────────────────────────────────────────────────
def salvar_tokens(data: dict):
    TOKENS_FILE.parent.mkdir(exist_ok=True)
    data["saved_at"] = int(time.time())
    with open(TOKENS_FILE, "w") as f:
        json.dump(data, f, indent=2)
    print(f"  ✔  Tokens salvos em {TOKENS_FILE}")


def carregar_tokens() -> dict | None:
    if not TOKENS_FILE.exists():
        return None
    with open(TOKENS_FILE) as f:
        return json.load(f)


# ─────────────────────────────────────────────────────────────
# CHECAR SE TOKEN AINDA É VÁLIDO
# ─────────────────────────────────────────────────────────────
def token_valido(tokens: dict) -> bool:
    saved_at   = tokens.get("saved_at", 0)
    expires_in = tokens.get("expires_in", 21600)
    return (time.time() - saved_at) < (expires_in - 300)  # 5min de margem


# ─────────────────────────────────────────────────────────────
# RENOVAR ACCESS TOKEN
# ─────────────────────────────────────────────────────────────
def renovar_token(refresh_token: str) -> dict:
    resp = requests.post(ML_TOKEN_URL, data={
        "grant_type":    "refresh_token",
        "client_id":     CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "refresh_token": refresh_token,
    }, timeout=15)
    resp.raise_for_status()
    tokens = resp.json()
    salvar_tokens(tokens)
    return tokens


# ─────────────────────────────────────────────────────────────
# OBTER ACCESS TOKEN (renovando se necessário)
# ─────────────────────────────────────────────────────────────
def get_access_token() -> str:
    tokens = carregar_tokens()
    if not tokens:
        raise RuntimeError("Tokens não encontrados. Rode python ml_auth.py primeiro.")

    if token_valido(tokens):
        return tokens["access_token"]

    print("  [!] Token expirado — renovando automaticamente...")
    tokens = renovar_token(tokens["refresh_token"])
    return tokens["access_token"]


# ─────────────────────────────────────────────────────────────
# FLUXO INICIAL — PRIMEIRA AUTORIZAÇÃO
# ─────────────────────────────────────────────────────────────
def autorizar():
    params = {
        "response_type": "code",
        "client_id":     CLIENT_ID,
        "redirect_uri":  REDIRECT_URI,
    }
    url = ML_AUTH_URL + "?" + urllib.parse.urlencode(params)

    print("\n" + "═" * 62)
    print("  Mercado Livre — Autorização OAuth")
    print("═" * 62)
    print(f"\n  Abrindo o browser para autorização...\n")
    print(f"  URL: {url}\n")

    webbrowser.open(url)

    print("  Após autorizar, o ML vai redirecionar para:")
    print(f"  {REDIRECT_URI}?code=XXXXXX\n")
    print("  Copie APENAS o valor do 'code' da URL e cole abaixo:")

    code = input("  code= ").strip()
    if not code:
        print("  [ERRO] Nenhum código informado.")
        return

    print("\n  Trocando code por tokens...")
    resp = requests.post(ML_TOKEN_URL, data={
        "grant_type":    "authorization_code",
        "client_id":     CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "code":          code,
        "redirect_uri":  REDIRECT_URI,
    }, timeout=15)

    if resp.status_code != 200:
        print(f"  [ERRO] {resp.status_code}: {resp.text}")
        return

    tokens = resp.json()
    salvar_tokens(tokens)

    print(f"\n  ✔  Autorizado! User ID: {tokens.get('user_id')}")
    print(f"  Access Token: {tokens['access_token'][:20]}...")
    print(f"  Expira em: {tokens.get('expires_in', '?')}s (~6h)\n")


# ─────────────────────────────────────────────────────────────
# TESTE RÁPIDO DA CONEXÃO
# ─────────────────────────────────────────────────────────────
def testar_conexao():
    token = get_access_token()
    tokens = carregar_tokens()
    user_id = tokens.get("user_id")

    resp = requests.get(
        f"https://api.mercadolivre.com/users/{user_id}",
        headers={"Authorization": f"Bearer {token}"},
        timeout=10
    )
    if resp.status_code == 200:
        u = resp.json()
        print(f"  ✔  Conectado como: {u.get('nickname')} ({u.get('email')})")
    else:
        print(f"  [ERRO] {resp.status_code}: {resp.text}")


if __name__ == "__main__":
    tokens = carregar_tokens()
    if tokens:
        print("  Tokens existentes encontrados.")
        testar_conexao()
    else:
        autorizar()
