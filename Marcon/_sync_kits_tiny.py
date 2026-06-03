"""
Sincroniza de-para kit -> componentes do Tiny v3 para a tabela sku_kits.
"""
import os, sys, json, time
import requests
from sqlalchemy import create_engine, text

# ── CONFIG ──
DB_CONFIG = {
    "host": "37.60.236.200", "port": 5432,
    "database": "Marcon", "user": "postgres",
    "password": "131105Gv",
}
TINY_API = "https://erp.tiny.com.br/public-api/v3"
TOKEN_URL = "https://accounts.tiny.com.br/realms/tiny/protocol/openid-connect/token"
ACCOUNT = "tiny_marcon"
EMPRESA = "marcon"

engine = create_engine(f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")

# ── 1. Ler credenciais do banco ──
with engine.connect() as conn:
    rows = conn.execute(text(
        "SELECT chave, valor FROM configuracoes WHERE empresa=:e AND (chave LIKE :p OR chave IN ('tiny_client_id','tiny_client_secret'))"
    ), {"e": EMPRESA, "p": ACCOUNT + "%"}).fetchall()
    cfg = {r[0]: r[1] for r in rows}

access_token = cfg.get(ACCOUNT + "_token")
refresh_token = cfg.get(ACCOUNT + "_refresh")
client_id = cfg.get("tiny_client_id", os.getenv("TINY_CLIENT_ID", "")).strip()
client_secret = cfg.get("tiny_client_secret", os.getenv("TINY_CLIENT_SECRET", "")).strip()

if not access_token:
    print("Token não encontrado. Precisa autenticar no Have Gestor primeiro.")
    sys.exit(1)

print(f"Token encontrado (len={len(access_token)}). Client ID: {client_id[:20]}...")

# ── 2. Refresh token ──
token_ok = False
if refresh_token and client_id and client_secret:
    r = requests.post(TOKEN_URL, data={
        "grant_type": "refresh_token",
        "client_id": client_id,
        "client_secret": client_secret,
        "refresh_token": refresh_token,
    })
    if r.ok:
        data = r.json()
        access_token = data["access_token"]
        new_refresh = data.get("refresh_token", refresh_token)
        print("Token refresh OK!")
        token_ok = True
        with engine.begin() as conn:
            from datetime import datetime, timezone
            exp = datetime.now(timezone.utc).isoformat()
            conn.execute(text("""
                INSERT INTO configuracoes (empresa, chave, valor) VALUES (:e, :k1, :v1), (:e, :k2, :v2), (:e, :k3, :v3)
                ON CONFLICT (empresa, chave) DO UPDATE SET valor=EXCLUDED.valor
            """), {
                "e": EMPRESA,
                "k1": ACCOUNT + "_token", "v1": access_token,
                "k2": ACCOUNT + "_refresh", "v2": new_refresh,
                "k3": ACCOUNT + "_exp", "v3": exp,
            })
    else:
        print(f"Refresh falhou: {r.status_code} {r.text[:200]}")

if not token_ok and client_id and client_secret:
    print("Tentando client_credentials...")
    r = requests.post(TOKEN_URL, data={
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
    })
    if r.ok:
        data = r.json()
        access_token = data["access_token"]
        print("client_credentials OK!")
        token_ok = True
        with engine.begin() as conn:
            from datetime import datetime, timezone
            exp = datetime.now(timezone.utc).isoformat()
            conn.execute(text("""
                INSERT INTO configuracoes (empresa, chave, valor) VALUES (:e, :k1, :v1), (:e, :k3, :v3)
                ON CONFLICT (empresa, chave) DO UPDATE SET valor=EXCLUDED.valor
            """), {
                "e": EMPRESA,
                "k1": ACCOUNT + "_token", "v1": access_token,
                "k3": ACCOUNT + "_exp", "v3": exp,
            })
    else:
        print(f"client_credentials falhou: {r.status_code} {r.text[:200]}")

if not token_ok:
    print("Não foi possível obter token válido. Abortando.")
    sys.exit(1)

print(f"Token ativo (len={len(access_token)}). Prosseguindo...\n")

# ── 3. Buscar produtos paginados ──
def tiny_get(path, params=None):
    url = f"{TINY_API}{path}"
    headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}
    r = requests.get(url, headers=headers, params=params or {}, timeout=30)
    if not r.ok:
        print(f"  API erro: {r.status_code} {r.text[:200]}")
        return None
    return r.json()

print("\nBuscando produtos...")
produtos = []
offset = 0
limit = 100
while True:
    data = tiny_get("/produtos", {"limit": limit, "offset": offset})
    if not data:
        break
    itens = data.get("itens") or data.get("data") or []
    produtos.extend(itens)
    total = data.get("paginacao", {}).get("total", 0) if isinstance(data.get("paginacao"), dict) else data.get("total", 0)
    offset += len(itens)
    print(f"  {offset}/{total} produtos...")
    if len(itens) == 0 or offset >= total:
        break

print(f"Total produtos: {len(produtos)}")

# ── 4. Buscar detalhes dos produtos tipo K (kit) ──
kits = []
for p in produtos:
    tid = p.get("id")
    if not tid:
        continue
    det = tiny_get(f"/produtos/{tid}")
    if not det:
        continue
    # Verificar se é kit
    tipo = det.get("tipo")
    if tipo == "K":
        sku_kit = det.get("codigo") or det.get("sku") or ""
        nome = det.get("nome") or det.get("descricao") or ""
        componentes = []
        # Na v3, kit pode vir em diferentes formatos
        kit_data = det.get("kit") or det.get("composicao") or det.get("itens") or []
        if isinstance(kit_data, list):
            for c in kit_data:
                item = c.get("item") or c.get("produto") or c
                sku_comp = item.get("codigo") or item.get("sku") or ""
                nome_comp = item.get("descricao") or item.get("nome") or ""
                qtd = float(item.get("quantidade", 1) or 1)
                if sku_comp:
                    componentes.append({"sku": sku_comp, "nome": nome_comp, "qtd": qtd})
        if componentes:
            kits.append({"sku_kit": sku_kit, "nome_kit": nome, "componentes": componentes})
        else:
            print(f"  Kit {sku_kit} sem componentes detectados")
    time.sleep(0.05)  # rate limit friendly

print(f"\nKits encontrados: {len(kits)}")
for k in kits[:10]:
    print(f"  {k['sku_kit']}: {len(k['componentes'])} componentes")

# ── 5. Gravar em sku_kits ──
if kits:
    with engine.begin() as conn:
        for k in kits:
            for c in k["componentes"]:
                conn.execute(text("""
                    INSERT INTO sku_kits (empresa, sku_kit, sku_componente, quantidade, ativo)
                    VALUES (:e, :sk, :sc, :q, TRUE)
                    ON CONFLICT (empresa, sku_kit, sku_componente) DO UPDATE SET
                        quantidade=EXCLUDED.quantidade, ativo=TRUE
                """), {"e": EMPRESA, "sk": k["sku_kit"], "sc": c["sku"], "q": c["qtd"]})
    print(f"\nGravados {sum(len(k['componentes']) for k in kits)} registros em sku_kits")
else:
    print("Nenhum kit encontrado.")
