"""
╔══════════════════════════════════════════════════════════════╗
║           S&OP Intelligence · Ponto de Pedido + Semana       ║
║  Fontes    : forecast_12m · estoque_seguranca                ║
║              estoque_consolidado · po · cadastros_sku        ║
║  Banco     : PostgreSQL local (Autoequip)                    ║
║  Output    : ponto_pedido + semana_pedidos                   ║
╚══════════════════════════════════════════════════════════════╝
"""

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import os
from pathlib import Path

# ─── Carrega .env da raiz ────────────────────────────────────────
_root = Path(__file__).resolve().parent.parent.parent
_env_path = _root / ".env"
if _env_path.exists():
    try:
        from dotenv import load_dotenv
        load_dotenv(str(_env_path))
    except ImportError:
        with open(_env_path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    k, _, v = line.partition('=')
                    os.environ.setdefault(k.strip(), v.strip())

# ─────────────────────────────────────────────────────────────────
# CONFIGURAÇÃO
# ─────────────────────────────────────────────────────────────────
DB_CONFIG = {
    "host"    : os.getenv("AUTOEQUIP_HOST",     "37.60.236.200"),
    "port"    : int(os.getenv("AUTOEQUIP_PORT", "5432")),
    "database": os.getenv("AUTOEQUIP_DB",       "Autoequip"),
    "user"    : os.getenv("AUTOEQUIP_USER",     "postgres"),
    "password": os.getenv("AUTOEQUIP_PASSWORD", ""),
}

# ── Defaults (substituídos pelo banco se sopc_config existir) ────────────────
HORIZONTE_DEMANDA_DIAS = 90
CICLO_REPOSICAO_DIAS   = 30
FATOR_EXCESSO          = 2.0


def ler_config(engine, modulo: str) -> dict:
    """Lê sopc_config do banco; retorna {chave: valor}. Silencioso se tabela não existe."""
    try:
        import pandas as _pd
        df = _pd.read_sql(
            f"SELECT chave, valor FROM sopc_config WHERE empresa='autoequip' AND modulo='{modulo}'",
            engine
        )
        return dict(zip(df["chave"], df["valor"]))
    except Exception:
        return {}


def aplicar_config(engine):
    global HORIZONTE_DEMANDA_DIAS, CICLO_REPOSICAO_DIAS, FATOR_EXCESSO
    cfg = ler_config(engine, 'ponto_pedido')
    if not cfg:
        return
    if 'horizonte_demanda_dias' in cfg:
        HORIZONTE_DEMANDA_DIAS = int(cfg['horizonte_demanda_dias'])
    if 'ciclo_reposicao_dias' in cfg:
        CICLO_REPOSICAO_DIAS = int(cfg['ciclo_reposicao_dias'])
    if 'fator_excesso' in cfg:
        FATOR_EXCESSO = float(cfg['fator_excesso'])
    print(f"[CFG] Ponto Pedido: horizonte={HORIZONTE_DEMANDA_DIAS}d, ciclo={CICLO_REPOSICAO_DIAS}d, excesso=x{FATOR_EXCESSO}")


def conectar():
    url = (
        f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )
    engine = create_engine(url)
    print("[OK] Conexão com o banco estabelecida.")
    return engine


def limpar_sku(df: pd.DataFrame, col: str = "sku") -> pd.DataFrame:
    antes = len(df)
    df = df[df[col].notna()].copy()
    df[col] = df[col].astype(str).str.strip()
    df = df[df[col] != ""]
    df = df[~df[col].str.match(r"^[\d\.]+[Ee][+\-]\d+$")]
    removidos = antes - len(df)
    if removidos > 0:
        print(f"   [i] {removidos} SKUs removidos (nulos, em branco ou notação científica)")
    return df


# ─────────────────────────────────────────────────────────────────
# 1. PREVISÃO — demanda diária (próximos 3 meses)
# ─────────────────────────────────────────────────────────────────
def ler_forecast(engine) -> pd.DataFrame:
    print("\n[...] Lendo forecast (próximos 90 dias)...")
    hoje     = datetime.today()
    data_fim = hoje + timedelta(days=HORIZONTE_DEMANDA_DIAS)

    query = text("""
        SELECT "Sku" AS sku, "Previsao_Quantidade" AS previsao_quantidade
        FROM forecast_12m
        WHERE "Data" >= :hoje AND "Data" <= :data_fim
    """)

    df = pd.read_sql(query, engine, params={"hoje": hoje, "data_fim": data_fim})
    df = limpar_sku(df)

    demanda = (
        df.groupby("sku")["previsao_quantidade"]
        .sum().reset_index()
        .rename(columns={"previsao_quantidade": "demanda_90d"})
    )
    demanda["demanda_diaria"] = (demanda["demanda_90d"] / 90).round(4)
    print(f"[OK] Demanda diária calculada para {len(demanda)} SKUs")
    return demanda[["sku", "demanda_diaria"]]


# ─────────────────────────────────────────────────────────────────
# 2. ESTOQUE DE SEGURANÇA
# ─────────────────────────────────────────────────────────────────
def ler_estoque_seguranca(engine) -> pd.DataFrame:
    print("\n[...] Lendo estoque de segurança...")
    query = text("""
        SELECT sku, estoque_seguranca, lead_time, abc_cruzada, confianca, media_mensal
        FROM estoque_seguranca
    """)
    df = pd.read_sql(query, engine)
    df = limpar_sku(df)

    if df["media_mensal"].dtype == object:
        df["media_mensal"] = (
            df["media_mensal"]
            .str.replace(",", ".", regex=False)
            .astype(float)
        )

    print(f"[OK] Estoque de segurança carregado: {len(df)} SKUs")
    return df


# ─────────────────────────────────────────────────────────────────
# 3. ESTOQUE ATUAL
# ─────────────────────────────────────────────────────────────────
def ler_estoque_atual(engine) -> pd.DataFrame:
    print("\n[...] Lendo estoque atual (estoque_consolidado)...")
    query = text("""
        SELECT sku, SUM(estoque_base) AS estoque_atual
        FROM estoque_consolidado
        WHERE sku IS NOT NULL AND TRIM(sku) != ''
        GROUP BY sku
    """)
    df = pd.read_sql(query, engine)
    df = limpar_sku(df)

    zerados = (df["estoque_atual"] == 0).sum()
    print(f"[OK] Estoque atual carregado: {len(df)} SKUs  |  {zerados} com estoque zerado")
    return df


# ─────────────────────────────────────────────────────────────────
# 4. PEDIDOS EM ABERTO (tabela po)
# ─────────────────────────────────────────────────────────────────
def ler_pedidos_aberto(engine) -> pd.DataFrame:
    print("\n[...] Lendo pedidos em aberto (PO)...")
    try:
        query = text("""
            SELECT
                "SKU"                   AS sku,
                SUM("Quantidade")       AS pedidos_aberto,
                MAX("Previsao_Entrega") AS entrega_pedido_aberto
            FROM po
            WHERE "SKU" IS NOT NULL
            GROUP BY "SKU"
        """)
        df = pd.read_sql(query, engine)
        df = limpar_sku(df)
        print(f"[OK] Pedidos em aberto: {len(df)} SKUs")
        return df
    except Exception as e:
        print(f"   [AVISO] Tabela 'po' não encontrada — ignorando pedidos em aberto. ({e})")
        return pd.DataFrame(columns=["sku", "pedidos_aberto", "entrega_pedido_aberto"])


# ─────────────────────────────────────────────────────────────────
# 5. CALCULAR PONTO DE PEDIDO
# ─────────────────────────────────────────────────────────────────
def calcular_ponto_pedido(demanda, es, estoque, pedidos) -> pd.DataFrame:
    print("\n[...] Calculando ponto de pedido...")

    df = demanda.merge(es,      on="sku", how="left")
    df = df.merge(estoque,      on="sku", how="left")
    df = df.merge(pedidos,      on="sku", how="left")

    df["pedidos_aberto"]    = df["pedidos_aberto"].fillna(0)
    df["sem_dados_estoque"] = df["estoque_atual"].isna()
    df["estoque_atual"]     = df["estoque_atual"].fillna(0)
    df["estoque_seguranca"] = df["estoque_seguranca"].fillna(0)
    df["lead_time"]         = df["lead_time"].where(df["lead_time"] > 0).fillna(30)
    df["media_mensal"]      = df["media_mensal"].fillna(0)

    sem_forecast = (df["demanda_diaria"] == 0) & (df["media_mensal"] > 0)
    df.loc[sem_forecast, "demanda_diaria"] = (df.loc[sem_forecast, "media_mensal"] / 30).round(4)

    hoje = datetime.today()

    df["ponto_pedido"] = (
        df["demanda_diaria"] * df["lead_time"] + df["estoque_seguranca"]
    ).round(0).astype(int)

    df["qty_sugerida"] = (
        (df["demanda_diaria"] * (df["lead_time"] + CICLO_REPOSICAO_DIAS))
        + df["estoque_seguranca"]
        - df["estoque_atual"]
        - df["pedidos_aberto"]
    ).clip(lower=0).round(0).astype(int)

    df["dias_ate_pp"] = np.where(
        df["demanda_diaria"] > 0,
        ((df["estoque_atual"] - df["ponto_pedido"]) / df["demanda_diaria"]).round(0),
        999
    )

    def data_para_segunda(dias_ate_pp, lead_time):
        if dias_ate_pp >= 999:
            return None
        dias = max(dias_ate_pp - lead_time, 0)
        data_exata = (hoje + timedelta(days=dias)).date()
        if data_exata <= hoje.date():
            return hoje.date()
        segunda = data_exata - timedelta(days=data_exata.weekday())
        return segunda

    df["data_sugerida_pedido"] = df.apply(
        lambda r: data_para_segunda(r["dias_ate_pp"], r["lead_time"]), axis=1
    )

    df["data_prevista_entrega"] = df.apply(
        lambda r: (hoje + timedelta(days=max(r["dias_ate_pp"], 0) + r["lead_time"])).date()
        if r["dias_ate_pp"] < 999 else None,
        axis=1
    )

    def definir_alerta(row):
        dd           = row["demanda_diaria"]
        ea           = row["estoque_atual"]
        pp           = row["ponto_pedido"]
        es_val       = row["estoque_seguranca"]
        em_transito  = row["pedidos_aberto"]
        estoque_real = ea + em_transito
        sem_dados    = row["sem_dados_estoque"]

        if dd == 0:            return "SEM MOVIMENTO"
        if sem_dados:          return "SEM DADOS"
        elif ea <= es_val and em_transito == 0:
            return "RUPTURA IMINENTE"
        elif estoque_real <= es_val:
            return "RUPTURA IMINENTE"
        elif estoque_real <= pp:
            return "PEDIR AGORA"
        elif estoque_real <= pp * FATOR_EXCESSO:
            return "OK"
        else:
            return "EXCESSO"

    df["alerta"] = df.apply(definir_alerta, axis=1)

    sem_acao = df["alerta"].isin(["OK", "EXCESSO", "SEM MOVIMENTO", "SEM DADOS"])
    df.loc[sem_acao, "qty_sugerida"] = 0
    df.loc[sem_acao, "data_sugerida_pedido"] = None

    print(f"\n[OK] Distribuição de alertas:")
    print(df["alerta"].value_counts().to_string())
    return df


# ─────────────────────────────────────────────────────────────────
# 6. GERAR LISTA SEMANAL
# ─────────────────────────────────────────────────────────────────
def gerar_semana_pedidos(df: pd.DataFrame) -> pd.DataFrame:
    print("\n[...] Gerando lista semanal...")
    hoje = datetime.today().date()

    dias_ate_sexta = 4 - datetime.today().weekday()
    if dias_ate_sexta < 0:
        dias_ate_sexta += 7
    sexta   = hoje + timedelta(days=dias_ate_sexta)
    segunda = hoje - timedelta(days=hoje.weekday())

    datas = pd.to_datetime(df["data_sugerida_pedido"]).dt.date

    mascara = (
        (
            (datas.notna() & (datas <= sexta)) |
            (df["alerta"].isin(["RUPTURA IMINENTE", "PEDIR AGORA"]))
        )
        & (~df["alerta"].isin(["SEM MOVIMENTO", "SEM DADOS", "KIT"]))
    )
    semana = df[mascara].copy()

    semana["urgencia"] = semana["alerta"].map({
        "RUPTURA IMINENTE": 1,
        "PEDIR AGORA"     : 2,
        "OK"              : 3,
        "EXCESSO"         : 4,
        "SEM MOVIMENTO"   : 5,
        "SEM DADOS"       : 6,
        "KIT"             : 7,
    })

    semana["semana_inicio"] = segunda
    semana["semana_fim"]    = sexta
    semana = semana.sort_values(["urgencia", "data_sugerida_pedido"]).reset_index(drop=True)

    print(f"[OK] {len(semana)} SKUs na lista desta semana")
    return semana


# ─────────────────────────────────────────────────────────────────
# 7. GRAVAR NO POSTGRESQL
# ─────────────────────────────────────────────────────────────────
def gravar(engine, df: pd.DataFrame, semana: pd.DataFrame):
    print("\n[...] Salvando resultados no banco...")
    hoje = datetime.today().date()

    resultado = df[[
        "sku", "abc_cruzada", "confianca",
        "demanda_diaria", "lead_time",
        "estoque_seguranca", "ponto_pedido",
        "estoque_atual", "pedidos_aberto",
        "qty_sugerida",
        "data_sugerida_pedido", "data_prevista_entrega",
        "alerta", "sem_dados_estoque"
    ]].copy()

    for col in ["demanda_diaria"]:
        resultado[col] = resultado[col].apply(
            lambda x: f"{x:.4f}".replace(".", ",") if pd.notna(x) else ""
        )

    resultado["data_calculo"] = hoje
    resultado.to_sql("ponto_pedido", engine, if_exists="replace", index=False)
    print(f"[OK] {len(resultado)} SKUs salvos em 'ponto_pedido'")

    if len(semana) > 0:
        semana_out = semana[[
            "sku", "abc_cruzada",
            "estoque_atual", "ponto_pedido", "estoque_seguranca",
            "pedidos_aberto", "entrega_pedido_aberto",
            "qty_sugerida", "demanda_diaria",
            "data_sugerida_pedido", "data_prevista_entrega",
            "alerta", "semana_inicio", "semana_fim"
        ]].copy()

        for col in ["demanda_diaria"]:
            semana_out[col] = semana_out[col].apply(
                lambda x: f"{x:.4f}".replace(".", ",") if pd.notna(x) and not isinstance(x, str) else x
            )

        semana_out["data_calculo"] = hoje
        semana_out.to_sql("semana_pedidos", engine, if_exists="replace", index=False)
        print(f"[OK] {len(semana_out)} SKUs salvos em 'semana_pedidos'")
    else:
        print("\n[i] Nenhum SKU na lista semanal desta semana.")


# ─────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────
def main():
    print("=" * 55)
    print("  S&OP Intelligence · Ponto de Pedido + Semana  ·  Autoequip")
    print(f"  Rodando em: {datetime.today().strftime('%d/%m/%Y %H:%M')}")
    print("=" * 55)

    engine  = conectar()
    aplicar_config(engine)
    demanda = ler_forecast(engine)
    es      = ler_estoque_seguranca(engine)
    estoque = ler_estoque_atual(engine)
    pedidos = ler_pedidos_aberto(engine)
    df      = calcular_ponto_pedido(demanda, es, estoque, pedidos)
    semana  = gerar_semana_pedidos(df)
    gravar(engine, df, semana)

    print("\n" + "=" * 55)
    print("[OK] Ponto de Pedido + Lista Semanal finalizados!")
    print("=" * 55)


if __name__ == "__main__":
    main()
