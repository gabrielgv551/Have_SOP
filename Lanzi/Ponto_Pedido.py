"""
╔══════════════════════════════════════════════════════════════╗
║           S&OP Intelligence · Ponto de Pedido + Semana       ║
║  Fontes    : forecast_12m · estoque_seguranca                ║
║              estoque_consolidado · po · cadastros_sku        ║
║  Banco     : PostgreSQL local (Lanzi)                        ║
║  Output    : ponto_pedido + semana_pedidos                   ║
╚══════════════════════════════════════════════════════════════╝

FÓRMULAS:
PP   = (Demanda Diária × Lead Time) + Estoque de Segurança
QP   = (Demanda Diária × (Lead Time + 30)) + ES - Estoque Atual - Pedidos em Aberto
Dias = (Estoque Atual - PP) ÷ Demanda Diária
Data Pedido  = Hoje se atrasado/urgente, senão Segunda da semana correspondente
Data Entrega = Data Pedido + Lead Time

AGRUPAMENTO SEMANAL:
- Pedidos atrasados ou urgentes → data = hoje
- Pedidos futuros → segunda-feira da semana correspondente
- Salva na tabela semana_pedidos

ALERTAS (considera pedidos em aberto):
- estoque_real = estoque_atual + pedidos_aberto
- SEM MOVIMENTO      → demanda_diaria = 0
- RUPTURA IMINENTE   → estoque_atual <= ES e sem pedido aberto
                       OU estoque_real <= ES (mesmo com pedido, vai faltar)
- PEDIR AGORA        → estoque_real <= PP
- OK                 → estoque_real entre PP e PP×2
- EXCESSO            → estoque_real > PP×2

DEPENDÊNCIAS (rodar ANTES deste script):
  1. UPLOAD_ETL.py         → cria bd_vendas, cadastros_sku, estoque_consolidado, full_1, full_2, po
  2. Curva_ABC.PY          → cria curva_abc
  3. Estoque_Seguranca.py  → cria estoque_seguranca
  4. PREVISÃO 12M.py       → cria forecast_12m
"""

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta

# ─────────────────────────────────────────────
# CONFIGURAÇÃO
# ─────────────────────────────────────────────
DB_CONFIG = {
    "host"    : "37.60.236.200",
    "port"    : 5432,
    "database": "Lanzi",
    "user"    : "postgres",
    "password": "131105Gv",
}

def conectar():
    url = (
        f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )
    engine = create_engine(url)
    print("[OK] Conexão com o banco estabelecida.")
    return engine


def limpar_sku(df: pd.DataFrame, col: str = "sku") -> pd.DataFrame:
    """
    Remove SKUs nulos, em branco e em notação científica.
    Notação científica acontece quando o Excel converte SKUs numéricos longos.
    """
    antes = len(df)
    df = df[df[col].notna()].copy()
    df[col] = df[col].astype(str).str.strip()
    df = df[df[col] != ""]

    # Remove notação científica (ex: 1.87417E+11 → descarta linha)
    df = df[~df[col].str.match(r"^[\d\.]+[Ee][+\-]\d+$")]

    removidos = antes - len(df)
    if removidos > 0:
        print(f"   [i] {removidos} SKUs removidos (nulos, em branco ou notação científica)")
    return df


# ─────────────────────────────────────────────
# 1. PREVISÃO — demanda diária (próximos 3 meses)
#    Fallback: se forecast = 0, usa média mensal histórica
# ─────────────────────────────────────────────
def ler_forecast(engine) -> pd.DataFrame:
    print("\n[...] Lendo forecast (próximos 90 dias)...")
    hoje     = datetime.today()
    data_fim = hoje + timedelta(days=90)

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


# ─────────────────────────────────────────────
# 2. ESTOQUE DE SEGURANÇA + MÉDIA MENSAL (fallback)
# ─────────────────────────────────────────────
def ler_estoque_seguranca(engine) -> pd.DataFrame:
    print("\n[...] Lendo estoque de segurança...")
    query = text("""
        SELECT sku, estoque_seguranca, lead_time, abc_cruzada, confianca, media_mensal
        FROM estoque_seguranca
    """)
    df = pd.read_sql(query, engine)
    df = limpar_sku(df)

    # media_mensal pode vir como texto com vírgula (padrão BR) → converte para float
    if df["media_mensal"].dtype == object:
        df["media_mensal"] = (
            df["media_mensal"]
            .str.replace(",", ".", regex=False)
            .astype(float)
        )

    print(f"[OK] Estoque de segurança carregado: {len(df)} SKUs")
    return df


# ─────────────────────────────────────────────
# 3. ESTOQUE ATUAL (tabela estoque_consolidado)
# ─────────────────────────────────────────────
def ler_estoque_atual(engine) -> pd.DataFrame:
    print("\n[...] Lendo estoque atual (estoque_consolidado)...")
    query = text("""
        SELECT "SKU" AS sku, SUM("Estoque Base") AS estoque_atual
        FROM estoque_consolidado
        WHERE "SKU" IS NOT NULL AND TRIM("SKU") != ''
        GROUP BY "SKU"
    """)
    df = pd.read_sql(query, engine)
    df = limpar_sku(df)

    zerados = (df["estoque_atual"] == 0).sum()
    print(f"[OK] Estoque atual carregado: {len(df)} SKUs  |  {zerados} com estoque zerado")
    return df


# ─────────────────────────────────────────────
# 4. PEDIDOS EM ABERTO (tabela po)
# ─────────────────────────────────────────────
def ler_pedidos_aberto(engine) -> pd.DataFrame:
    print("\n[...] Lendo pedidos em aberto (PO)...")
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


# ─────────────────────────────────────────────
# 5. CALCULAR PONTO DE PEDIDO
# ─────────────────────────────────────────────
def calcular_ponto_pedido(demanda, es, estoque, pedidos) -> pd.DataFrame:
    print("\n[...] Calculando ponto de pedido...")

    df = demanda.merge(es,      on="sku", how="left")
    df = df.merge(estoque,      on="sku", how="left")
    df = df.merge(pedidos,      on="sku", how="left")

    df["pedidos_aberto"]    = df["pedidos_aberto"].fillna(0)
    df["estoque_atual"]     = df["estoque_atual"].fillna(0)
    df["estoque_seguranca"] = df["estoque_seguranca"].fillna(0)
    df["lead_time"]         = df["lead_time"].fillna(30)
    df["media_mensal"]      = df["media_mensal"].fillna(0)

    zerados = (df["estoque_atual"] == 0).sum()
    print(f"   [i] SKUs com estoque_atual = 0 após merge: {zerados} de {len(df)}")
    if zerados > len(df) * 0.8:
        print("   [ATENÇÃO] Mais de 80% dos SKUs com estoque zero.")
        print("             Verifique se a tabela 'estoque_consolidado' foi carregada corretamente.")

    # FALLBACK: forecast zerado mas tem histórico → usa média mensal / 30
    sem_forecast = (df["demanda_diaria"] == 0) & (df["media_mensal"] > 0)
    df.loc[sem_forecast, "demanda_diaria"] = (df.loc[sem_forecast, "media_mensal"] / 30).round(4)
    print(f"   [i] Fallback média mensal aplicado em {sem_forecast.sum()} SKUs")

    hoje = datetime.today()

    # Ponto de Pedido: PP = (Demanda Diária × Lead Time) + ES
    df["ponto_pedido"] = (
        df["demanda_diaria"] * df["lead_time"] + df["estoque_seguranca"]
    ).round(0).astype(int)

    # ── FIX 1: qty_sugerida usa lead_time real em vez de 30 fixo ─────────────
    # Cobre o consumo durante o lead time + 30 dias de ciclo + ES
    # menos o que já tem em estoque e o que já está vindo
    df["qty_sugerida"] = (
        (df["demanda_diaria"] * (df["lead_time"] + 30))
        + df["estoque_seguranca"]
        - df["estoque_atual"]
        - df["pedidos_aberto"]
    ).clip(lower=0).round(0).astype(int)

    # Dias até atingir o PP
    df["dias_ate_pp"] = np.where(
        df["demanda_diaria"] > 0,
        ((df["estoque_atual"] - df["ponto_pedido"]) / df["demanda_diaria"]).round(0),
        999
    )

    # ── FIX 2: data_sugerida_pedido ──────────────────────────────────────────
    # Pedidos atrasados ou com data = hoje → mantém hoje
    # Pedidos futuros → puxa para a segunda-feira da semana correspondente
    def data_para_segunda(dias_ate_pp, lead_time):
        if dias_ate_pp >= 999:
            return None

        dias = max(dias_ate_pp - lead_time, 0)
        data_exata = (hoje + timedelta(days=dias)).date()

        # Atrasado ou hoje → pedido imediato
        if data_exata <= hoje.date():
            return hoje.date()

        # Futuro → segunda da semana correspondente
        segunda = data_exata - timedelta(days=data_exata.weekday())
        return segunda

    df["data_sugerida_pedido"] = df.apply(
        lambda r: data_para_segunda(r["dias_ate_pp"], r["lead_time"]),
        axis=1
    )

    # Data Prevista de Entrega
    df["data_prevista_entrega"] = df.apply(
        lambda r: (hoje + timedelta(days=max(r["dias_ate_pp"], 0) + r["lead_time"])).date()
        if r["dias_ate_pp"] < 999 else None,
        axis=1
    )

    # ── FIX 3: alertas consideram pedidos em aberto ──────────────────────────
    # estoque_real = estoque_atual + pedidos_aberto (o que está a caminho)
    def definir_alerta(row):
        dd           = row["demanda_diaria"]
        ea           = row["estoque_atual"]
        pp           = row["ponto_pedido"]
        es_val       = row["estoque_seguranca"]
        em_transito  = row["pedidos_aberto"]
        estoque_real = ea + em_transito

        if dd == 0:
            return "SEM MOVIMENTO"
        elif ea <= es_val and em_transito == 0:
            # Abaixo do ES e nada chegando → ruptura confirmada
            return "RUPTURA IMINENTE"
        elif estoque_real <= es_val:
            # Mesmo somando o pedido em aberto, não cobre o ES → ainda crítico
            return "RUPTURA IMINENTE"
        elif estoque_real <= pp:
            # Com pedido em aberto ainda fica abaixo do PP → precisa pedir
            return "PEDIR AGORA"
        elif estoque_real <= pp * 2:
            return "OK"
        else:
            return "EXCESSO"

    df["alerta"] = df.apply(definir_alerta, axis=1)

    print(f"\n[OK] Distribuição de alertas:")
    print(df["alerta"].value_counts().to_string())
    return df


# ─────────────────────────────────────────────
# 6. GERAR LISTA SEMANAL
# ─────────────────────────────────────────────
def gerar_semana_pedidos(df: pd.DataFrame) -> pd.DataFrame:
    print("\n[...] Gerando lista semanal...")
    hoje = datetime.today().date()

    # Sexta-feira da semana corrente
    dias_ate_sexta = 4 - datetime.today().weekday()
    if dias_ate_sexta < 0:
        dias_ate_sexta += 7
    sexta   = hoje + timedelta(days=dias_ate_sexta)
    segunda = hoje - timedelta(days=hoje.weekday())

    print(f"   [i] Semana: {segunda.strftime('%d/%m/%Y')} (seg) → {sexta.strftime('%d/%m/%Y')} (sex)")

    datas = pd.to_datetime(df["data_sugerida_pedido"]).dt.date

    # Entra na lista semanal:
    #   → data de pedido até sexta desta semana, OU
    #   → já em alerta crítico (RUPTURA IMINENTE ou PEDIR AGORA)
    #   → excluindo SEM MOVIMENTO
    mascara = (
        (
            (datas.notna() & (datas <= sexta)) |
            (df["alerta"].isin(["RUPTURA IMINENTE", "PEDIR AGORA"]))
        )
        & (df["alerta"] != "SEM MOVIMENTO")
    )
    semana = df[mascara].copy()

    semana["urgencia"] = semana["alerta"].map({
        "RUPTURA IMINENTE": 1,
        "PEDIR AGORA"     : 2,
        "OK"              : 3,
        "EXCESSO"         : 4,
        "SEM MOVIMENTO"   : 5,
    })

    semana["semana_inicio"] = segunda
    semana["semana_fim"]    = sexta
    semana = semana.sort_values(["urgencia", "data_sugerida_pedido"]).reset_index(drop=True)

    print(f"[OK] {len(semana)} SKUs na lista desta semana")
    return semana


# ─────────────────────────────────────────────
# 7. GRAVAR NO POSTGRESQL
# ─────────────────────────────────────────────
def gravar(engine, df: pd.DataFrame, semana: pd.DataFrame):
    print("\n[...] Salvando resultados no banco...")
    hoje = datetime.today().date()

    # --- Tabela ponto_pedido ---
    resultado = df[[
        "sku", "abc_cruzada", "confianca",
        "demanda_diaria", "lead_time",
        "estoque_seguranca", "ponto_pedido",
        "estoque_atual", "pedidos_aberto",
        "qty_sugerida",
        "data_sugerida_pedido", "data_prevista_entrega",
        "alerta"
    ]].copy()

    for col in ["demanda_diaria"]:
        resultado[col] = resultado[col].apply(
            lambda x: f"{x:.4f}".replace(".", ",") if pd.notna(x) else ""
        )

    resultado["data_calculo"] = hoje
    resultado.to_sql("ponto_pedido", engine, if_exists="replace", index=False)
    print(f"[OK] {len(resultado)} SKUs salvos em 'ponto_pedido'")

    # --- Tabela semana_pedidos ---
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

        print(f"\n[i] Prévia da lista semanal ({semana_out['semana_inicio'].iloc[0].strftime('%d/%m/%Y')} → {semana_out['semana_fim'].iloc[0].strftime('%d/%m/%Y')}):")
        print(semana_out[[
            "sku", "estoque_atual", "pedidos_aberto",
            "qty_sugerida", "data_sugerida_pedido", "alerta"
        ]].head(20).to_string(index=False))
    else:
        print("\n[i] Nenhum SKU na lista semanal desta semana.")


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
def main():
    print("=" * 55)
    print("  S&OP Intelligence · Ponto de Pedido + Semana")
    print(f"  Rodando em: {datetime.today().strftime('%d/%m/%Y %H:%M')}")
    print("=" * 55)

    engine  = conectar()
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