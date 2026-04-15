"""
╔══════════════════════════════════════════════════════════════╗
║           S&OP Intelligence · Forecast Diário                ║
║  Desagrega forecast mensal → quantidades diárias por SKU     ║
╚══════════════════════════════════════════════════════════════╝

DESCRIÇÃO:
  Lê o forecast mensal (forecast_12m) e o histórico de vendas
  (bd_vendas) para desagregar as quantidades mensais em
  quantidades diárias usando dois padrões históricos:
    1. Padrão semanal intra-mensal: % de vendas por semana do mês
       (sem1=dias 1-7, sem2=8-14, sem3=15-21, sem4=22-fim)
    2. Padrão dia-da-semana: % de vendas por dia da semana (seg-dom)

DEPENDÊNCIAS (rodar ANTES deste script):
  3. PREVISÃO 12M.py  → cria forecast_12m
  1. GEFINANCE_ETL.py → cria bd_vendas

OUTPUT:
  Tabela: forecast_diario
  Colunas:
    sku, canal, data, semana_do_mes,
    quantidade_prevista, data_calculo
"""

import calendar
from datetime import datetime, date

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

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

JANELA_HISTORICO_MESES = 12
MIN_MESES_PADRAO_PROPRIO = 3


# ─────────────────────────────────────────────
# CONEXÃO
# ─────────────────────────────────────────────
def conectar():
    url = (
        f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )
    return create_engine(url)


# ─────────────────────────────────────────────
# 1. LER HISTÓRICO DE VENDAS
# ─────────────────────────────────────────────
def ler_historico(engine) -> pd.DataFrame:
    print("\n[...] Lendo histórico de vendas...")
    hoje = datetime.today()
    data_corte = pd.Timestamp(hoje.year, hoje.month, 1) - pd.DateOffset(months=JANELA_HISTORICO_MESES)

    query = text("""
        SELECT
            TRIM("Sku"::text) AS sku,
            COALESCE(NULLIF(TRIM("Canal Apelido"::text), ''), TRIM("Canal de venda"::text), 'N/A') AS canal,
            "Data"::date AS data_venda,
            COALESCE("Quantidade Vendida"::numeric, 0) AS quantidade
        FROM bd_vendas
        WHERE "Status" !~* '(cancel|devol|n[aã]o.?pago)'
          AND "Data" >= :data_corte
          AND "Sku" IS NOT NULL
          AND TRIM("Sku"::text) != ''
    """)

    df = pd.read_sql(query, engine, params={"data_corte": data_corte})
    df["data_venda"] = pd.to_datetime(df["data_venda"])
    df["quantidade"] = pd.to_numeric(df["quantidade"], errors="coerce").fillna(0)
    print(f"[OK] {len(df)} registros lidos desde {data_corte.date()}")
    return df


# ─────────────────────────────────────────────
# 2. PADRÃO SEMANAL INTRA-MENSAL
#    semana 1 = dias 1-7 | sem 2 = 8-14 | sem 3 = 15-21 | sem 4 = 22-fim
# ─────────────────────────────────────────────
def _semana_do_mes(day: int) -> int:
    if day <= 7:
        return 1
    if day <= 14:
        return 2
    if day <= 21:
        return 3
    return 4


def calcular_padrao_semanal(df: pd.DataFrame):
    print("\n[...] Calculando padrão semanal intra-mensal...")

    df = df.copy()
    df["mes"]       = df["data_venda"].dt.to_period("M")
    df["semana"]    = df["data_venda"].dt.day.apply(_semana_do_mes)
    df["unique_id"] = df["sku"] + "§§" + df["canal"]

    agg = (
        df.groupby(["unique_id", "mes", "semana"])["quantidade"]
        .sum()
        .reset_index()
    )

    total_mes = (
        agg.groupby(["unique_id", "mes"])["quantidade"]
        .sum()
        .rename("total_mes")
        .reset_index()
    )
    agg = agg.merge(total_mes, on=["unique_id", "mes"])
    agg["pct"] = agg["quantidade"] / agg["total_mes"].replace(0, np.nan)

    media_pct = (
        agg[agg["total_mes"] > 0]
        .groupby(["unique_id", "semana"])["pct"]
        .mean()
        .unstack(fill_value=0)
    )
    for s in [1, 2, 3, 4]:
        if s not in media_pct.columns:
            media_pct[s] = 0.0
    media_pct = media_pct[[1, 2, 3, 4]]

    row_sum = media_pct.sum(axis=1)
    media_pct = media_pct.div(row_sum.replace(0, np.nan), axis=0).fillna(0.25)
    media_pct.columns = ["pct_sem1", "pct_sem2", "pct_sem3", "pct_sem4"]
    media_pct = media_pct.reset_index()

    n_meses = (
        agg[agg["total_mes"] > 0]
        .groupby("unique_id")["mes"]
        .nunique()
        .rename("n_meses")
        .reset_index()
    )
    media_pct = media_pct.merge(n_meses, on="unique_id", how="left")
    media_pct["n_meses"] = media_pct["n_meses"].fillna(0).astype(int)

    sem_cols = ["pct_sem1", "pct_sem2", "pct_sem3", "pct_sem4"]
    mask_ok = media_pct["n_meses"] >= MIN_MESES_PADRAO_PROPRIO
    if mask_ok.sum() > 0:
        fallback_sem = media_pct.loc[mask_ok, sem_cols].mean()
        s = fallback_sem.sum()
        fallback_sem = fallback_sem / s if s > 0 else pd.Series(dict.fromkeys(sem_cols, 0.25))
    else:
        fallback_sem = pd.Series(dict.fromkeys(sem_cols, 0.25))

    print(f"[OK] Padrão semanal calculado para {len(media_pct)} combinações SKU×Canal")
    print(f"     Fallback global: {fallback_sem.round(3).to_dict()}")
    return media_pct, fallback_sem


# ─────────────────────────────────────────────
# 3. PADRÃO DIA-DA-SEMANA  (0=seg … 6=dom)
# ─────────────────────────────────────────────
def calcular_padrao_dow(df: pd.DataFrame):
    print("\n[...] Calculando padrão dia-da-semana...")

    df = df.copy()
    df["dow"]       = df["data_venda"].dt.dayofweek
    df["unique_id"] = df["sku"] + "§§" + df["canal"]

    agg = (
        df.groupby(["unique_id", "dow"])["quantidade"]
        .sum()
        .unstack(fill_value=0)
    )
    for d in range(7):
        if d not in agg.columns:
            agg[d] = 0.0
    agg = agg[[0, 1, 2, 3, 4, 5, 6]]

    row_sum = agg.sum(axis=1)
    agg = agg.div(row_sum.replace(0, np.nan), axis=0).fillna(1 / 7)
    agg.columns = [f"pct_dow_{d}" for d in range(7)]
    agg = agg.reset_index()

    total_qtd = (
        df.groupby("unique_id")["quantidade"]
        .sum()
        .rename("total_qtd")
        .reset_index()
    )
    agg = agg.merge(total_qtd, on="unique_id", how="left")
    agg["total_qtd"] = agg["total_qtd"].fillna(0)

    dow_cols = [f"pct_dow_{d}" for d in range(7)]
    mask_ok = agg["total_qtd"] > 0
    if mask_ok.sum() > 0:
        fallback_dow = agg.loc[mask_ok, dow_cols].mean()
        s = fallback_dow.sum()
        fallback_dow = fallback_dow / s if s > 0 else pd.Series(dict.fromkeys(dow_cols, 1 / 7))
    else:
        fallback_dow = pd.Series(dict.fromkeys(dow_cols, 1 / 7))

    print(f"[OK] Padrão dia-da-semana calculado para {len(agg)} combinações SKU×Canal")
    return agg, fallback_dow


# ─────────────────────────────────────────────
# 4. LER FORECAST MENSAL
# ─────────────────────────────────────────────
def ler_forecast(engine) -> pd.DataFrame:
    print("\n[...] Lendo forecast mensal (forecast_12m)...")

    query = text("""
        SELECT
            TRIM("Sku"::text) AS sku,
            COALESCE(NULLIF(TRIM("Canal"::text), ''), 'N/A') AS canal,
            "Data"::date AS data_mes,
            COALESCE("Previsao_Quantidade"::numeric, 0) AS previsao_quantidade
        FROM forecast_12m
        WHERE "Sku" IS NOT NULL AND TRIM("Sku"::text) != ''
          AND COALESCE("Previsao_Quantidade"::numeric, 0) > 0
    """)

    df = pd.read_sql(query, engine)
    df["data_mes"]             = pd.to_datetime(df["data_mes"])
    df["previsao_quantidade"]  = pd.to_numeric(df["previsao_quantidade"], errors="coerce").fillna(0)
    print(f"[OK] {len(df)} registros de forecast mensal carregados")
    return df


# ─────────────────────────────────────────────
# 5. GERAR FORECAST DIÁRIO
# ─────────────────────────────────────────────
def gerar_forecast_diario(
    df_forecast: pd.DataFrame,
    df_sem: pd.DataFrame,
    fallback_sem: pd.Series,
    df_dow: pd.DataFrame,
    fallback_dow: pd.Series,
) -> pd.DataFrame:
    print("\n[...] Gerando forecast diário...")

    sem_idx = df_sem.set_index("unique_id")
    dow_idx = df_dow.set_index("unique_id")

    sem_cols = ["pct_sem1", "pct_sem2", "pct_sem3", "pct_sem4"]
    dow_cols = [f"pct_dow_{d}" for d in range(7)]

    # Definição das faixas de dias por semana intra-mensal
    FAIXAS_SEM = {
        1: (1,  7),
        2: (8,  14),
        3: (15, 21),
        4: (22, 31),
    }

    registros = []
    hoje = date.today()

    for _, row in df_forecast.iterrows():
        sku      = row["sku"]
        canal    = row["canal"]
        data_mes = row["data_mes"]
        qty_mes  = float(row["previsao_quantidade"])
        uid      = f"{sku}§§{canal}"

        # Padrão semanal
        if uid in sem_idx.index and sem_idx.at[uid, "n_meses"] >= MIN_MESES_PADRAO_PROPRIO:
            pct_sem = sem_idx.loc[uid, sem_cols].values.astype(float)
        else:
            pct_sem = fallback_sem[sem_cols].values.astype(float)

        # Padrão dow
        if uid in dow_idx.index and dow_idx.at[uid, "total_qtd"] > 0:
            pct_dow = dow_idx.loc[uid, dow_cols].values.astype(float)
        else:
            pct_dow = fallback_dow[dow_cols].values.astype(float)

        ano    = data_mes.year
        mes    = data_mes.month
        n_dias = calendar.monthrange(ano, mes)[1]

        for sem_num, (d_ini, d_fim) in FAIXAS_SEM.items():
            dias = list(range(d_ini, min(d_fim, n_dias) + 1))
            if not dias:
                continue

            qty_semana = qty_mes * pct_sem[sem_num - 1]

            dows_presentes = [date(ano, mes, d).weekday() for d in dias]
            pesos = np.array([pct_dow[dow] for dow in dows_presentes], dtype=float)
            soma_pesos = pesos.sum()
            if soma_pesos > 0:
                pesos = pesos / soma_pesos
            else:
                pesos = np.ones(len(dias)) / len(dias)

            for i, dia in enumerate(dias):
                registros.append({
                    "sku"                 : sku,
                    "canal"               : canal,
                    "data"                : date(ano, mes, dia),
                    "semana_do_mes"       : sem_num,
                    "quantidade_prevista" : round(qty_semana * pesos[i], 4),
                    "data_calculo"        : hoje,
                })

    df_diario = pd.DataFrame(registros)
    print(f"[OK] {len(df_diario)} linhas de forecast diário geradas")
    return df_diario


# ─────────────────────────────────────────────
# 6. GRAVAR NO POSTGRESQL
# ─────────────────────────────────────────────
def gravar(engine, df: pd.DataFrame):
    print("\n[...] Salvando em forecast_diario...")
    df.to_sql("forecast_diario", engine, if_exists="replace", index=False)
    print(f"[OK] {len(df)} linhas salvas na tabela 'forecast_diario'")

    amostra = (
        df.groupby("sku")["quantidade_prevista"]
        .sum()
        .sort_values(ascending=False)
        .head(5)
        .round(1)
        .reset_index()
        .rename(columns={"quantidade_prevista": "total_previsto"})
    )
    print("\n[i] Top 5 SKUs por quantidade total prevista:")
    print(amostra.to_string(index=False))

    # Exemplo de distribuição semanal para o SKU de maior volume
    if len(amostra):
        top_sku = amostra.iloc[0]["sku"]
        ex = (
            df[df["sku"] == top_sku]
            .groupby(["semana_do_mes"])["quantidade_prevista"]
            .sum()
            .round(1)
            .reset_index()
        )
        print(f"\n[i] Distribuição semanal de '{top_sku}':")
        print(ex.to_string(index=False))


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
def main():
    print("=" * 60)
    print("  S&OP Intelligence · Forecast Diário por SKU")
    print(f"  Rodando em: {datetime.today().strftime('%d/%m/%Y %H:%M')}")
    print("=" * 60)

    engine = conectar()

    df_hist              = ler_historico(engine)
    df_sem, fb_sem       = calcular_padrao_semanal(df_hist)
    df_dow, fb_dow       = calcular_padrao_dow(df_hist)
    df_fc                = ler_forecast(engine)
    df_diario            = gerar_forecast_diario(df_fc, df_sem, fb_sem, df_dow, fb_dow)
    gravar(engine, df_diario)

    print("\n[OK] Forecast Diário finalizado com sucesso!")


if __name__ == "__main__":
    main()
