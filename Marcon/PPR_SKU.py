"""
╔══════════════════════════════════════════════════════════════╗
║           S&OP Intelligence · PPR por SKU                    ║
║  Dimensões : Performance de Vendas por Janela Temporal       ║
║  Janelas   : 7d · 14d · 21d · 1 mês · 3 meses               ║
║  Métrica   : Quantidade Vendida                              ║
║  Banco     : PostgreSQL (Marcon)                             ║
╚══════════════════════════════════════════════════════════════╝

DESCRIÇÃO:
  Calcula a quantidade vendida por SKU em múltiplas janelas
  temporais, retroativas a partir de hoje. Ideal para identificar
  tendências recentes de giro, aceleração ou queda de demanda.

DEPENDÊNCIAS (rodar ANTES deste script):
  1. UPLOAD_ETL.py  → cria bd_vendas e cadastros_sku
  2. Curva_ABC.PY   → cria curva_abc (opcional, enriquece o output)

INPUTS:
  ┌─────────────┬────────────────────────┬──────────────────────────────┐
  │  Tabela     │  Colunas necessárias   │  Origem                      │
  ├─────────────┼────────────────────────┼──────────────────────────────┤
  │ bd_vendas   │ "Sku"                  │ UPLOAD_ETL.py (Excel)        │
  │             │ "Data"                 │                              │
  │             │ "Quantidade Vendida"   │                              │
  │             │ "Status"               │                              │
  ├─────────────┼────────────────────────┼──────────────────────────────┤
  │ curva_abc   │ sku                    │ Curva_ABC.PY (opcional)      │
  │             │ abc_cruzada            │                              │
  └─────────────┴────────────────────────┴──────────────────────────────┘

OUTPUT:
  Tabela: ppr_sku
  Colunas:
    sku, abc_cruzada,
    qtd_7d, qtd_14d, qtd_21d, qtd_1m, qtd_3m,
    media_diaria_7d, media_diaria_14d, media_diaria_21d,
    media_diaria_1m, media_diaria_3m,
    tendencia_curto_prazo,   ← compara 7d vs 14d
    tendencia_medio_prazo,   ← compara 1m vs 3m
    data_calculo
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
    "database": "Marcon",
    "user"    : "postgres",
    "password": "131105Gv",
}

# Janelas em dias
JANELAS = {
    "7d" : 7,
    "14d": 14,
    "21d": 21,
    "1m" : 30,
    "3m" : 90,
}

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
# 1. LER VENDAS (janela máxima = 90 dias)
# ─────────────────────────────────────────────
def ler_vendas(engine) -> pd.DataFrame:
    janela_max = max(JANELAS.values())
    data_corte = datetime.today() - timedelta(days=janela_max)

    query = text("""
        SELECT
            "Sku"                AS sku,
            "Data"               AS data_venda,
            "Quantidade Vendida" AS quantidade
        FROM bd_vendas
        WHERE "Status" != 'Cancelado'
          AND "Data"   >= :data_corte
          AND "Sku"    IS NOT NULL
    """)

    df = pd.read_sql(query, engine, params={"data_corte": data_corte})
    df["data_venda"] = pd.to_datetime(df["data_venda"])
    df["quantidade"] = pd.to_numeric(df["quantidade"], errors="coerce").fillna(0)

    print(f"[OK] {len(df)} registros lidos | desde {data_corte.date()}")
    return df


# ─────────────────────────────────────────────
# 2. CALCULAR QUANTIDADE POR JANELA
# ─────────────────────────────────────────────
def calcular_janelas(df: pd.DataFrame) -> pd.DataFrame:
    hoje = pd.Timestamp(datetime.today().date())
    resultados = {}

    for nome, dias in JANELAS.items():
        limite = hoje - timedelta(days=dias)
        subset = df[df["data_venda"] > limite]
        agg = (
            subset.groupby("sku")["quantidade"]
            .sum()
            .rename(f"qtd_{nome}")
        )
        resultados[f"qtd_{nome}"] = agg

    df_result = pd.DataFrame(resultados).reset_index()

    # Garantir que todos os SKUs do histórico apareçam (mesmo que zerado)
    todos_skus = df["sku"].unique()
    df_base = pd.DataFrame({"sku": todos_skus})
    df_result = df_base.merge(df_result, on="sku", how="left").fillna(0)

    # Converter para inteiro
    for col in [f"qtd_{j}" for j in JANELAS]:
        df_result[col] = df_result[col].round(0).astype(int)

    print(f"[OK] Quantidades calculadas para {len(df_result)} SKUs")
    return df_result


# ─────────────────────────────────────────────
# 3. CALCULAR MÉDIAS DIÁRIAS
# ─────────────────────────────────────────────
def calcular_medias(df: pd.DataFrame) -> pd.DataFrame:
    for nome, dias in JANELAS.items():
        df[f"media_diaria_{nome}"] = (df[f"qtd_{nome}"] / dias).round(2)
    return df


# ─────────────────────────────────────────────
# 4. CALCULAR TENDÊNCIAS
# ─────────────────────────────────────────────
def calcular_tendencias(df: pd.DataFrame) -> pd.DataFrame:
    """
    Tendência curto prazo : compara média diária 7d vs 14d
    Tendência médio prazo : compara média diária 1m vs 3m

    ACELERANDO    → janela mais recente > janela mais longa (+10%)
    ESTÁVEL       → variação dentro de ±10%
    DESACELERANDO → janela mais recente < janela mais longa (-10%)
    SEM DADOS     → média 3m ou 14d zerada (sem histórico suficiente)
    """
    def classificar(recente, longa):
        if longa == 0:
            return "SEM DADOS"
        variacao = (recente - longa) / longa
        if variacao > 0.10:
            return "ACELERANDO"
        elif variacao < -0.10:
            return "DESACELERANDO"
        else:
            return "ESTÁVEL"

    df["tendencia_curto_prazo"] = df.apply(
        lambda r: classificar(r["media_diaria_7d"], r["media_diaria_14d"]), axis=1
    )
    df["tendencia_medio_prazo"] = df.apply(
        lambda r: classificar(r["media_diaria_1m"], r["media_diaria_3m"]), axis=1
    )

    print("\n[i] Tendência Curto Prazo (7d vs 14d):")
    print(df["tendencia_curto_prazo"].value_counts().to_string())
    print("\n[i] Tendência Médio Prazo (1m vs 3m):")
    print(df["tendencia_medio_prazo"].value_counts().to_string())
    return df


# ─────────────────────────────────────────────
# 5. ENRIQUECER COM CURVA ABC (opcional)
# ─────────────────────────────────────────────
def enriquecer_abc(engine, df: pd.DataFrame) -> pd.DataFrame:
    try:
        query = text("SELECT sku, abc_cruzada FROM curva_abc")
        abc = pd.read_sql(query, engine)
        df = df.merge(abc, on="sku", how="left")
        df["abc_cruzada"] = df["abc_cruzada"].fillna("N/A")
        print(f"[OK] Curva ABC incorporada: {abc['abc_cruzada'].nunique()} categorias")
    except Exception:
        df["abc_cruzada"] = "N/A"
        print("[!] Tabela curva_abc não encontrada — campo abc_cruzada = N/A")
    return df


# ─────────────────────────────────────────────
# 6. REPASSE FINANCEIRO MÉDIO POR UNIDADE
# ─────────────────────────────────────────────
def calcular_repasse_medio(engine) -> pd.DataFrame:
    """
    Calcula o repasse financeiro médio por unidade nos últimos 90 dias.
    Pro-rateia o repasse do pedido por linha de produto:
      repasse_linha = (Total Venda / Total Venda Pedido) × Repasse Financeiro
      repasse_medio_und = SUM(repasse_linha) / SUM(Quantidade Vendida)
    """
    janela_max = max(JANELAS.values())
    data_corte = datetime.today() - timedelta(days=janela_max)

    query = text("""
        SELECT
            TRIM("Sku"::text) AS sku,
            COALESCE("Quantidade Vendida"::numeric, 0) AS quantidade,
            COALESCE("Total Venda"::numeric, 0)        AS total_venda_prod,
            COALESCE("Total Venda Pedido"::numeric, 0) AS total_venda_pedido,
            COALESCE("Repasse Financeiro"::numeric, 0) AS repasse_financeiro
        FROM bd_vendas
        WHERE "Status" !~* '(cancel|devol|n[aã]o.?pago)'
          AND "Data"  >= :data_corte
          AND "Sku"   IS NOT NULL
          AND TRIM("Sku"::text) != ''
          AND COALESCE("Quantidade Vendida"::numeric, 0) >= 1
    """)

    df = pd.read_sql(query, engine, params={"data_corte": data_corte})

    for col in ["quantidade", "total_venda_prod", "total_venda_pedido", "repasse_financeiro"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # Pro-rateia repasse por linha de produto
    df["repasse_linha"] = df.apply(
        lambda r: (r["total_venda_prod"] / r["total_venda_pedido"]) * r["repasse_financeiro"]
        if r["total_venda_pedido"] > 0 else 0,
        axis=1,
    )

    agg = (
        df.groupby("sku")
        .apply(lambda g: g["repasse_linha"].sum() / g["quantidade"].sum()
               if g["quantidade"].sum() > 0 else 0, include_groups=False)
        .rename("repasse_medio_und")
        .round(4)
        .reset_index()
    )

    print(f"[OK] Repasse médio por unidade calculado para {len(agg)} SKUs")
    return agg


# ─────────────────────────────────────────────
# 7. GRAVAR NO POSTGRESQL
# ─────────────────────────────────────────────
def gravar(engine, df: pd.DataFrame):
    df["data_calculo"] = datetime.today().date()

    colunas_finais = [
        "sku", "abc_cruzada",
        "qtd_7d", "qtd_14d", "qtd_21d", "qtd_1m", "qtd_3m",
        "media_diaria_7d", "media_diaria_14d", "media_diaria_21d",
        "media_diaria_1m", "media_diaria_3m",
        "tendencia_curto_prazo", "tendencia_medio_prazo",
        "repasse_medio_und",
        "data_calculo",
    ]

    resultado = df[colunas_finais].copy()
    resultado.to_sql("ppr_sku", engine, if_exists="replace", index=False)

    print(f"\n[OK] {len(resultado)} SKUs salvos na tabela 'ppr_sku'")
    print("\n[i] Amostra (top 10 por qtd_7d):")
    print(
        resultado.sort_values("qtd_7d", ascending=False)
        .head(10)[[
            "sku", "abc_cruzada",
            "qtd_7d", "qtd_14d", "qtd_1m", "qtd_3m",
            "tendencia_curto_prazo", "tendencia_medio_prazo"
        ]]
        .to_string(index=False)
    )


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
def main():
    print("=" * 55)
    print("  S&OP Intelligence · PPR por SKU")
    print(f"  Rodando em: {datetime.today().strftime('%d/%m/%Y %H:%M')}")
    print("=" * 55)

    engine = conectar()

    df_vendas = ler_vendas(engine)
    df        = calcular_janelas(df_vendas)
    df        = calcular_medias(df)
    df        = calcular_tendencias(df)
    df        = enriquecer_abc(engine, df)
    df_rep    = calcular_repasse_medio(engine)
    df        = df.merge(df_rep, on="sku", how="left")
    df["repasse_medio_und"] = df["repasse_medio_und"].fillna(0)
    gravar(engine, df)

    print("\n[OK] PPR por SKU finalizado com sucesso!")


if __name__ == "__main__":
    main()
