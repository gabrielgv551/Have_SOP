"""
╔══════════════════════════════════════════════════════════════╗
║           S&OP Intelligence · Estoque de Segurança           ║
║  Fonte     : bd_vendas (últimos 12 meses) + curva_abc        ║
║  Método    : Desvio Padrão Mensal × Fator Z (curva ABC)      ║
║  Banco     : PostgreSQL local (Lanzi)                        ║
╚══════════════════════════════════════════════════════════════╝

FÓRMULA:
ES = Fator_Z × Desvio_Padrão_Mensal × √(Lead_Time ÷ 30)

FATOR Z POR CURVA ABC CRUZADA:
AA          → 2.05  (99% nível de serviço)
AB / BA     → 1.88  (97%)
BB/AC/CA    → 1.65  (95%)
BC / CB     → 1.41  (92%)
CC          → 1.28  (90%)

DEPENDÊNCIAS (rodar ANTES deste script):
  1. UPLOAD_ETL.py    → cria bd_vendas e cadastro_sku
  2. Curva_ABC.PY     → cria curva_abc (com abc_cruzada)

INPUTS:
  ┌─────────────┬────────────────────────┬──────────────────────────────┐
  │  Tabela     │  Colunas necessárias   │  Origem                      │
  ├─────────────┼────────────────────────┼──────────────────────────────┤
  │ bd_vendas   │ "Sku"                  │ UPLOAD_ETL.py (Excel)        │
  │             │ "Data"                 │                              │
  │             │ "Quantidade Vendida"   │                              │
  │             │ "Status"               │                              │
  ├─────────────┼────────────────────────┼──────────────────────────────┤
  │ curva_abc   │ sku                    │ Curva_ABC.PY                 │
  │             │ abc_cruzada            │                              │
  ├─────────────┼────────────────────────┼──────────────────────────────┤
  │ cadastro_sku│ "Sku"                  │ UPLOAD_ETL.py (Excel)        │
  │             │ "Lead Time"            │                              │
  └─────────────┴────────────────────────┴──────────────────────────────┘

OUTPUT:
  Tabela: estoque_seguranca
  Colunas: sku, media_mensal, desvio_padrao, lead_time, abc_cruzada,
           fator_z, estoque_seguranca, meses_com_dados, confianca,
           data_calculo
"""

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta

# ─────────────────────────────────────────────
# CONFIGURAÇÃO — mesmo banco dos demais scripts
# ─────────────────────────────────────────────
DB_CONFIG = {
    "host"    : "localhost",
    "port"    : 5432,
    "database": "Lanzi",
    "user"    : "postgres",
    "password": "1234",
}

JANELA_MESES = 12

FATOR_Z = {
    "AA": 2.05,
    "AB": 1.88, "BA": 1.88,
    "BB": 1.65, "AC": 1.65, "CA": 1.65,
    "BC": 1.41, "CB": 1.41,
    "CC": 1.28,
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
# 1. LER HISTÓRICO DE VENDAS (12 meses)
# ─────────────────────────────────────────────
def ler_vendas(engine) -> pd.DataFrame:
    data_corte = datetime.today() - timedelta(days=JANELA_MESES * 30)

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
    print(f"[OK] {len(df)} registros lidos | desde {data_corte.date()}")
    return df


# ─────────────────────────────────────────────
# 2. AGREGAR VENDAS POR MÊS
# ─────────────────────────────────────────────
def agregar_mensal(df: pd.DataFrame) -> pd.DataFrame:
    df["data_venda"] = pd.to_datetime(df["data_venda"])

    df_mensal = (
        df.groupby(["sku", pd.Grouper(key="data_venda", freq="ME")])
        .agg(quantidade=("quantidade", "sum"))
        .reset_index()
    )

    # Preencher meses sem venda com 0
    todos_skus  = df_mensal["sku"].unique()
    todas_datas = pd.date_range(
        df_mensal["data_venda"].min(),
        df_mensal["data_venda"].max(),
        freq="ME"
    )
    idx = pd.MultiIndex.from_product(
        [todos_skus, todas_datas], names=["sku", "data_venda"]
    )
    df_mensal = (
        df_mensal.set_index(["sku", "data_venda"])
        .reindex(idx, fill_value=0)
        .reset_index()
    )

    print(
        f"[OK] Base mensal: {df_mensal['sku'].nunique()} SKUs "
        f"× {df_mensal['data_venda'].nunique()} meses"
    )
    return df_mensal


# ─────────────────────────────────────────────
# 3. CALCULAR DESVIO PADRÃO POR SKU
# ─────────────────────────────────────────────
def calcular_desvio(df_mensal: pd.DataFrame) -> pd.DataFrame:
    stats = (
        df_mensal.groupby("sku")["quantidade"]
        .agg(
            media_mensal    = "mean",
            desvio_padrao   = "std",
            meses_com_dados = lambda x: (x > 0).sum()
        )
        .reset_index()
    )

    # SKU com só 1 mês: std retorna NaN → estima 30% da média
    stats["desvio_padrao"] = stats.apply(
        lambda r: r["desvio_padrao"]
        if pd.notna(r["desvio_padrao"])
        else r["media_mensal"] * 0.30,
        axis=1
    )

    print(f"[OK] Desvio padrão calculado para {len(stats)} SKUs")
    return stats


# ─────────────────────────────────────────────
# 4. BUSCAR CURVA ABC E LEAD TIME
# ─────────────────────────────────────────────
def ler_abc_e_leadtime(engine) -> pd.DataFrame:
    query = text("""
        SELECT
            c.sku,
            c.abc_cruzada,
            s."LeadTtime" AS lead_time
        FROM curva_abc c
        LEFT JOIN cadastros_sku s ON s."Sku" = c.sku
    """)
    df = pd.read_sql(query, engine)
    print(f"[OK] Curva ABC + Lead Time: {len(df)} SKUs")
    return df


# ─────────────────────────────────────────────
# 5. CALCULAR ESTOQUE DE SEGURANÇA
# ─────────────────────────────────────────────
def calcular_es(stats: pd.DataFrame, abc_lt: pd.DataFrame) -> pd.DataFrame:
    df = stats.merge(abc_lt, on="sku", how="left")

    # Fator Z da curva ABC (default CC se não encontrar)
    df["fator_z"]   = df["abc_cruzada"].map(FATOR_Z).fillna(FATOR_Z["CC"])

    # Lead time: default 30 dias se não cadastrado
    df["lead_time"] = df["lead_time"].fillna(30)

    # Fórmula: ES = Z × σ × √(LT ÷ 30)
    df["estoque_seguranca"] = (
        df["fator_z"] * df["desvio_padrao"] * np.sqrt(df["lead_time"] / 30)
    ).clip(lower=0).round(0).astype(int)

    # Nível de confiança por histórico disponível
    df["confianca"] = df["meses_com_dados"].apply(
        lambda x: "ALTA" if x >= 6 else ("MEDIA" if x >= 3 else "BAIXA")
    )

    print(f"\n[i] Distribuição de confiança:")
    print(df["confianca"].value_counts().to_string())
    return df


# ─────────────────────────────────────────────
# 6. GRAVAR NO POSTGRESQL
# ─────────────────────────────────────────────
def gravar(engine, df: pd.DataFrame):
    resultado = df[[
        "sku", "media_mensal", "desvio_padrao",
        "lead_time", "abc_cruzada", "fator_z",
        "estoque_seguranca", "meses_com_dados", "confianca"
    ]].copy()

    # Formatar decimais com vírgula (padrão BR)
    for col in ["media_mensal", "desvio_padrao", "fator_z"]:
        resultado[col] = resultado[col].apply(
            lambda x: f"{x:.2f}".replace(".", ",") if pd.notna(x) else ""
        )

    resultado["data_calculo"] = datetime.today().date()

    resultado.to_sql("estoque_seguranca", engine, if_exists="replace", index=False)
    print(f"\n[OK] {len(resultado)} SKUs salvos na tabela 'estoque_seguranca'")
    print(f"\n[i] Amostra:")
    print(
        resultado[[
            "sku", "abc_cruzada", "media_mensal", "fator_z",
            "estoque_seguranca", "confianca"
        ]].head(10).to_string(index=False)
    )


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
def main():
    print("=" * 55)
    print("  S&OP Intelligence · Estoque de Segurança")
    print(f"  Rodando em: {datetime.today().strftime('%d/%m/%Y %H:%M')}")
    print("=" * 55)

    engine    = conectar()
    df_vendas = ler_vendas(engine)
    df_mensal = agregar_mensal(df_vendas)
    stats     = calcular_desvio(df_mensal)
    abc_lt    = ler_abc_e_leadtime(engine)
    df_es     = calcular_es(stats, abc_lt)
    gravar(engine, df_es)

    print("\n[OK] Estoque de Segurança finalizado com sucesso!")


if __name__ == "__main__":
    main()
