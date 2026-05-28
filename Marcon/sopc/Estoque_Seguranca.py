"""
╔══════════════════════════════════════════════════════════════╗
║           S&OP Intelligence · Estoque de Segurança           ║
║  Fonte     : bd_vendas (últimos 12 meses) + curva_abc        ║
║  Método    : Desvio Padrão Mensal × Fator Z (curva ABC)      ║
║              + Teto máximo em dias por curva ABC             ║
║  Banco     : PostgreSQL local (Marcon)                       ║
╚══════════════════════════════════════════════════════════════╝

FÓRMULA:
ES = MIN( Fator_Z × Desvio_Padrão_Mensal × √(Lead_Time ÷ 30) , Teto_Dias × Demanda_Diária )

FATOR Z POR CURVA ABC CRUZADA:
AA          → 2.05  (99% nível de serviço)
AB / BA     → 1.88  (97%)
BB/AC/CA    → 1.65  (95%)
BC / CB     → 1.41  (92%)
CC          → 1.28  (90%)

TETO MÁXIMO EM DIAS DE COBERTURA:
Curva A (AA, AB, BA, AC, CA) → 20 dias
Curva B/C (BB, BC, CB, CC)   → 15 dias

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
           fator_z, es_estatistico, es_teto, estoque_seguranca,
           teto_aplicado, meses_com_dados, confianca, data_calculo
"""

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta

# ─────────────────────────────────────────────
# CONFIGURAÇÃO — mesmo banco dos demais scripts
# ─────────────────────────────────────────────
DB_CONFIG = {
    "host"    : "37.60.236.200",
    "port"    : 5432,
    "database": "Marcon",
    "user"    : "postgres",
    "password": "131105Gv",
}

# ── Defaults (substituídos pelo banco se sopc_config existir) ────────────────
JANELA_MESES = 12

FATOR_Z = {
    "AA": 2.05,
    "AB": 1.88, "BA": 1.88,
    "BB": 1.65, "AC": 1.65, "CA": 1.65,
    "BC": 1.41, "CB": 1.41,
    "CC": 1.28,
}

TETO_DIAS = {
    "AA": 20, "AB": 20, "BA": 20, "AC": 20, "CA": 20,
    "BB": 15, "BC": 15, "CB": 15, "CC": 15,
}


def ler_config(engine, modulo: str) -> dict:
    """Lê sopc_config do banco; retorna {chave: valor}. Silencioso se tabela não existe."""
    try:
        import pandas as _pd
        df = _pd.read_sql(
            f"SELECT chave, valor FROM sopc_config WHERE empresa='marcon' AND modulo='{modulo}'",
            engine
        )
        return dict(zip(df["chave"], df["valor"]))
    except Exception:
        return {}


def aplicar_config(engine):
    global JANELA_MESES, FATOR_Z, TETO_DIAS
    cfg = ler_config(engine, 'estoque_seg')
    if not cfg:
        return
    if 'janela_meses' in cfg:
        JANELA_MESES = int(cfg['janela_meses'])
    for classe in ['AA','AB','BA','BB','AC','CA','BC','CB','CC']:
        k = f'fator_z_{classe}'
        if k in cfg:
            FATOR_Z[classe] = float(cfg[k])
    teto_a  = float(cfg.get('teto_dias_A',  20))
    teto_bc = float(cfg.get('teto_dias_BC', 15))
    for c in ['AA','AB','BA','AC','CA']:
        TETO_DIAS[c] = teto_a
    for c in ['BB','BC','CB','CC']:
        TETO_DIAS[c] = teto_bc
    print(f"[CFG] Estoque Seg: janela={JANELA_MESES}m, teto_A={teto_a}d, teto_BC={teto_bc}d")


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
# 4. MÉDIA MENSAL FUTURA (forecast_12m)
# ─────────────────────────────────────────────
def ler_media_forecast(engine) -> pd.DataFrame:
    hoje     = datetime.today()
    data_fim = hoje + timedelta(days=JANELA_MESES * 30)

    query = text("""
        SELECT sku, AVG(total_mes) AS media_mensal_fc
        FROM (
            SELECT "Sku" AS sku,
                   DATE_TRUNC('month', "Data") AS mes,
                   SUM("Previsao_Quantidade")  AS total_mes
            FROM forecast_12m
            WHERE "Data" >= :hoje AND "Data" <= :data_fim
            GROUP BY "Sku", DATE_TRUNC('month', "Data")
        ) sub
        GROUP BY sku
    """)
    df = pd.read_sql(query, engine, params={"hoje": hoje, "data_fim": data_fim})
    print(f"[OK] Media mensal forecast: {len(df)} SKUs (proximos {JANELA_MESES} meses)")
    return df


# ─────────────────────────────────────────────
# 5. BUSCAR CURVA ABC E LEAD TIME
# ─────────────────────────────────────────────
def ler_abc_e_leadtime(engine) -> pd.DataFrame:
    query = text("""
        SELECT
            c.sku,
            c.abc_cruzada,
            COALESCE(fc.lead_time_dias, 30) AS lead_time
        FROM curva_abc c
        LEFT JOIN cadastros_sku s ON s."Sku" = c.sku
        LEFT JOIN fornecedores_config fc
               ON fc.marca = s."Marca" AND fc.empresa = 'marcon'
    """)
    df = pd.read_sql(query, engine)
    configurados = (df["lead_time"] != 30).sum()
    print(f"[OK] Curva ABC + Lead Time: {len(df)} SKUs | {configurados} com lead time configurado")
    return df


# ─────────────────────────────────────────────
# 6. CALCULAR ESTOQUE DE SEGURANÇA COM TETO
# ─────────────────────────────────────────────
def calcular_es(stats: pd.DataFrame, abc_lt: pd.DataFrame, media_fc: pd.DataFrame) -> pd.DataFrame:
    df = stats.merge(abc_lt, on="sku", how="left")
    df = df.merge(media_fc, on="sku", how="left")
    use_fc = df["media_mensal_fc"].notna() & (df["media_mensal_fc"] > 0)
    df.loc[use_fc, "media_mensal"] = df.loc[use_fc, "media_mensal_fc"]
    df = df.drop(columns=["media_mensal_fc"])
    fc_count = use_fc.sum()
    print(f"[OK] Media mensal: {fc_count} SKUs usam forecast, {len(df)-fc_count} usam historico (floor)")

    # Fator Z da curva ABC (default CC se não encontrar)
    df["fator_z"] = df["abc_cruzada"].map(FATOR_Z).fillna(FATOR_Z["CC"])

    # Lead time: default 30 dias se nao cadastrado (0 ou NULL = default)
    df["lead_time"] = df["lead_time"].where(df["lead_time"] > 0).fillna(30)

    # ES Estatístico: Z × σ × √(LT ÷ 30)
    df["es_estatistico"] = (
        df["fator_z"] * df["desvio_padrao"] * np.sqrt(df["lead_time"] / 30)
    ).clip(lower=0)

    # Teto máximo: dias de cobertura × demanda diária
    df["teto_dias"] = df["abc_cruzada"].map(TETO_DIAS).fillna(15)
    df["es_teto"]   = (df["media_mensal"] / 30) * df["teto_dias"]

    # ES Final = menor entre estatístico e teto
    df["estoque_seguranca"] = (
        df[["es_estatistico", "es_teto"]].min(axis=1)
    ).clip(lower=0).round(0).astype(int)

    # Flag indicando se o teto foi aplicado
    df["teto_aplicado"] = df["es_estatistico"] > df["es_teto"]

    # Nível de confiança por histórico disponível
    df["confianca"] = df["meses_com_dados"].apply(
        lambda x: "ALTA" if x >= 6 else ("MEDIA" if x >= 3 else "BAIXA")
    )

    teto_count = df["teto_aplicado"].sum()
    print(f"\n[i] Teto aplicado em {teto_count} SKUs ({teto_count/len(df)*100:.1f}%)")
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
        "es_estatistico", "es_teto", "estoque_seguranca",
        "teto_aplicado", "meses_com_dados", "confianca"
    ]].copy()

    # Formatar decimais com vírgula (padrão BR)
    for col in ["media_mensal", "desvio_padrao", "fator_z", "es_estatistico", "es_teto"]:
        resultado[col] = resultado[col].apply(
            lambda x: f"{x:.2f}".replace(".", ",") if pd.notna(x) else ""
        )

    resultado["data_calculo"] = datetime.today().date()

    resultado.to_sql("estoque_seguranca", engine, if_exists="replace", index=False)
    print(f"\n[OK] {len(resultado)} SKUs salvos na tabela 'estoque_seguranca'")
    print(f"\n[i] Amostra:")
    print(
        resultado[[
            "sku", "abc_cruzada", "media_mensal",
            "es_estatistico", "es_teto", "estoque_seguranca",
            "teto_aplicado", "confianca"
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
    aplicar_config(engine)
    df_vendas = ler_vendas(engine)
    df_mensal = agregar_mensal(df_vendas)
    stats     = calcular_desvio(df_mensal)
    media_fc  = ler_media_forecast(engine)
    abc_lt    = ler_abc_e_leadtime(engine)
    df_es     = calcular_es(stats, abc_lt, media_fc)
    gravar(engine, df_es)

    print("\n[OK] Estoque de Segurança finalizado com sucesso!")


if __name__ == "__main__":
    main()
