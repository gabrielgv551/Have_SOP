import pandas as pd
from sqlalchemy import create_engine
from statsforecast import StatsForecast
from statsforecast.models import WindowAverage, ADIDA
import warnings
warnings.filterwarnings("ignore")

# ==============================
# 1️⃣ Conexão com PostgreSQL
# ==============================

DB_CONFIG = {
    "host"    : "37.60.236.200",
    "port"    : 5432,
    "database": "Lanzi",
    "user"    : "postgres",
    "password": "131105Gv",
}

engine = create_engine(
    f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
    f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
)

print("📥 Buscando dados do banco (sem cancelados)...")

query = """
SELECT 
    "Sku",
    "Canal de venda",
    "Data",
    "Quantidade Vendida"
FROM bd_vendas
WHERE "Status" != 'Cancelado'
"""

df = pd.read_sql(query, engine)
print(f"✅ Total de linhas carregadas: {len(df)}")

# ==============================
# 2️⃣ Preparar base mensal
# ==============================

df['Data'] = pd.to_datetime(df['Data'])
df['Canal'] = df['Canal de venda'].fillna('N/A').astype(str).str.strip()
df['unique_id'] = df['Sku'].astype(str) + '§§' + df['Canal']

df_mensal = (
    df.groupby(['unique_id', pd.Grouper(key='Data', freq='ME')])
    .agg({'Quantidade Vendida': 'sum'})
    .reset_index()
)

# ✅ Corte: treinar só até janeiro (fevereiro pode estar incompleto)
data_corte = pd.Timestamp('2026-01-31')
df_mensal = df_mensal[df_mensal['Data'] <= data_corte].copy()

# ✅ Preencher meses sem venda com 0 (evita médias infladas)
todos_ids = df_mensal['unique_id'].unique()
todas_datas = pd.date_range(df_mensal['Data'].min(), data_corte, freq='ME')
idx = pd.MultiIndex.from_product([todos_ids, todas_datas], names=['unique_id', 'Data'])
df_mensal = df_mensal.set_index(['unique_id', 'Data']).reindex(idx, fill_value=0).reset_index()

print(f"📊 Base mensal criada: {len(df_mensal)} linhas")
print(f"📅 Dados de treino: até {data_corte.strftime('%m/%Y')}")
print(f"📅 Previsão: março/2026 até fevereiro/2027")

# ==============================
# 3️⃣ Classificar SKUs
# ==============================

meses_por_id = df_mensal.groupby('unique_id')['Data'].nunique()
grupo_a = meses_por_id[meses_por_id >= 6].index.tolist()
grupo_b = meses_por_id[meses_por_id < 6].index.tolist()

print(f"\n📦 Grupo A (WindowAverage 6m) — {len(grupo_a)} séries (6+ meses)")
print(f"📦 Grupo B (ADIDA)            — {len(grupo_b)} séries (< 6 meses)")

# ==============================
# 4️⃣ Formatar para StatsForecast
# ==============================

def preparar_sf(uids):
    subset = df_mensal[df_mensal['unique_id'].isin(uids)].copy()
    subset = subset.rename(columns={'Data': 'ds', 'Quantidade Vendida': 'y'})
    subset['y'] = subset['y'].clip(lower=0).astype(float)
    return subset

# ==============================
# 5️⃣ Rodar previsões
# ==============================

if __name__ == '__main__':

    H = 13  # prevê 13 meses a partir de jan → descarta fev, mantém mar~fev
    resultados = []

    # --- Grupo A: WindowAverage 6 meses ---
    if grupo_a:
        print(f"\n⚡ Grupo A — WindowAverage (6 meses) para {len(grupo_a)} séries...")
        df_a = preparar_sf(grupo_a)
        sf_a = StatsForecast(models=[WindowAverage(window_size=6)], freq='ME', n_jobs=1)
        fc_a = sf_a.forecast(df=df_a, h=H).reset_index()
        fc_a = fc_a[fc_a['ds'] >= pd.Timestamp('2026-03-31')]
        fc_a['Previsao_Quantidade'] = fc_a['WindowAverage'].clip(lower=0).round(0).astype(int)
        fc_a['Previsao_Minima']     = fc_a['Previsao_Quantidade']
        fc_a['Previsao_Maxima']     = fc_a['Previsao_Quantidade']
        fc_a['Metodo'] = 'WindowAverage_6m'
        fc_a = fc_a.rename(columns={'ds': 'Data'})
        fc_a['Sku']   = fc_a['unique_id'].str.split('§§').str[0]
        fc_a['Canal'] = fc_a['unique_id'].str.split('§§').str[1]
        resultados.append(fc_a[['Sku', 'Canal', 'Data', 'Metodo', 'Previsao_Quantidade', 'Previsao_Minima', 'Previsao_Maxima']])
        print(f"✅ Grupo A concluído")

    # --- Grupo B: ADIDA ---
    if grupo_b:
        print(f"\n📊 Grupo B — ADIDA para {len(grupo_b)} séries...")
        df_b = preparar_sf(grupo_b)
        sf_b = StatsForecast(models=[ADIDA()], freq='ME', n_jobs=1)
        fc_b = sf_b.forecast(df=df_b, h=H).reset_index()
        fc_b = fc_b[fc_b['ds'] >= pd.Timestamp('2026-03-31')]
        fc_b['Previsao_Quantidade'] = fc_b['ADIDA'].clip(lower=0).round(0).astype(int)
        fc_b['Previsao_Minima']     = fc_b['Previsao_Quantidade']
        fc_b['Previsao_Maxima']     = fc_b['Previsao_Quantidade']
        fc_b['Metodo'] = 'ADIDA'
        fc_b = fc_b.rename(columns={'ds': 'Data'})
        fc_b['Sku']   = fc_b['unique_id'].str.split('§§').str[0]
        fc_b['Canal'] = fc_b['unique_id'].str.split('§§').str[1]
        resultados.append(fc_b[['Sku', 'Canal', 'Data', 'Metodo', 'Previsao_Quantidade', 'Previsao_Minima', 'Previsao_Maxima']])
        print(f"✅ Grupo B concluído")

    # ==============================
    # 6️⃣ Consolidar e salvar
    # ==============================

    if not resultados:
        print("❌ Nenhuma previsão gerada.")
    else:
        df_forecast = pd.concat(resultados, ignore_index=True)
        df_forecast = df_forecast.sort_values(['Sku', 'Canal', 'Data']).reset_index(drop=True)

        df_forecast['Tendencia']            = 0.0
        df_forecast['Sazonalidade_Anual']   = 0.0
        df_forecast['Efeito_Total_Aditivo'] = 0.0

        df_forecast = df_forecast[[
            'Sku', 'Canal', 'Data', 'Metodo',
            'Previsao_Quantidade', 'Previsao_Minima', 'Previsao_Maxima',
            'Tendencia', 'Sazonalidade_Anual', 'Efeito_Total_Aditivo'
        ]]

        print("\n📤 Salvando no PostgreSQL...")
        df_forecast.to_sql("forecast_12m", engine, if_exists="replace", index=False)

        print("✅ Previsões salvas na tabela forecast_12m!")
        print(f"\n📊 Resumo final:")
        print(df_forecast.groupby('Metodo')[['Sku', 'Canal']].nunique().rename(columns={'Sku': 'SKUs', 'Canal': 'Canais'}))
        print(f"\nTotal de linhas salvas: {len(df_forecast)}")
        print(f"\n📊 Amostra:")
        print(df_forecast.head(12).to_string(index=False))