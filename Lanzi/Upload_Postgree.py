import pandas as pd
from sqlalchemy import create_engine
from prophet import Prophet
import warnings
import logging
from multiprocessing import Pool, cpu_count

warnings.filterwarnings("ignore")
logging.getLogger('prophet').setLevel(logging.ERROR)
logging.getLogger('cmdstanpy').setLevel(logging.ERROR)

# ==============================
# 1️⃣ Conexão com PostgreSQL
# ==============================

engine = create_engine("postgresql+psycopg2://postgres:1234@localhost:5432/Lanzi")

print("📥 Buscando dados do banco (sem cancelados)...")

query = """
SELECT 
    "Sku",
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

df_mensal = (
    df.groupby(['Sku', pd.Grouper(key='Data', freq='ME')])
    .agg({'Quantidade Vendida': 'sum'})
    .reset_index()
)

print(f"📊 Base mensal criada: {len(df_mensal)} linhas")

data_inicio_previsao = df_mensal['Data'].max()
print(f"📅 Previsão iniciará a partir de: {data_inicio_previsao.strftime('%m/%Y')}")

# Classificar SKUs por quantidade de meses de histórico
meses_por_sku = df_mensal.groupby('Sku')['Data'].nunique()
grupo_a = meses_por_sku[meses_por_sku >= 12].index.tolist()  # Prophet
grupo_b = meses_por_sku[(meses_por_sku >= 6) & (meses_por_sku < 12)].index.tolist()  # Média móvel
grupo_c = meses_por_sku[meses_por_sku < 6].index.tolist()  # Média simples

print(f"\n📦 Grupo A (Prophet)        — {len(grupo_a)} SKUs (12+ meses)")
print(f"📦 Grupo B (Média Móvel)    — {len(grupo_b)} SKUs (6-11 meses)")
print(f"📦 Grupo C (Média Simples)  — {len(grupo_c)} SKUs (< 6 meses)")

# Gerar datas futuras (próximos 12 meses)
datas_futuras = pd.date_range(
    start=data_inicio_previsao + pd.DateOffset(months=1),
    periods=12,
    freq='ME'
)

# ==============================
# 3️⃣ GRUPO A — Prophet
# ==============================

def rodar_prophet(args):
    sku, df_sku, data_inicio = args

    df_prophet = df_sku.rename(columns={
        'Data': 'ds',
        'Quantidade Vendida': 'y'
    })[['ds', 'y']]

    cap = df_prophet['y'].mean() * 2
    floor = 0
    df_prophet['cap'] = cap
    df_prophet['floor'] = floor

    try:
        modelo = Prophet(
            growth='logistic',
            yearly_seasonality=True,
            weekly_seasonality=False,
            daily_seasonality=False,
            changepoint_prior_scale=0.05,
            seasonality_prior_scale=1.0,
            interval_width=0.95
        )

        modelo.add_seasonality(name='monthly', period=30.5, fourier_order=3)
        modelo.add_country_holidays(country_name='BR')
        modelo.fit(df_prophet)

        futuro = modelo.make_future_dataframe(periods=12, freq='ME')
        futuro['cap'] = cap
        futuro['floor'] = floor

        forecast = modelo.predict(futuro)

        previsao_12m = forecast[forecast['ds'] > data_inicio][
            ['ds', 'yhat', 'yhat_lower', 'yhat_upper', 'trend', 'yearly', 'additive_terms']
        ].head(12).copy()

        for col in ['yhat', 'yhat_lower', 'yhat_upper']:
            previsao_12m[col] = previsao_12m[col].clip(lower=0).round(0).astype(int)

        previsao_12m['trend'] = previsao_12m['trend'].round(2)
        previsao_12m['yearly'] = previsao_12m['yearly'].round(2)
        previsao_12m['additive_terms'] = previsao_12m['additive_terms'].round(2)
        previsao_12m['Sku'] = sku
        previsao_12m['Metodo'] = 'Prophet'

        return previsao_12m

    except Exception as e:
        print(f"⚠️ Falha Prophet SKU={sku}: {e}")
        return None

# ==============================
# 4️⃣ GRUPO B — Média Móvel
# ==============================

def rodar_media_movel(sku, df_sku, datas_futuras):
    df_sku = df_sku.sort_values('Data')
    ultimos = df_sku['Quantidade Vendida'].tail(6).values

    # Pesos maiores para meses mais recentes
    pesos = list(range(1, len(ultimos) + 1))
    media_ponderada = round(sum(u * p for u, p in zip(ultimos, pesos)) / sum(pesos))
    std = df_sku['Quantidade Vendida'].std()
    std = 0 if pd.isna(std) else std  # ✅ Fix: evita NaN quando só há 1 registro

    rows = []
    for data in datas_futuras:
        rows.append({
            'Sku': sku,
            'Data': data,
            'Previsao_Quantidade': max(0, media_ponderada),
            'Previsao_Minima': max(0, round(media_ponderada - std)),
            'Previsao_Maxima': max(0, round(media_ponderada + std)),
            'Tendencia': 0.0,
            'Sazonalidade_Anual': 0.0,
            'Efeito_Total_Aditivo': 0.0,
            'Metodo': 'Media_Movel'
        })
    return pd.DataFrame(rows)

# ==============================
# 5️⃣ GRUPO C — Média Simples
# ==============================

def rodar_media_simples(sku, df_sku, datas_futuras):
    media = round(df_sku['Quantidade Vendida'].mean())
    std = df_sku['Quantidade Vendida'].std()
    std = 0 if pd.isna(std) else std  # ✅ Fix: evita NaN quando só há 1 registro

    rows = []
    for data in datas_futuras:
        rows.append({
            'Sku': sku,
            'Data': data,
            'Previsao_Quantidade': max(0, media),
            'Previsao_Minima': max(0, round(media - std)),
            'Previsao_Maxima': max(0, round(media + std)),
            'Tendencia': 0.0,
            'Sazonalidade_Anual': 0.0,
            'Efeito_Total_Aditivo': 0.0,
            'Metodo': 'Media_Simples'
        })
    return pd.DataFrame(rows)

# ==============================
# 6️⃣ Executar tudo
# ==============================

if __name__ == '__main__':

    forecast_final = []

    # --- Grupo A: Prophet em paralelo ---
    print(f"\n⚡ Rodando Prophet para {len(grupo_a)} SKUs em paralelo...")
    args_list = [
        (sku, df_mensal[df_mensal['Sku'] == sku].copy(), data_inicio_previsao)
        for sku in grupo_a
    ]

    nucleos = cpu_count() - 1
    print(f"⚡ Usando {nucleos} núcleos de {cpu_count()} disponíveis\n")

    with Pool(processes=nucleos) as pool:
        resultados = []
        for i, resultado in enumerate(pool.imap(rodar_prophet, args_list)):
            resultados.append(resultado)
            print(f"✔ {i + 1}/{len(grupo_a)}")

    forecast_final += [r for r in resultados if r is not None]
    print(f"✅ Grupo A concluído: {len([r for r in resultados if r is not None])} SKUs")

    # --- Grupo B: Média Móvel ---
    print(f"\n📊 Rodando Média Móvel para {len(grupo_b)} SKUs...")
    for i, sku in enumerate(grupo_b):
        df_sku = df_mensal[df_mensal['Sku'] == sku].copy()
        resultado = rodar_media_movel(sku, df_sku, datas_futuras)
        forecast_final.append(resultado)
        print(f"✔ {i + 1}/{len(grupo_b)}")
    print(f"✅ Grupo B concluído")

    # --- Grupo C: Média Simples ---
    print(f"\n📊 Rodando Média Simples para {len(grupo_c)} SKUs...")
    for i, sku in enumerate(grupo_c):
        df_sku = df_mensal[df_mensal['Sku'] == sku].copy()
        resultado = rodar_media_simples(sku, df_sku, datas_futuras)
        forecast_final.append(resultado)
        print(f"✔ {i + 1}/{len(grupo_c)}")
    print(f"✅ Grupo C concluído")

    # ==============================
    # 7️⃣ Consolidar e salvar
    # ==============================

    if len(forecast_final) == 0:
        print("❌ Nenhuma previsão gerada.")
    else:
        df_forecast = pd.concat(forecast_final).reset_index(drop=True)

        # Renomear colunas do Prophet
        df_forecast.rename(columns={
            'ds': 'Data',
            'yhat': 'Previsao_Quantidade',
            'yhat_lower': 'Previsao_Minima',
            'yhat_upper': 'Previsao_Maxima',
            'trend': 'Tendencia',
            'yearly': 'Sazonalidade_Anual',
            'additive_terms': 'Efeito_Total_Aditivo'
        }, inplace=True)

        # ✅ Fix: remove colunas duplicadas que possam ter surgido após concat
        df_forecast = df_forecast.loc[:, ~df_forecast.columns.duplicated()]

        df_forecast = df_forecast[[
            'Sku', 'Data', 'Metodo',
            'Previsao_Quantidade', 'Previsao_Minima', 'Previsao_Maxima',
            'Tendencia', 'Sazonalidade_Anual', 'Efeito_Total_Aditivo'
        ]]

        df_forecast = df_forecast.sort_values(['Sku', 'Data']).reset_index(drop=True)

        print("\n📤 Salvando no PostgreSQL...")
        df_forecast.to_sql("forecast_12m", engine, if_exists="replace", index=False)

        print("✅ Previsões salvas na tabela forecast_12m!")
        print(f"\n📊 Resumo final:")
        print(df_forecast.groupby('Metodo')['Sku'].nunique().reset_index().rename(columns={'Sku': 'SKUs'}))
        print(f"\nTotal de linhas salvas: {len(df_forecast)}")
        print(f"\n📊 Amostra:")
        print(df_forecast.head(12).to_string(index=False))
