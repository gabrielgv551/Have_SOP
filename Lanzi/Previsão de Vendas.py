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

# ==============================
# 3️⃣ Função Prophet por SKU
# ==============================

def rodar_prophet(args):
    sku, df_sku, data_inicio = args

    if len(df_sku) < 6:
        return None

    df_prophet = df_sku.rename(columns={
        'Data': 'ds',
        'Quantidade Vendida': 'y'
    })[['ds', 'y']]

    try:
        modelo = Prophet(
            yearly_seasonality=True,
            weekly_seasonality=False,
            daily_seasonality=False,
            changepoint_prior_scale=0.1,
            seasonality_prior_scale=10,
            interval_width=0.95
        )

        modelo.add_seasonality(name='monthly', period=30.5, fourier_order=5)
        modelo.add_country_holidays(country_name='BR')

        modelo.fit(df_prophet)

        futuro = modelo.make_future_dataframe(periods=12, freq='ME')
        forecast = modelo.predict(futuro)

        previsao_12m = forecast[forecast['ds'] > data_inicio][
            ['ds', 'yhat', 'yhat_lower', 'yhat_upper',
             'trend', 'yearly', 'additive_terms']
        ].head(12).copy()

        for col in ['yhat', 'yhat_lower', 'yhat_upper']:
            previsao_12m[col] = previsao_12m[col].clip(lower=0).round(0).astype(int)

        previsao_12m['trend'] = previsao_12m['trend'].round(2)
        previsao_12m['yearly'] = previsao_12m['yearly'].round(2)
        previsao_12m['additive_terms'] = previsao_12m['additive_terms'].round(2)
        previsao_12m['Sku'] = sku

        return previsao_12m

    except Exception as e:
        print(f"⚠️ Falha SKU={sku}: {e}")
        return None

# ==============================
# 4️⃣ Rodar em paralelo
# ==============================

skus = df_mensal['Sku'].unique()
print(f"🔍 Total de SKUs: {len(skus)}")

args_list = [
    (sku, df_mensal[df_mensal['Sku'] == sku].copy(), data_inicio_previsao)
    for sku in skus
]

nucleos = cpu_count() - 1
print(f"⚡ Usando {nucleos} núcleos de {cpu_count()} disponíveis")
print("⏳ Rodando Prophet em paralelo...\n")

if __name__ == '__main__':
    with Pool(processes=nucleos) as pool:
        resultados = []
        for i, resultado in enumerate(pool.imap(rodar_prophet, args_list)):
            resultados.append(resultado)
            print(f"✔ {i + 1}/{len(skus)}")

    forecast_final = [r for r in resultados if r is not None]
    print(f"\n📈 Previsões geradas para {len(forecast_final)} SKUs")

    # ==============================
    # 5️⃣ Consolidar e salvar
    # ==============================

    if len(forecast_final) == 0:
        print("❌ Nenhuma previsão gerada.")
    else:
        df_forecast = pd.concat(forecast_final).reset_index(drop=True)

        df_forecast.rename(columns={
            'ds': 'Data',
            'yhat': 'Previsao_Quantidade',
            'yhat_lower': 'Previsao_Minima',
            'yhat_upper': 'Previsao_Maxima',
            'trend': 'Tendencia',
            'yearly': 'Sazonalidade_Anual',
            'additive_terms': 'Efeito_Total_Aditivo'
        }, inplace=True)

        df_forecast = df_forecast[[
            'Sku', 'Data',
            'Previsao_Quantidade', 'Previsao_Minima', 'Previsao_Maxima',
            'Tendencia', 'Sazonalidade_Anual', 'Efeito_Total_Aditivo'
        ]]

        print("📤 Salvando no PostgreSQL...")
        df_forecast.to_sql("forecast_12m", engine, if_exists="replace", index=False)

        print("✅ Previsões salvas na tabela forecast_12m!")
        print(f"\n📊 Amostra:")
        print(df_forecast.head(12).to_string(index=False))