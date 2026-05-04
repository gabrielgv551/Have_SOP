import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from statsforecast import StatsForecast
from statsforecast.models import ADIDA
import warnings
warnings.filterwarnings("ignore")

# ==============================
# 1️⃣ Conexão com PostgreSQL
# ==============================

DB_CONFIG = {
    "host"    : "37.60.236.200",
    "port"    : 5432,
    "database": "Marcon",
    "user"    : "postgres",
    "password": "131105Gv",
}

engine = create_engine(
    f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
    f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
)

def _ler_config_db(modulo: str) -> dict:
    """Lê sopc_config do banco; retorna {chave: valor}. Silencioso se tabela não existe."""
    try:
        df = pd.read_sql(
            f"SELECT chave, valor FROM sopc_config WHERE empresa='marcon' AND modulo='{modulo}'",
            engine
        )
        return dict(zip(df["chave"], df["valor"]))
    except Exception:
        return {}

_cfg_prev = _ler_config_db('prev_12m')

print("📥 Buscando dados do banco (sem cancelados)...")

query = """
SELECT 
    "Sku",
    COALESCE(NULLIF(TRIM("Canal Apelido"::text), ''), TRIM("Canal de venda"::text), 'N/A') AS "Canal",
    "Data",
    "Quantidade Vendida"
FROM bd_vendas
WHERE "Status" !~* '(cancel|devol|n[aã]o.?pago)'
"""

df = pd.read_sql(query, engine)
print(f"✅ Total de linhas carregadas: {len(df)}")

# ==============================
# 2️⃣ Preparar base mensal
# ==============================

df['Data'] = pd.to_datetime(df['Data'])
df['Canal'] = df['Canal'].fillna('N/A').astype(str).str.strip()
df['unique_id'] = df['Sku'].astype(str) + '§§' + df['Canal']

df_mensal = (
    df.groupby(['unique_id', pd.Grouper(key='Data', freq='ME')])
    .agg({'Quantidade Vendida': 'sum'})
    .reset_index()
)

# ✅ Corte automático: último dia do mês anterior
hoje = pd.Timestamp.today()
data_corte      = pd.Timestamp(hoje.year, hoje.month, 1) - pd.Timedelta(days=1)
mes_inicio_prev = data_corte + pd.offsets.MonthEnd(1)
mes_fim_prev    = data_corte + pd.offsets.MonthEnd(12)
df_mensal = df_mensal[df_mensal['Data'] <= data_corte].copy()

# ✅ Preencher meses sem venda com 0
todos_ids   = df_mensal['unique_id'].unique()
todas_datas = pd.date_range(df_mensal['Data'].min(), data_corte, freq='ME')
idx = pd.MultiIndex.from_product([todos_ids, todas_datas], names=['unique_id', 'Data'])
df_mensal = df_mensal.set_index(['unique_id', 'Data']).reindex(idx, fill_value=0).reset_index()

print(f"📊 Base mensal criada: {len(df_mensal)} linhas")
print(f"📅 Dados de treino: até {data_corte.strftime('%m/%Y')}")
print(f"📅 Previsão: {mes_inicio_prev.strftime('%m/%Y')} até {mes_fim_prev.strftime('%m/%Y')}")

# ==============================
# 3️⃣ Classificar SKUs
# ==============================

_min_meses_a = int(_cfg_prev.get('min_meses_grupo_a', 6))
meses_por_id = df_mensal.groupby('unique_id')['Data'].nunique()
grupo_a = meses_por_id[meses_por_id >= _min_meses_a].index.tolist()
grupo_b = meses_por_id[meses_por_id <  _min_meses_a].index.tolist()

print(f"\n📦 Grupo A (WAvg ponderada curto prazo) — {len(grupo_a)} séries ({_min_meses_a}+ meses)")
print(f"📦 Grupo B (ADIDA)                       — {len(grupo_b)} séries (< {_min_meses_a} meses)")

# ==============================
# 4️⃣ Média ponderada com tendência curto prazo (Grupo A)
# ==============================
# Lógica:
#   - media_longa  = WindowAverage dos últimos 6 meses (linha de base)
#   - media_curta  = média ponderada exponencial dos últimos 3 meses
#     (pesos: mês-2 → 1, mês-1 → 2, mês atual → 4, normalizados)
#   - blended      = 0.4 * media_longa + 0.6 * media_curta
#     (dá mais peso ao curto prazo mas não ignora o histórico)
#   - Tendencia    = media_curta - media_longa  (positivo = crescimento recente)

_p0 = float(_cfg_prev.get('peso_t_minus2', 1))
_p1 = float(_cfg_prev.get('peso_t_minus1', 2))
_p2 = float(_cfg_prev.get('peso_t',        4))
PESOS_CURTO = np.array([_p0, _p1, _p2], dtype=float)
PESOS_CURTO /= PESOS_CURTO.sum()

BLEND_LONGO = float(_cfg_prev.get('blend_longo', 0.40))
BLEND_CURTO = float(_cfg_prev.get('blend_curto', 0.60))

def calcular_previsao_grupo_a(df_mensal, grupo_a, H, mes_inicio_prev):
    subset = df_mensal[df_mensal['unique_id'].isin(grupo_a)].copy()
    subset = subset.sort_values(['unique_id', 'Data'])

    registros = []

    for uid, grp in subset.groupby('unique_id'):
        y = grp['y_val'].values if 'y_val' in grp.columns else grp['Quantidade Vendida'].values
        y = y.astype(float)

        if len(y) < 6:
            continue

        # Médias de referência (sobre os últimos N meses)
        media_longa = y[-6:].mean()

        ultimos_3 = y[-3:]
        media_curta = np.dot(PESOS_CURTO[-len(ultimos_3):], ultimos_3 / PESOS_CURTO[-len(ultimos_3):].sum()
                             ) if len(ultimos_3) == 3 else ultimos_3.mean()
        # Simplificado:
        media_curta = float(np.dot(PESOS_CURTO, y[-3:]))

        # Previsão blended (constante por horizonte)
        previsao_base = BLEND_LONGO * media_longa + BLEND_CURTO * media_curta

        tendencia_mensal = media_curta - media_longa  # delta curto vs longo

        # Gera H meses de previsão
        for h in range(1, H + 1):
            data_prev = mes_inicio_prev + pd.offsets.MonthEnd(h - 1)
            if data_prev < mes_inicio_prev:
                continue

            # Previsão com leve amortecimento da tendência ao longo do horizonte
            # (a tendência vai perdendo força conforme o horizonte aumenta)
            fator_amortecimento = 1 / (1 + 0.15 * (h - 1))  # decai ~13% a cada mês
            previsao = previsao_base + tendencia_mensal * fator_amortecimento * 0.5

            previsao = max(previsao, 0)

            registros.append({
                'unique_id'          : uid,
                'Data'               : data_prev,
                'Previsao_Quantidade': round(previsao),
                'Tendencia'          : round(tendencia_mensal, 2),
                'Media_Longa_6m'     : round(media_longa, 2),
                'Media_Curta_3m_pond': round(media_curta, 2),
                'Metodo'             : 'BlendedWAvg_6m_3m',
            })

    return pd.DataFrame(registros)


# Preparar coluna correta antes de chamar
df_mensal_a = df_mensal[df_mensal['unique_id'].isin(grupo_a)].copy()
df_mensal_a = df_mensal_a.rename(columns={'Quantidade Vendida': 'y_val'})

# ==============================
# 5️⃣ Rodar previsões
# ==============================

if __name__ == '__main__':

    H = 12
    resultados = []

    # --- Grupo A: Blended WAvg com tendência curto prazo ---
    if grupo_a:
        print(f"\n⚡ Grupo A — BlendedWAvg (6m + 3m ponderado) para {len(grupo_a)} séries...")

        df_mensal_a2 = df_mensal[df_mensal['unique_id'].isin(grupo_a)].copy()
        df_mensal_a2 = df_mensal_a2.rename(columns={'Quantidade Vendida': 'y_val'})

        fc_a = calcular_previsao_grupo_a(df_mensal_a2, grupo_a, H, mes_inicio_prev)
        fc_a = fc_a[fc_a['Data'] >= mes_inicio_prev]

        fc_a['Sku']   = fc_a['unique_id'].str.split('§§').str[0]
        fc_a['Canal'] = fc_a['unique_id'].str.split('§§').str[1]

        # Intervalo simples: ±30% da previsão (pode ajustar)
        fc_a['Previsao_Minima'] = (fc_a['Previsao_Quantidade'] * 0.70).round(0).astype(int)
        fc_a['Previsao_Maxima'] = (fc_a['Previsao_Quantidade'] * 1.30).round(0).astype(int)

        resultados.append(fc_a[[
            'Sku', 'Canal', 'Data', 'Metodo',
            'Previsao_Quantidade', 'Previsao_Minima', 'Previsao_Maxima',
            'Tendencia', 'Media_Longa_6m', 'Media_Curta_3m_pond'
        ]])
        print(f"✅ Grupo A concluído — {len(fc_a)} linhas")

    # --- Grupo B: ADIDA ---
    if grupo_b:
        print(f"\n📊 Grupo B — ADIDA para {len(grupo_b)} séries...")

        df_b = df_mensal[df_mensal['unique_id'].isin(grupo_b)].copy()
        df_b = df_b.rename(columns={'Data': 'ds', 'Quantidade Vendida': 'y'})
        df_b['y'] = df_b['y'].clip(lower=0).astype(float)

        sf_b = StatsForecast(models=[ADIDA()], freq='ME', n_jobs=1)
        fc_b = sf_b.forecast(df=df_b, h=H).reset_index()
        fc_b = fc_b[fc_b['ds'] >= mes_inicio_prev]

        fc_b['Previsao_Quantidade'] = fc_b['ADIDA'].clip(lower=0).round(0).astype(int)
        fc_b['Previsao_Minima']     = (fc_b['Previsao_Quantidade'] * 0.70).round(0).astype(int)
        fc_b['Previsao_Maxima']     = (fc_b['Previsao_Quantidade'] * 1.30).round(0).astype(int)
        fc_b['Metodo']              = 'ADIDA'
        fc_b['Tendencia']           = 0.0
        fc_b['Media_Longa_6m']      = 0.0
        fc_b['Media_Curta_3m_pond'] = 0.0

        fc_b = fc_b.rename(columns={'ds': 'Data'})
        fc_b['Sku']   = fc_b['unique_id'].str.split('§§').str[0]
        fc_b['Canal'] = fc_b['unique_id'].str.split('§§').str[1]

        resultados.append(fc_b[[
            'Sku', 'Canal', 'Data', 'Metodo',
            'Previsao_Quantidade', 'Previsao_Minima', 'Previsao_Maxima',
            'Tendencia', 'Media_Longa_6m', 'Media_Curta_3m_pond'
        ]])
        print(f"✅ Grupo B concluído — {len(fc_b)} linhas")

    # ==============================
    # 6️⃣ Consolidar e salvar
    # ==============================

    if not resultados:
        print("❌ Nenhuma previsão gerada.")
    else:
        df_forecast = pd.concat(resultados, ignore_index=True)
        df_forecast = df_forecast.sort_values(['Sku', 'Canal', 'Data']).reset_index(drop=True)

        # Mantém compatibilidade com a tabela anterior
        df_forecast['Sazonalidade_Anual']   = 0.0
        df_forecast['Efeito_Total_Aditivo'] = 0.0

        df_forecast = df_forecast[[
            'Sku', 'Canal', 'Data', 'Metodo',
            'Previsao_Quantidade', 'Previsao_Minima', 'Previsao_Maxima',
            'Tendencia', 'Media_Longa_6m', 'Media_Curta_3m_pond',
            'Sazonalidade_Anual', 'Efeito_Total_Aditivo'
        ]]

        print("\n📤 Salvando no PostgreSQL...")
        df_forecast.to_sql("forecast_12m", engine, if_exists="replace", index=False)

        print("✅ Previsões salvas na tabela forecast_12m!")
        print(f"\n📊 Resumo final:")
        print(df_forecast.groupby('Metodo')[['Sku']].nunique().rename(columns={'Sku': 'SKUs únicos'}))
        print(f"\nTotal de linhas salvas: {len(df_forecast)}")

        print(f"\n📊 Amostra (itens com maior tendência positiva):")
        amostra = (
            df_forecast[df_forecast['Metodo'] == 'BlendedWAvg_6m_3m']
            .drop_duplicates('Sku')
            .nlargest(5, 'Tendencia')
            [['Sku', 'Canal', 'Media_Longa_6m', 'Media_Curta_3m_pond',
              'Tendencia', 'Previsao_Quantidade']]
        )
        print(amostra.to_string(index=False))
