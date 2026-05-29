"""
╔══════════════════════════════════════════════════════════════╗
║           S&OP Intelligence · Curva ABC Cruzada              ║
║  Dimensões : Volume (Quantidade) × Margem (MC)               ║
║  Janela    : últimos 6 meses móveis                          ║
║  Cortes    : 20% A · 30% B · 50% C                           ║
║  Output    : UPDATE em cadastro_sku                          ║
╚══════════════════════════════════════════════════════════════╝
"""

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta

# ─────────────────────────────────────────────
# CONFIGURAÇÃO — ajuste aqui para cada empresa
# ─────────────────────────────────────────────
DB_CONFIG = {
    "host"    : "37.60.236.200",
    "port"    : 5432,
    "database": "Lanzi",
    "user"    : "postgres",
    "password": "131105Gv",
}

# ── Defaults (substituídos pelo banco se sopc_config existir) ────────────────
JANELA_MESES   = 6
CORTE_A        = 0.20
CORTE_B        = 0.50

NIVEL_SERVICO = {
    "AA": 0.98,
    "AB": 0.97, "BA": 0.97,
    "BB": 0.95, "AC": 0.95, "CA": 0.95,
    "BC": 0.92, "CB": 0.92,
    "CC": 0.90,
}


def ler_config(engine, modulo: str) -> dict:
    """Lê sopc_config do banco; retorna {chave: valor}. Silencioso se tabela não existe."""
    try:
        import pandas as _pd
        df = _pd.read_sql(
            f"SELECT chave, valor FROM sopc_config WHERE empresa='lanzi' AND modulo='{modulo}'",
            engine
        )
        return dict(zip(df["chave"], df["valor"]))
    except Exception:
        return {}


def aplicar_config(engine):
    global JANELA_MESES, CORTE_A, CORTE_B, NIVEL_SERVICO
    cfg = ler_config(engine, 'curva_abc')
    if not cfg:
        return
    if 'janela_meses' in cfg:
        JANELA_MESES = int(cfg['janela_meses'])
    if 'corte_a' in cfg:
        CORTE_A = float(cfg['corte_a'])
    if 'corte_b' in cfg:
        CORTE_B = float(cfg['corte_b'])
    for classe in ['AA','AB','BA','BB','AC','CA','BC','CB','CC']:
        k = f'nivel_servico_{classe}'
        if k in cfg:
            NIVEL_SERVICO[classe] = float(cfg[k])
    print(f"[CFG] Curva ABC: janela={JANELA_MESES}m, A={CORTE_A*100:.0f}%, B={CORTE_B*100:.0f}%")

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
# 1. LER BD_VENDAS
# ─────────────────────────────────────────────
def ler_vendas(engine) -> pd.DataFrame:
    data_corte = datetime.today() - timedelta(days=JANELA_MESES * 30)

    query = text("""
        SELECT
            "Sku"                          AS sku,
            "Quantidade Vendida"           AS quantidade,
            "Margem Contribuicao Calc"     AS margem_contribuicao,
            "Data"                         AS data_venda
        FROM bd_vendas
        WHERE "Status" !~* '(cancel|devol|n[aã]o.?pago)'
          AND "Data"   >= :data_corte
          AND "Sku"    IS NOT NULL
    """)

    df = pd.read_sql(query, engine, params={"data_corte": data_corte})
    print(f"[OK] {len(df)} registros lidos | período: {data_corte.date()} -> hoje")
    return df


# ─────────────────────────────────────────────
# 2. AGREGAR POR SKU
# ─────────────────────────────────────────────
def agregar(df: pd.DataFrame) -> pd.DataFrame:
    agg = df.groupby("sku").agg(
        volume_total = ("quantidade",          "sum"),
        margem_total = ("margem_contribuicao", "sum"),
    ).reset_index()
    
    # Arredonda margem para 2 casas decimais para evitar números gigantes quebrados (double precision)
    agg["margem_total"] = agg["margem_total"].round(2)

    print(f"[OK] {len(agg)} SKUs únicos encontrados")
    return agg


# ─────────────────────────────────────────────
# 3. CLASSIFICAR A/B/C (corte por % de SKUs)
# ─────────────────────────────────────────────
def classificar_abc(serie: pd.Series) -> pd.Series:
    """
    Classifica uma série numérica em A/B/C
    pelo ranking de SKUs (não por % do valor acumulado).
    Cortes: top 20% → A | próximos 30% → B | demais 50% → C
    """
    n      = len(serie)
    rank   = serie.rank(method="first", ascending=False)
    limite_a = int(np.ceil(n * CORTE_A))
    limite_b = int(np.ceil(n * CORTE_B))

    resultado = pd.Series(index=serie.index, dtype=str)
    resultado[rank <= limite_a]                        = "A"
    resultado[(rank > limite_a) & (rank <= limite_b)]  = "B"
    resultado[rank > limite_b]                         = "C"
    return resultado


# ─────────────────────────────────────────────
# 4. MONTAR CRUZAMENTO E NÍVEL DE SERVIÇO
# ─────────────────────────────────────────────
def montar_curva(agg: pd.DataFrame) -> pd.DataFrame:
    agg["abc_volume"] = classificar_abc(agg["volume_total"])
    agg["abc_margem"] = classificar_abc(agg["margem_total"])
    agg["abc_cruzada"] = agg["abc_volume"] + agg["abc_margem"]
    agg["nivel_servico"] = agg["abc_cruzada"].map(NIVEL_SERVICO)

    # Resumo para conferência
    print("\n[i] Distribuição Curva ABC Cruzada:")
    print(agg["abc_cruzada"].value_counts().sort_index().to_string())
    print(f"\n[i] SKUs por nível de serviço:")
    print(agg.groupby("nivel_servico")["sku"].count().to_string())

    return agg


# ─────────────────────────────────────────────
# 5. GRAVAR NA TABELA CURVA ABC
# ─────────────────────────────────────────────
def gravar(engine, agg: pd.DataFrame):
    """
    Salva o resultado da curva ABC em uma tabela chamada curva_abc
    """
    nome_tabela = "curva_abc"
    try:
        agg.to_sql(nome_tabela, engine, if_exists="replace", index=False)
        print(f"\n[OK] {len(agg)} SKUs salvos com sucesso na tabela '{nome_tabela}'")
    except Exception as e:
        print(f"\n[X] Erro ao salvar na tabela '{nome_tabela}': {e}")


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
def main():
    print("=" * 55)
    print("  S&OP Intelligence · Curva ABC Cruzada")
    print(f"  Rodando em: {datetime.today().strftime('%d/%m/%Y %H:%M')}")
    print("=" * 55)

    engine = conectar()
    aplicar_config(engine)

    df  = ler_vendas(engine)
    agg = agregar(df)
    agg = montar_curva(agg)
    gravar(engine, agg)

    print("\n[OK] Curva ABC finalizada com sucesso!")


if __name__ == "__main__":
    main()
