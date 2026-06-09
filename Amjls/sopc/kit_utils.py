"""
╔══════════════════════════════════════════════════════════════╗
║           S&OP Intelligence · Kit Utils                      ║
║  Utilitários para explosão de kits no pipeline S&OP          ║
╚══════════════════════════════════════════════════════════════╝

DESCRIÇÃO:
  Um "kit" é um SKU composto (ex: KIT-OLEO-10) que na venda
  aparece em bd_vendas com seu próprio SKU, mas ao ser vendido
  consome N unidades de um ou mais SKUs simples (componentes).

  Este módulo:
    1. Carrega a tabela sku_kits do banco (empresa-específica)
    2. Para cada venda de kit, gera linhas sintéticas de demanda
       para os SKUs componentes com quantidade = venda × kit_qty
    3. As linhas sintéticas são usadas APENAS nos cálculos S&OP
       (forecast, estoque segurança) — não voltam para bd_vendas

FUNÇÕES EXPORTADAS:
  carregar_kits(engine, empresa) → dict[sku_kit → list[(sku_comp, qty)]]
  explodir_vendas_kits(df, kits) → DataFrame com linhas sintéticas adicionadas

COLUNAS ESPERADAS NO df DE ENTRADA:
  sku, data_venda (ou Data), quantidade (ou Quantidade Vendida), canal (ou Canal)
"""

import pandas as pd
from sqlalchemy import text


# ─────────────────────────────────────────────
# 1. CARREGAR MAPEAMENTOS DE KITS DO BANCO
# ─────────────────────────────────────────────
def carregar_kits(engine, empresa: str = "amjls") -> dict:
    """
    Lê a tabela sku_kits para a empresa informada.
    Retorna: {sku_kit: [(sku_componente, quantidade), ...]}
    Retorna {} silenciosamente se a tabela não existir.
    """
    try:
        df = pd.read_sql(
            text("""
                SELECT sku_kit, sku_componente, quantidade::float
                FROM sku_kits
                WHERE empresa = :empresa AND ativo = true
            """),
            engine,
            params={"empresa": empresa},
        )
        if df.empty:
            return {}

        kits = {}
        for _, row in df.iterrows():
            kit = row["sku_kit"].strip().upper()
            comp = row["sku_componente"].strip().upper()
            qty = float(row["quantidade"])
            kits.setdefault(kit, []).append((comp, qty))

        total_kits = len(kits)
        total_comps = sum(len(v) for v in kits.values())
        print(f"[KITS] {total_kits} kits carregados com {total_comps} componentes (empresa={empresa})")
        return kits

    except Exception as e:
        print(f"[KITS] Tabela sku_kits não encontrada ou erro — kits ignorados: {e}")
        return {}


# ─────────────────────────────────────────────
# 2. EXPLODIR VENDAS DE KITS EM COMPONENTES
# ─────────────────────────────────────────────
def explodir_vendas_kits(df: pd.DataFrame, kits: dict) -> pd.DataFrame:
    """
    Recebe um DataFrame de vendas e um dicionário de kits.
    Para cada linha onde 'sku' pertence a um kit, gera novas linhas
    com os SKUs dos componentes e quantidade = venda × kit_qty.

    O DataFrame original NÃO é modificado — apenas recebe as linhas
    sintéticas adicionadas ao final.

    Colunas aceitas (normaliza internamente):
      sku           → "Sku" | "sku"
      quantidade    → "Quantidade Vendida" | "quantidade"
      canal         → "Canal" | "canal" (opcional)
      data          → "Data" | "data_venda" (opcional)

    Retorna o DataFrame unificado com uma coluna extra '_kit_origem'
    que indica o SKU de kit que gerou a linha (NaN para linhas reais).
    """
    if not kits or df.empty:
        return df

    # ── Normalizar nomes de colunas ──────────────────────────────
    col_map = {}
    col_lower = {c.lower(): c for c in df.columns}

    for alias, normalized in [
        ("sku",               "sku"),
        ("quantidade vendida","quantidade"),
        ("quantidade",        "quantidade"),
        ("data",              "data_venda"),
        ("data_venda",        "data_venda"),
        ("canal",             "canal"),
        ("canal apelido",     "canal"),
    ]:
        if alias in col_lower and normalized not in col_map:
            col_map[col_lower[alias]] = normalized

    work = df.rename(columns=col_map).copy()

    if "sku" not in work.columns:
        print("[KITS] Coluna 'sku' não encontrada — explosão de kits ignorada.")
        return df

    if "quantidade" not in work.columns:
        print("[KITS] Coluna 'quantidade' não encontrada — explosão de kits ignorada.")
        return df

    # Normaliza SKU para maiúsculas antes de comparar
    work["sku"] = work["sku"].astype(str).str.strip().str.upper()
    work["quantidade"] = pd.to_numeric(work["quantidade"], errors="coerce").fillna(0)

    # ── Gerar linhas sintéticas ──────────────────────────────────
    sinteticas = []
    kit_skus   = set(kits.keys())
    mask_kits  = work["sku"].isin(kit_skus)
    df_kits    = work[mask_kits]

    if df_kits.empty:
        print("[KITS] Nenhuma venda de kit encontrada em bd_vendas — sem explosão necessária.")
        return df

    n_linhas_kit = len(df_kits)
    for _, row in df_kits.iterrows():
        sku_kit = row["sku"]
        qty_venda = float(row["quantidade"])
        if qty_venda <= 0:
            continue
        for (sku_comp, kit_qty) in kits[sku_kit]:
            nova = row.copy()
            nova["sku"]        = sku_comp
            nova["quantidade"] = qty_venda * kit_qty
            nova["_kit_origem"] = sku_kit
            sinteticas.append(nova)

    if not sinteticas:
        return df

    df_sint = pd.DataFrame(sinteticas)

    # Renomear de volta para os nomes originais do df
    inv_map = {v: k for k, v in col_map.items() if k != v}
    df_sint = df_sint.rename(columns=inv_map)

    # Adicionar coluna _kit_origem ao df original (NaN para linhas reais)
    df_orig = df.copy()
    if "_kit_origem" not in df_orig.columns:
        df_orig["_kit_origem"] = pd.NA

    n_sint = len(df_sint)
    total  = len(df_orig) + n_sint
    print(
        f"[KITS] {n_linhas_kit} vendas de kit → {n_sint} linhas sintéticas geradas "
        f"({len(kits)} kits diferentes). Total: {total} linhas."
    )
    return pd.concat([df_orig, df_sint], ignore_index=True)
