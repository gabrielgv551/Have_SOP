"""
consolidar.py — Hub de Vendas Consolidado
==========================================
Utilitários para inspecionar e validar a VIEW bd_vendas_consolidado,
que unifica todas as fontes de venda em colunas idênticas às de bd_vendas.

De-Para oficial (bd_vendas_ml → colunas bd_vendas):
┌─────────────────────────┬───────────────────────────┬──────────────────────────────────┐
│ Coluna bd_vendas        │ Origem bd_vendas_ml        │ Observação                       │
├─────────────────────────┼───────────────────────────┼──────────────────────────────────┤
│ id_pedido_canal (nova)  │ 'Mercado Livre-'||num_ec   │ Chave única entre canais         │
│ origem (nova)           │ 'bd_vendas_ml'             │ Identifica tabela fonte          │
│ "Ano"                   │ ano                        │ direto                           │
│ "Mes"                   │ mes                        │ direto                           │
│ "Data"                  │ data                       │ direto                           │
│ "Status"                │ status                     │ paid→Aprovado, cancelled→Cancel. │
│ "Order ID"              │ numero_ecommerce           │ direto                           │
│ "Total Venda Pedido"    │ total_venda_pedido         │ total do pedido                  │
│ "Total Venda"           │ total_item                 │ total do item/linha              │
│ "Quantidade Vendida"    │ quantidade                 │ direto                           │
│ "Comissao Produto"      │ comissao_item              │ rateado por item                 │
│ "Imposto Produto"       │ taxes_amount               │ rateado por item                 │
│ "Canal de venda"        │ canal                      │ fixo 'Mercado Livre'             │
│ "Canal Apelido"         │ seller_nickname            │ diferencia contas ML             │
│ "Repasse Financeiro"    │ repasse_financeiro         │ rateado por item                 │
│ "Sku"                   │ sku                        │ direto                           │
│ "Nome Produto"          │ nome_produto               │ direto                           │
│ "Custo Total"           │ NULL                       │ sem custo na API ML              │
│ "Margem Produto"        │ NULL                       │ sem custo                        │
│ "Categoria"             │ categoria_id               │ direto                           │
│ "Frete Pago Prod"       │ frete_item                 │ rateado por item                 │
└─────────────────────────┴───────────────────────────┴──────────────────────────────────┘

USO:
  python -m consolidar                  # exibe status (contagem por origem/canal)
  python -m consolidar --testar         # exibe 5 linhas de cada origem
"""

import sys
from dotenv import load_dotenv
from sqlalchemy import text

load_dotenv()

from db import get_engine

VIEW_NAME = "bd_vendas_consolidado"


# ─────────────────────────────────────────────────────────────
# STATUS
# ─────────────────────────────────────────────────────────────
def status_consolidado(engine=None, silent: bool = False) -> dict:
    """
    Imprime (e retorna) contagem de registros por origem e por canal
    na view bd_vendas_consolidado.

    Retorna dict com chaves: ok (bool), total (int), por_origem (list),
    por_canal (list), erro (str|None).
    """
    if engine is None:
        engine = get_engine()

    resultado = {"ok": False, "total": 0, "por_origem": [], "por_canal": [], "erro": None}

    try:
        with engine.connect() as conn:
            # Verifica se a view existe
            existe = conn.execute(text("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.views
                    WHERE table_name = :v
                )
            """), {"v": VIEW_NAME}).scalar()

            if not existe:
                msg = (
                    f"  [!] VIEW '{VIEW_NAME}' não existe no banco.\n"
                    f"      Execute: migrations/032_create_bd_vendas_consolidado.sql"
                )
                if not silent:
                    print(msg)
                resultado["erro"] = msg
                return resultado

            total = conn.execute(
                text(f'SELECT COUNT(*) FROM {VIEW_NAME}')
            ).scalar()

            por_origem = conn.execute(text(f"""
                SELECT origem, COUNT(*) AS registros
                FROM {VIEW_NAME}
                GROUP BY origem
                ORDER BY registros DESC
            """)).fetchall()

            por_canal = conn.execute(text(f"""
                SELECT "Canal de venda", "Canal Apelido", COUNT(*) AS registros
                FROM {VIEW_NAME}
                WHERE "Canal de venda" IS NOT NULL
                GROUP BY "Canal de venda", "Canal Apelido"
                ORDER BY registros DESC
                LIMIT 20
            """)).fetchall()

        resultado.update({
            "ok": True,
            "total": int(total),
            "por_origem": [{"origem": r[0], "registros": int(r[1])} for r in por_origem],
            "por_canal":  [{"canal": r[0], "apelido": r[1], "registros": int(r[2])} for r in por_canal],
        })

        if not silent:
            _sep = "─" * 60
            print(f"\n{_sep}")
            print(f"  {VIEW_NAME}  —  {total:,} registros totais")
            print(_sep)
            print(f"  {'Origem':<25} {'Registros':>10}")
            print(f"  {'-'*25} {'-'*10}")
            for r in resultado["por_origem"]:
                print(f"  {r['origem']:<25} {r['registros']:>10,}")
            print(f"\n  {'Canal de Venda':<25} {'Apelido':<20} {'Registros':>8}")
            print(f"  {'-'*25} {'-'*20} {'-'*8}")
            for r in resultado["por_canal"]:
                print(f"  {str(r['canal']):<25} {str(r['apelido'] or ''):<20} {r['registros']:>8,}")
            print(_sep)

    except Exception as exc:
        resultado["erro"] = str(exc)
        if not silent:
            print(f"  [ERRO] {VIEW_NAME}: {exc}")

    return resultado


# ─────────────────────────────────────────────────────────────
# TESTAR VIEW (amostra de linhas)
# ─────────────────────────────────────────────────────────────
def testar_view(engine=None, n: int = 5):
    """Exibe n linhas de amostra de cada origem na view."""
    if engine is None:
        engine = get_engine()

    print(f"\n  Amostra da VIEW {VIEW_NAME}  (n={n} por origem)\n")
    try:
        with engine.connect() as conn:
            origens = conn.execute(text(f"""
                SELECT DISTINCT origem FROM {VIEW_NAME} ORDER BY origem
            """)).scalars().all()

            for origem in origens:
                print(f"  ── {origem} ──")
                rows = conn.execute(text(f"""
                    SELECT
                        id_pedido_canal,
                        origem,
                        "Data",
                        "Status",
                        "Sku",
                        "Nome Produto",
                        "Total Venda",
                        "Canal de venda",
                        "Canal Apelido"
                    FROM {VIEW_NAME}
                    WHERE origem = :o
                    ORDER BY "Data" DESC NULLS LAST
                    LIMIT :n
                """), {"o": origem, "n": n}).fetchall()

                for row in rows:
                    print(f"    {row[0]:<40} {str(row[2]):<12} {str(row[3]):<12} "
                          f"{str(row[4] or ''):<15} {str(row[6] or ''):<10}")
                print()
    except Exception as exc:
        print(f"  [ERRO] {exc}")


# ─────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    testar = "--testar" in sys.argv
    engine = get_engine()
    status_consolidado(engine)
    if testar:
        testar_view(engine)
