import sqlalchemy as sa
url = 'postgresql+psycopg2://postgres:131105Gv@37.60.236.200:5432/Lanzi'
eng = sa.create_engine(url)
with eng.begin() as c:
    r = c.execute(sa.text("""
        SELECT 
            SUM("Total Venda") as total_venda,
            SUM("Total Venda Pedido") as total_venda_pedido,
            SUM("Valor Desconto") as valor_desconto,
            SUM("Valor Liquido") as valor_liquido,
            SUM("Valor Liquido Prod") as valor_liquido_prod,
            SUM("Receita Bruta") as receita_bruta,
            SUM("Receita Liquida") as receita_liquida,
            SUM("Margem Bruta") as margem_bruta,
            SUM("Margem Contribuicao") as margem_contribuicao,
            SUM("Margem Contribuicao Calc") as mc_calc,
            SUM("Custo Total") as custo_total,
            SUM("Repasse Financeiro") as repasse,
            SUM("Frete Recebido") as frete_recebido,
            SUM("Frete Pago") as frete_pago,
            SUM("Comissao Pedido") as comissao_pedido,
            SUM("Taxas") as taxas,
            SUM("Embalagem") as embalagem
        FROM bd_vendas 
        WHERE "Data" >= '2026-05-01' AND "Data" < '2026-06-01'
    """)).fetchone()
    
    cols = [
        "Total Venda", "Total Venda Pedido", "Valor Desconto", 
        "Valor Liquido", "Valor Liquido Prod", "Receita Bruta",
        "Receita Liquida", "Margem Bruta", "Margem Contribuicao",
        "MC Calc", "Custo Total", "Repasse", "Frete Recebido",
        "Frete Pago", "Comissao Pedido", "Taxas", "Embalagem"
    ]
    
    for i, col in enumerate(cols):
        val = r[i] or 0
        print(f"{col:30s}: R$ {val:>15,.2f}")
    
    # Testar combinacoes
    print("\n--- Testes ---")
    print(f"Total Venda + Taxas: R$ {r[0] + r[15]:,.2f}")
    print(f"Total Venda + Embalagem: R$ {r[0] + r[16]:,.2f}")
    print(f"Total Venda + Frete Recebido: R$ {r[0] + r[12]:,.2f}")
    print(f"Total Venda Pedido + Taxas: R$ {r[1] + r[15]:,.2f}")
    print(f"Total Venda + Frete Pago: R$ {r[0] + r[13]:,.2f}")
    print(f"Total Venda Pedido unico + Taxas: ???")
    
    # Valor com desconto
    print(f"\nValor Desconto: R$ {r[2]:,.2f}")
    print(f"Total Venda + Valor Desconto: R$ {r[0] + r[2]:,.2f}")
    print(f"Total Venda - Valor Desconto: R$ {r[0] - r[2]:,.2f}")
