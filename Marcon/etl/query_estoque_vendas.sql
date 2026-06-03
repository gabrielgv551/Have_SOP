-- Query: Estoque Atual + Médias de Venda por SKU (formato BR: vírgula como decimal)
-- Junta tiny_estoque_marcon (estoque) com bd_vendas (médias)

WITH vendas_medias AS (
  SELECT 
    "Sku" AS sku,
    ROUND(AVG("Custo Total" / NULLIF("Quantidade Vendida", 0))::numeric, 2) AS custo_medio_und,
    ROUND(AVG("Total Venda"  / NULLIF("Quantidade Vendida", 0))::numeric, 2) AS preco_medio_und,
    ROUND(AVG("Repasse Financeiro" / NULLIF("Quantidade Vendida", 0))::numeric, 2) AS repasse_medio_und,
    SUM("Quantidade Vendida") AS total_vendido,
    COUNT(*) AS total_pedidos
  FROM bd_vendas
  WHERE "Status" !~* '(cancel|devol|n[aã]o.?pago)'
    AND "Quantidade Vendida" > 0
  GROUP BY "Sku"
)

SELECT 
  e.sku,
  e.nome,
  e.deposito_nome,
  ROUND(e.quantidade, 2)::numeric AS estoque_atual,
  ROUND(e.saldo_total, 2)::numeric AS saldo_total,
  ROUND(e.saldo_reservado, 2)::numeric AS saldo_reservado,
  ROUND(COALESCE(v.custo_medio_und, 0), 2)::numeric AS custo_medio_und,
  ROUND(COALESCE(v.preco_medio_und, 0), 2)::numeric AS preco_medio_und,
  ROUND(COALESCE(v.repasse_medio_und, 0), 2)::numeric AS repasse_medio_und,
  ROUND(COALESCE(v.total_vendido, 0), 2)::numeric AS total_vendido_12m,
  COALESCE(v.total_pedidos, 0) AS total_pedidos,
  e.atualizado_em
FROM tiny_estoque_marcon e
LEFT JOIN vendas_medias v ON e.sku = v.sku
WHERE e.quantidade > 0
ORDER BY e.sku, e.deposito_nome;
