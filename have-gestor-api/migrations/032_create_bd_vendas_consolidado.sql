-- Migration: Criar VIEW bd_vendas_consolidado
-- Hub central de vendas via API — consolida canais de marketplace em colunas idênticas às de bd_vendas
-- Run: psql -h <HOST> -U <USER> -d <DB> -f migrations/032_create_bd_vendas_consolidado.sql
--
-- IMPORTANTE: bd_vendas (GE Finance / Tiny ERP) NÃO entra aqui.
-- Ela continua rodando de forma independente pelo sistema atual.
-- bd_vendas_consolidado é exclusivo para fontes via API (ML, Amazon, Shopee…).
--
-- ┌──────────────────────────────────────────────────────────────────┐
-- │  Colunas extras vs. bd_vendas original:                          │
-- │    id_pedido_canal  — chave única canal+pedido (sem duplicação)  │
-- │    origem          — identifica a tabela fonte                   │
-- └──────────────────────────────────────────────────────────────────┘
--
-- Para adicionar nova fonte (ex: Amazon):
--   1. Criar tabela bd_vendas_amazon com ETL próprio
--   2. Adicionar bloco UNION ALL abaixo com o de-para equivalente
--   3. Rodar este script novamente (CREATE OR REPLACE VIEW)

CREATE OR REPLACE VIEW bd_vendas_consolidado AS

-- ── Bloco 1: Mercado Livre ───────────────────────────────────────────
-- De-para: bd_vendas_ml → colunas bd_vendas
--
--   id_pedido_canal   ← 'Mercado Livre-' || numero_ecommerce
--   origem            ← 'bd_vendas_ml'
--   "Ano"             ← ano
--   "Mes"             ← mes
--   "Data"            ← data
--   "Status"          ← status  (paid→Aprovado, cancelled→Cancelado)
--   "Order ID"        ← numero_ecommerce
--   "Total Venda Pedido" ← total_venda_pedido
--   "Total Venda"     ← total_item          (total do item, não do pedido)
--   "Quantidade Vendida" ← quantidade
--   "Comissao Produto" ← comissao_item      (rateado por item)
--   "Imposto Produto" ← taxes_amount        (rateado por item)
--   "Canal de venda"  ← canal               (fixo 'Mercado Livre')
--   "Canal Apelido"   ← seller_nickname     (diferencia contas ML: Lanzi1, Lanzi2…)
--   "Repasse Financeiro" ← repasse_financeiro (rateado por item)
--   "Sku"             ← sku
--   "Nome Produto"    ← nome_produto
--   "Custo Total"     ← NULL                (sem custo na API do ML)
--   "Margem Produto"  ← NULL                (sem custo)
--   "Categoria"       ← categoria_id
--   "Frete Pago Prod" ← frete_item          (rateado por item)
SELECT
    'Mercado Livre-' || COALESCE(numero_ecommerce, '')   AS id_pedido_canal,
    'bd_vendas_ml'                                       AS origem,
    ano::integer                                         AS "Ano",
    mes::integer                                         AS "Mes",
    data::date                                           AS "Data",
    CASE status
        WHEN 'paid'       THEN 'Aprovado'
        WHEN 'confirmed'  THEN 'Aprovado'
        WHEN 'cancelled'  THEN 'Cancelado'
        ELSE status
    END                                                  AS "Status",
    numero_ecommerce                                     AS "Order ID",
    total_venda_pedido                                   AS "Total Venda Pedido",
    total_item                                           AS "Total Venda",
    quantidade                                           AS "Quantidade Vendida",
    comissao_item                                        AS "Comissao Produto",
    taxes_amount                                         AS "Imposto Produto",
    canal                                                AS "Canal de venda",
    seller_nickname                                      AS "Canal Apelido",
    repasse_financeiro                                   AS "Repasse Financeiro",
    sku                                                  AS "Sku",
    nome_produto                                         AS "Nome Produto",
    NULL::numeric                                        AS "Custo Total",
    NULL::numeric                                        AS "Margem Produto",
    categoria_id                                         AS "Categoria",
    frete_item                                           AS "Frete Pago Prod"
FROM bd_vendas_ml

-- ── Bloco 3 (futuro): Amazon ─────────────────────────────────────────
-- UNION ALL
-- SELECT
--     'Amazon-' || COALESCE(order_id::text, '')          AS id_pedido_canal,
--     'bd_vendas_amazon'                                 AS origem,
--     <de-para amazon → colunas bd_vendas>
-- FROM bd_vendas_amazon

-- ── Bloco 4 (futuro): Shopee ─────────────────────────────────────────
-- UNION ALL
-- SELECT
--     'Shopee-' || COALESCE(order_sn, '')                AS id_pedido_canal,
--     'bd_vendas_shopee'                                 AS origem,
--     <de-para shopee → colunas bd_vendas>
-- FROM bd_vendas_shopee
;

-- Índice funcional (disponível apenas em tabelas, não em views — referência para futura migração)
-- CREATE UNIQUE INDEX uq_consolidado_id ON bd_vendas_consolidado (id_pedido_canal);

SELECT 'bd_vendas_consolidado VIEW criada com sucesso' AS status;
