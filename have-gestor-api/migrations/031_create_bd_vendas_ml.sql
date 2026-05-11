-- Migration: Criar tabela bd_vendas_ml (ETL direto da API Mercado Livre)
-- Run: psql -h <HOST> -U <USER> -d <DB> -f migrations/031_create_bd_vendas_ml.sql

CREATE TABLE IF NOT EXISTS bd_vendas_ml (
    -- Identificação
    order_id            BIGINT       NOT NULL,
    numero_ecommerce    TEXT,
    produto_id          TEXT         NOT NULL,
    -- Datas
    data                DATE,
    data_aprovacao      DATE,
    data_fechamento     DATE,
    ano                 INTEGER,
    mes                 INTEGER,
    -- Status
    status              TEXT,
    status_detalhe      TEXT,
    tags                TEXT,
    -- Comprador
    cliente             TEXT,
    buyer_id            BIGINT,
    doc_tipo            TEXT,
    doc_numero          TEXT,
    -- Produto
    sku                 TEXT,
    nome_produto        TEXT,
    categoria_id        TEXT,
    condicao            TEXT,
    variacao_id         TEXT,
    variacao_attrs      TEXT,
    -- Valores do item
    moeda               TEXT,
    quantidade          NUMERIC,
    preco_unitario      NUMERIC,
    preco_original      NUMERIC,
    desconto_unitario   NUMERIC,
    total_item          NUMERIC,
    -- Valores do pedido
    total_venda_pedido  NUMERIC,
    frete_recebido      NUMERIC,
    frete_item          NUMERIC,
    taxa_ml             NUMERIC,
    comissao_item       NUMERIC,
    coupon_amount       NUMERIC,
    repasse_financeiro  NUMERIC,
    -- DRE simplificado
    receita_bruta       NUMERIC,
    valor_liquido       NUMERIC,
    -- Envio
    shipping_id         BIGINT,
    metodo_envio        TEXT,
    logistic_type       TEXT,
    -- Pagamento
    metodo_pagamento    TEXT,
    parcelas            INTEGER,
    status_pagamento    TEXT,
    -- Canal
    canal               TEXT,

    PRIMARY KEY (order_id, produto_id)
);

-- Índices para queries comuns
CREATE INDEX IF NOT EXISTS idx_bd_vendas_ml_data
    ON bd_vendas_ml (data);

CREATE INDEX IF NOT EXISTS idx_bd_vendas_ml_data_status
    ON bd_vendas_ml (data, status);

CREATE INDEX IF NOT EXISTS idx_bd_vendas_ml_sku
    ON bd_vendas_ml (sku);

SELECT 'bd_vendas_ml criada com sucesso' AS status;
