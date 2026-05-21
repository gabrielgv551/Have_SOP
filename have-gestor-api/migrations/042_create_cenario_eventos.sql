-- Migration: cenario_eventos — eventos tipados com parâmetros JSONB
-- Date: 2026-04-21

CREATE TABLE IF NOT EXISTS cenario_eventos (
    id          SERIAL PRIMARY KEY,
    cenario_id  INTEGER      NOT NULL REFERENCES cenarios(id) ON DELETE CASCADE,

    -- Tipo determina qual aplicador será usado no motor de projeção
    tipo        VARCHAR(50)  NOT NULL CHECK (tipo IN (
                    'emprestimo',
                    'compra_estoque',
                    'ajuste_faturamento'
                )),

    nome        VARCHAR(200) NOT NULL,
    data_inicio DATE         NOT NULL,
    data_fim    DATE,

    -- Parâmetros específicos por tipo (ver comentários abaixo)
    parametros  JSONB        NOT NULL DEFAULT '{}',

    ativo       BOOLEAN      NOT NULL DEFAULT TRUE,
    ordem       INTEGER      NOT NULL DEFAULT 0,

    criado_em      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    atualizado_em  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_cenario_eventos_cenario
    ON cenario_eventos(cenario_id, ativo, ordem);

-- ─────────────────────────────────────────────────────────────────────────────
-- Estrutura esperada em `parametros` por tipo:
-- ─────────────────────────────────────────────────────────────────────────────
--
-- tipo = 'emprestimo'
-- {
--   "valor_principal": 50000000,        -- centavos
--   "tipo_amortizacao": "price"|"sac",  -- default: "price"
--   "taxa_juros_mensal": 0.015,          -- ex: 1.5% = 0.015
--   "carencia_meses": 0,                 -- meses sem amortizar principal
--   "prazo_meses": 24,
--   "categoria_entrada": "Captação de Recursos",
--   "categoria_parcela": "Serviço da Dívida"
-- }
--
-- tipo = 'compra_estoque'
-- {
--   "valor_total_centavos": 10000000,   -- centavos
--   "data_pagamento": "2026-05-15",     -- data da saída de caixa
--   "categoria_saida": "Pagamento Fornecedores"
-- }
--
-- tipo = 'ajuste_faturamento'
-- {
--   "fator": 1.10,                      -- ex: 10% de crescimento
--   "meses": [4, 5, 6],                 -- null = todos os meses do horizonte
--   "categoria_receita": "Receitas de Vendas Marketplace",
--   "manter_margem": true,              -- se true, ajusta CMV proporcionalmente
--   "categoria_cmv": "Custo de Mercadorias Vendidas"
-- }
-- ─────────────────────────────────────────────────────────────────────────────
