-- Migration: Create tables that were previously created inline in handlers
-- Date: 2026-05-21

CREATE TABLE IF NOT EXISTS configuracoes (
  empresa      VARCHAR(50)  NOT NULL,
  chave        VARCHAR(100) NOT NULL,
  valor        TEXT,
  atualizado_em TIMESTAMP   DEFAULT NOW(),
  PRIMARY KEY (empresa, chave)
);

CREATE TABLE IF NOT EXISTS fornecedores_config (
  empresa        VARCHAR(50) NOT NULL,
  marca          TEXT        NOT NULL,
  lead_time_dias INTEGER     NOT NULL DEFAULT 30,
  frequencia_tipo VARCHAR(20) DEFAULT 'mensal',
  dia_semana_preferido INTEGER DEFAULT 5,
  intervalo_dias INTEGER DEFAULT 30,
  PRIMARY KEY (empresa, marca)
);

CREATE TABLE IF NOT EXISTS vendas_canais_config (
  empresa        VARCHAR(50) NOT NULL,
  canal          TEXT        NOT NULL,
  lead_time_dias INTEGER     NOT NULL DEFAULT 3,
  PRIMARY KEY (empresa, canal)
);

CREATE TABLE IF NOT EXISTS vendas_previsao (
  empresa       VARCHAR(50)   NOT NULL,
  ano           INTEGER       NOT NULL,
  mes           INTEGER       NOT NULL,
  canal         TEXT          NOT NULL,
  valor         NUMERIC(18,2) NOT NULL DEFAULT 0,
  atualizado_em TIMESTAMP     DEFAULT NOW(),
  PRIMARY KEY (empresa, ano, mes, canal)
);

CREATE TABLE IF NOT EXISTS vendas_grupos_canais (
  empresa VARCHAR(50) NOT NULL,
  grupo   TEXT        NOT NULL,
  canal   TEXT        NOT NULL,
  PRIMARY KEY (empresa, grupo, canal)
);

CREATE TABLE IF NOT EXISTS tiny_canais_config (
  empresa             VARCHAR(50)  NOT NULL,
  canal               TEXT         NOT NULL,
  pct_comissao        NUMERIC(6,2) NOT NULL DEFAULT 0,
  pct_taxa            NUMERIC(6,2) NOT NULL DEFAULT 0,
  pct_imposto         NUMERIC(6,2) NOT NULL DEFAULT 0,
  atualizado_em       TIMESTAMP    DEFAULT NOW(),
  PRIMARY KEY (empresa, canal)
);

SELECT 'Missing tables created' AS status;
