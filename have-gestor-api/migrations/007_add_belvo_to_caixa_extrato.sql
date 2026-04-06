-- Adiciona coluna belvo_tx_id na caixa_extrato para rastrear transações do Open Finance
ALTER TABLE caixa_extrato ADD COLUMN IF NOT EXISTS belvo_tx_id VARCHAR(100);

-- Índice único por (empresa, belvo_tx_id) para evitar duplicatas no sync
-- PostgreSQL permite múltiplos NULLs em unique index, então entradas manuais (sem belvo_tx_id) ficam livres
CREATE UNIQUE INDEX IF NOT EXISTS idx_caixa_extrato_belvo_tx_id
    ON caixa_extrato(empresa, belvo_tx_id);
