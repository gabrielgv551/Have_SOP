CREATE TABLE IF NOT EXISTS dfs_fluxo_caixa_diario (
    id SERIAL PRIMARY KEY,
    empresa VARCHAR(50) NOT NULL,
    ano INTEGER NOT NULL,
    mes INTEGER NOT NULL CHECK (mes BETWEEN 1 AND 12),
    dia INTEGER NOT NULL CHECK (dia BETWEEN 1 AND 31),
    tipo VARCHAR(50) NOT NULL,
    valor INTEGER DEFAULT 0,
    atualizado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_dfs_fluxo_caixa_unique
    ON dfs_fluxo_caixa_diario(empresa, ano, mes, dia, tipo);

CREATE INDEX IF NOT EXISTS idx_dfs_fluxo_caixa_periodo
    ON dfs_fluxo_caixa_diario(empresa, ano, mes);
