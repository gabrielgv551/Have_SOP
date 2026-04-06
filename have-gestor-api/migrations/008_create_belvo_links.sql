-- Tabela de contas bancárias conectadas via Belvo (Open Finance)
CREATE TABLE IF NOT EXISTS belvo_links (
    id         SERIAL PRIMARY KEY,
    empresa    VARCHAR(50)  NOT NULL,
    link_id    VARCHAR(100) NOT NULL,
    institution VARCHAR(100),
    account_type VARCHAR(50),
    ultimo_sync  TIMESTAMP,
    ativo        BOOLEAN DEFAULT true,
    criado_em    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(empresa, link_id)
);

CREATE INDEX IF NOT EXISTS idx_belvo_links_empresa
    ON belvo_links(empresa);
