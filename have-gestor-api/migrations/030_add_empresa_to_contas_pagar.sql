-- Coluna que vincula cada título de contas_pagar a uma subunidade/empresa
ALTER TABLE contas_pagar
    ADD COLUMN IF NOT EXISTS empresa TEXT;

CREATE INDEX IF NOT EXISTS idx_contas_pagar_empresa ON contas_pagar(empresa);
