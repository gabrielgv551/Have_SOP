-- Migration 021: Create pedidos_compra_itens for SKU breakdown per order
CREATE TABLE IF NOT EXISTS pedidos_compra_itens (
  id          SERIAL PRIMARY KEY,
  grupo_id    TEXT          NOT NULL,
  empresa     TEXT          NOT NULL,
  sku         TEXT          NOT NULL,
  quantidade  NUMERIC(12,2) NOT NULL DEFAULT 0,
  custo_un    NUMERIC(12,4) NOT NULL DEFAULT 0,
  custo_total NUMERIC(12,2) NOT NULL DEFAULT 0,
  created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_pci_grupo ON pedidos_compra_itens(empresa, grupo_id);
SELECT 'pedidos_compra_itens table created' AS status;
