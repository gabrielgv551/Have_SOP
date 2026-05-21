import psycopg2

conn = psycopg2.connect(host='37.60.236.200', port=5432, dbname='Marcon', user='postgres', password='131105Gv')
cur = conn.cursor()

cur.execute('DROP TABLE IF EXISTS pedidos_compra')

cur.execute("""
CREATE TABLE pedidos_compra (
  pedido_id           TEXT        NOT NULL,
  pedido_nome         TEXT,
  documento           TEXT,
  status              INTEGER,
  fornecedor_id       TEXT,
  deposito_id         TEXT,
  moeda               TEXT,
  data_criacao        TIMESTAMPTZ,
  data_envio          TIMESTAMPTZ,
  data_entrega_prev   TIMESTAMPTZ,
  data_recebimento    TIMESTAMPTZ,
  item_id             TEXT        NOT NULL,
  posicao             INTEGER,
  produto_id          TEXT,
  sku                 TEXT,
  ean                 TEXT,
  nome_produto        TEXT,
  quantidade_pedida   NUMERIC     DEFAULT 0,
  quantidade_recebida NUMERIC     DEFAULT 0,
  quantidade_pendente NUMERIC     DEFAULT 0,
  preco_unit          NUMERIC     DEFAULT 0,
  custo_total_item    NUMERIC     DEFAULT 0,
  localizacao         TEXT,
  codigo_fornecedor   TEXT,
  comentarios         TEXT,
  atualizado_em       TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (pedido_id, item_id)
)
""")

conn.commit()
print('[OK] Tabela pedidos_compra criada na Marcon')
cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='pedidos_compra' ORDER BY ordinal_position")
print('Colunas:', [r[0] for r in cur.fetchall()])
cur.close()
conn.close()
