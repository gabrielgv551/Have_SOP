"""
Roda a migration 011_caixa_depara_tipo.sql no banco Lanzi.
Uso: python run_migration_011.py
"""
import psycopg2, os

conn = psycopg2.connect(
    host="37.60.236.200",
    port=5432,
    dbname="Lanzi",
    user="postgres",
    password="131105Gv",
    sslmode="require"
)
conn.autocommit = True
cur = conn.cursor()

sql = open(os.path.join(os.path.dirname(__file__), "migrations", "011_caixa_depara_tipo.sql"), encoding="utf-8").read()

# Executar statement por statement (psycopg2 não aceita múltiplos blocks bem)
statements = [
    """ALTER TABLE caixa_de_para ADD COLUMN IF NOT EXISTS tipo VARCHAR(20) NOT NULL DEFAULT 'extrato'""",

    """DO $$
DECLARE _con TEXT;
BEGIN
    SELECT conname INTO _con FROM pg_constraint
    WHERE conrelid = 'caixa_de_para'::regclass AND contype = 'u'
      AND array_to_string(ARRAY(SELECT attname FROM pg_attribute
          WHERE attrelid = conrelid AND attnum = ANY(conkey) ORDER BY attnum), ',') = 'empresa,palavra_chave';
    IF _con IS NOT NULL THEN
        EXECUTE 'ALTER TABLE caixa_de_para DROP CONSTRAINT ' || quote_ident(_con);
    END IF;
END$$""",

    """ALTER TABLE caixa_de_para DROP CONSTRAINT IF EXISTS caixa_de_para_empresa_palavra_chave_tipo_key""",

    """ALTER TABLE caixa_de_para ADD CONSTRAINT caixa_de_para_empresa_palavra_chave_tipo_key UNIQUE (empresa, palavra_chave, tipo)""",

    """CREATE INDEX IF NOT EXISTS idx_caixa_de_para_tipo ON caixa_de_para(empresa, tipo)""",
]

for stmt in statements:
    try:
        cur.execute(stmt)
        print(f"OK: {stmt[:60].strip()}...")
    except Exception as e:
        print(f"AVISO: {e}")

cur.execute("SELECT tipo, COUNT(*) FROM caixa_de_para GROUP BY tipo")
rows = cur.fetchall()
print("\nResultado:")
for r in rows:
    print(f"  tipo={r[0]}  count={r[1]}")

cur.close()
conn.close()
print("\nMigração concluída!")
