"""Replica schema das tabelas funcionais do Lanzi → Supershop (sem dados)."""
import psycopg2

# Tabelas a replicar (sem as bd_vendas_ml_* que são específicas do ML)
TABELAS = [
    'cenarios', 'cenarios_ajustes', 'cenarios_historico',
    'cenarios_refresh_log', 'cenarios_regras', 'cenarios_snapshot_base',
    'cenario_eventos',
    'contas_pagar',
    'dfs_base_movimento',
    'estoque_1', 'estoque_consolidado',
    'fornecedores_config',
    'full_1', 'full_2',
    'lead',
    'po',
    'semana_pedidos',
]

src = psycopg2.connect(host='37.60.236.200', dbname='Lanzi',     user='postgres', password='131105Gv')
dst = psycopg2.connect(host='37.60.236.200', dbname='Supershop', user='postgres', password='131105Gv')
src_cur = src.cursor()
dst_cur = dst.cursor()

for tabela in TABELAS:
    # Verifica se já existe no destino
    dst_cur.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name=%s)", (tabela,))
    if dst_cur.fetchone()[0]:
        print(f"  [SKIP] {tabela} já existe")
        continue

    # Gera CREATE TABLE a partir do schema do Lanzi
    src_cur.execute("""
        SELECT column_name, data_type, character_maximum_length,
               is_nullable, column_default, numeric_precision, numeric_scale
        FROM information_schema.columns
        WHERE table_name = %s AND table_schema = 'public'
        ORDER BY ordinal_position
    """, (tabela,))
    cols = src_cur.fetchall()
    if not cols:
        print(f"  [SKIP] {tabela} não encontrada no Lanzi")
        continue

    col_defs = []
    for col in cols:
        name, dtype, char_len, nullable, default, num_prec, num_scale = col
        # Mapeia tipo
        if dtype == 'character varying':
            type_str = f'VARCHAR({char_len})' if char_len else 'TEXT'
        elif dtype == 'numeric':
            if num_prec and num_scale is not None:
                type_str = f'NUMERIC({num_prec},{num_scale})'
            else:
                type_str = 'NUMERIC'
        elif dtype == 'integer':
            type_str = 'INTEGER'
        elif dtype == 'bigint':
            type_str = 'BIGINT'
        elif dtype == 'boolean':
            type_str = 'BOOLEAN'
        elif dtype in ('timestamp without time zone', 'timestamp with time zone'):
            type_str = 'TIMESTAMP'
        elif dtype == 'date':
            type_str = 'DATE'
        elif dtype == 'text':
            type_str = 'TEXT'
        elif dtype == 'double precision':
            type_str = 'DOUBLE PRECISION'
        elif dtype == 'json' or dtype == 'jsonb':
            type_str = 'JSONB'
        else:
            type_str = dtype.upper()

        null_str = '' if nullable == 'YES' else ' NOT NULL'
        # Simplifica defaults (remove sequences/funções complexas)
        def_str = ''
        if default and 'nextval' not in default and 'now()' not in default.lower():
            def_str = f' DEFAULT {default}'
        elif default and 'now()' in default.lower():
            def_str = ' DEFAULT NOW()'

        col_defs.append(f'  "{name}" {type_str}{null_str}{def_str}')

    ddl = f'CREATE TABLE IF NOT EXISTS {tabela} (\n' + ',\n'.join(col_defs) + '\n)'
    try:
        dst_cur.execute(ddl)
        dst.commit()
        print(f"  [OK] {tabela} criada ({len(cols)} colunas)")
    except Exception as e:
        dst.rollback()
        print(f"  [ERRO] {tabela}: {e}")

src.close()
dst.close()
print("\nConcluído.")
