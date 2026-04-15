const { Pool } = require('pg');

const pool = new Pool({
  host: '37.60.236.200',
  port: 5432,
  database: 'Lanzi',
  user: 'postgres',
  password: '131105Gv',
  ssl: { rejectUnauthorized: false },
});

const COMPANY = 'lanzi';

async function run() {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    // 1. Inserir ATIVIDADES OPERACIONAIS se não existir
    const r1 = await client.query(
      `INSERT INTO caixa_categorias (empresa, nome, tipo, parent, ordem)
       VALUES ($1, 'ATIVIDADES OPERACIONAIS', 'section', NULL, 10)
       ON CONFLICT (empresa, nome) DO NOTHING
       RETURNING nome`,
      [COMPANY]
    );
    console.log(r1.rows.length > 0
      ? '  ✅ Inserido: ATIVIDADES OPERACIONAIS'
      : '  ⚠️  Já existe: ATIVIDADES OPERACIONAIS');

    // 2. Garantir que ENTRADAS tenha parent = 'ATIVIDADES OPERACIONAIS'
    const r2 = await client.query(
      `UPDATE caixa_categorias
       SET parent = 'ATIVIDADES OPERACIONAIS', ordem = 11
       WHERE empresa = $1 AND nome = 'ENTRADAS'
       RETURNING nome, parent`,
      [COMPANY]
    );
    console.log(r2.rows.length > 0
      ? `  ✅ ENTRADAS → parent='ATIVIDADES OPERACIONAIS'`
      : '  ⚠️  ENTRADAS não encontrada');

    // 3. Garantir que SAÍDAS tenha parent = 'ATIVIDADES OPERACIONAIS'
    const r3 = await client.query(
      `UPDATE caixa_categorias
       SET parent = 'ATIVIDADES OPERACIONAIS', ordem = 20
       WHERE empresa = $1 AND nome = 'SAÍDAS'
       RETURNING nome, parent`,
      [COMPANY]
    );
    console.log(r3.rows.length > 0
      ? `  ✅ SAÍDAS → parent='ATIVIDADES OPERACIONAIS'`
      : '  ⚠️  SAÍDAS não encontrada');

    // 4. Inserir ATIVIDADES NÃO OPERACIONAIS se não existir
    const r4 = await client.query(
      `INSERT INTO caixa_categorias (empresa, nome, tipo, parent, ordem)
       VALUES ($1, 'ATIVIDADES NÃO OPERACIONAIS', 'section', NULL, 185)
       ON CONFLICT (empresa, nome) DO NOTHING
       RETURNING nome`,
      [COMPANY]
    );
    console.log(r4.rows.length > 0
      ? '  ✅ Inserido: ATIVIDADES NÃO OPERACIONAIS'
      : '  ⚠️  Já existe: ATIVIDADES NÃO OPERACIONAIS');

    // 5. Mostrar estrutura atual para verificação
    const check = await client.query(
      `SELECT nome, tipo, parent, ordem
       FROM caixa_categorias
       WHERE empresa = $1
       ORDER BY ordem`,
      [COMPANY]
    );
    console.log('\n📋 Estrutura atual das categorias:');
    check.rows.forEach(r => {
      console.log(`  [${String(r.ordem).padStart(3)}] ${r.tipo.padEnd(10)} ${r.nome.padEnd(40)} parent: ${r.parent || '(nenhum)'}`);
    });

    await client.query('COMMIT');
    console.log('\n✅ Migração 010 concluída.');
  } catch (e) {
    await client.query('ROLLBACK');
    console.error('Erro:', e.message);
    process.exit(1);
  } finally {
    client.release();
    pool.end();
  }
}

run();
