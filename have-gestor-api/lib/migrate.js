/**
 * lib/migrate.js
 * Aplica todas as migrations da pasta /migrations em uma empresa.
 * Idempotente: usa CREATE TABLE IF NOT EXISTS, ALTER TABLE IF NOT EXISTS etc.
 *
 * Uso manual:  node lib/migrate.js lanzi
 * Uso em código: await runMigrations(pool, 'lanzi')
 */

const fs   = require('fs');
const path = require('path');

const MIGRATIONS_DIR = path.join(__dirname, '..', 'migrations');

/**
 * Aplica todas as migrations NNN_*.sql no pool informado (suporta 001–999+).
 * Cria a tabela schema_migrations se não existir para rastrear quais já foram aplicadas.
 */
async function runMigrations(pool, company) {
  // Garante tabela de controle
  await pool.query(`
    CREATE TABLE IF NOT EXISTS schema_migrations (
      filename   VARCHAR(255) PRIMARY KEY,
      applied_at TIMESTAMP DEFAULT NOW()
    )
  `);

  const files = fs.readdirSync(MIGRATIONS_DIR)
    .filter(f => /^\d+_.*\.sql$/.test(f))
    .sort();

  const { rows: applied } = await pool.query('SELECT filename FROM schema_migrations');
  const appliedSet = new Set(applied.map(r => r.filename));

  let ran = 0;
  for (const file of files) {
    if (appliedSet.has(file)) continue;

    const sql = fs.readFileSync(path.join(MIGRATIONS_DIR, file), 'utf8');

    // Remove INSERTs hardcoded de empresa específica (ex: 'lanzi') para não duplicar
    const cleanSql = sql.replace(
      /INSERT INTO usuarios[^;]+ON CONFLICT[^;]+DO NOTHING;/gs,
      ''
    );

    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await client.query(cleanSql);
      await client.query('INSERT INTO schema_migrations (filename) VALUES ($1)', [file]);
      await client.query('COMMIT');
      console.log(`[migrate:${company}] ✓ ${file}`);
      ran++;
    } catch (e) {
      await client.query('ROLLBACK');
      console.warn(`[migrate:${company}] ⚠ ${file}: ${e.message}`);
    } finally {
      client.release();
    }
  }

  if (ran === 0) console.log(`[migrate:${company}] Nenhuma migration pendente.`);
  return ran;
}

// Execução direta: node lib/migrate.js <empresa>
if (require.main === module) {
  const { getPool } = require('./db');
  const company = process.argv[2];
  if (!company) { console.error('Uso: node lib/migrate.js <empresa>'); process.exit(1); }
  const pool = getPool(company);
  runMigrations(pool, company)
    .then(n => { console.log(`[migrate] ${n} migration(s) aplicadas.`); process.exit(0); })
    .catch(e => { console.error(e); process.exit(1); });
}

module.exports = { runMigrations };
