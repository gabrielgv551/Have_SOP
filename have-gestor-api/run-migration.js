#!/usr/bin/env node
const { Pool } = require('pg');
const fs = require('fs');
const path = require('path');

const pool = new Pool({
  host: '37.60.236.200',
  port: 5432,
  database: 'Lanzi',
  user: 'postgres',
  password: '131105Gv'
});

async function runMigration() {
  try {
    console.log('🔌 Conectando ao PostgreSQL...');
    await pool.query('SELECT NOW()');
    console.log('✅ Conexão estabelecida\n');

    // Read migration file
    const migrationPath = path.join(__dirname, 'migrations', '001_create_usuarios.sql');
    const sql = fs.readFileSync(migrationPath, 'utf-8');

    console.log('📋 Executando migration: 001_create_usuarios.sql\n');

    // Execute migration
    const result = await pool.query(sql);

    console.log('✅ Migration executada com sucesso!\n');

    // Check if table was created
    const tableCheck = await pool.query(`
      SELECT table_name
      FROM information_schema.tables
      WHERE table_schema = 'public'
      AND table_name = 'usuarios'
    `);

    if (tableCheck.rows.length > 0) {
      console.log('✅ Tabela "usuarios" criada com sucesso');
    }

    // Check initial users
    const userCheck = await pool.query('SELECT COUNT(*) as count FROM usuarios');
    console.log(`✅ Total de usuários: ${userCheck.rows[0].count}\n`);

    // List users
    const users = await pool.query(`
      SELECT id, empresa, nome, usuario, perfil, ativo, criado_em
      FROM usuarios
      ORDER BY criado_em
    `);

    console.log('📊 Usuários na tabela:');
    users.rows.forEach(u => {
      console.log(`   - ${u.usuario} (${u.perfil}) [${u.empresa}] - ${u.ativo ? 'Ativo' : 'Inativo'}`);
    });

    console.log('\n⚠️  PRÓXIMO PASSO: Gerar bcrypt hashes para as senhas!');
    console.log('\nExecute este comando Node.js para gerar os hashes:\n');
    console.log('node -e "');
    console.log('const bcrypt = require(\'bcrypt\');');
    console.log('(async () => {');
    console.log('  console.log(\'Admin (lanzi2024):\', await bcrypt.hash(\'lanzi2024\', 10));');
    console.log('  console.log(\'Gestor (have2024):\', await bcrypt.hash(\'have2024\', 10));');
    console.log('  console.log(\'Have (lanzi@2024):\', await bcrypt.hash(\'lanzi@2024\', 10));');
    console.log('})();');
    console.log('"');

    console.log('\nDepois execute os UPDATEs com os hashes gerados.');

    await pool.end();
    process.exit(0);

  } catch (e) {
    console.error('❌ Erro:', e.message);
    await pool.end();
    process.exit(1);
  }
}

runMigration();
