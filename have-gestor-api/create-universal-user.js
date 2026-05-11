#!/usr/bin/env node
/**
 * Cria o usuário universal "have" em TODOS os bancos configurados.
 * Uso: node create-universal-user.js <senha>
 * Exemplo: node create-universal-user.js MinhaSenh@123
 */
require('dotenv').config({ path: '.env.local' });
const { Pool } = require('pg');
const bcrypt = require('bcrypt');

const password = process.argv[2];
if (!password) {
  console.error('❌ Informe a senha: node create-universal-user.js <senha>');
  process.exit(1);
}

const companies = {
  lanzi: {
    name: 'Lanzi',
    host:     process.env.LANZI_HOST,
    port:     parseInt(process.env.LANZI_PORT || '5432'),
    database: process.env.LANZI_DB,
    user:     process.env.LANZI_USER,
    password: process.env.LANZI_PASSWORD,
  },
  marcon: {
    name: 'Marcon',
    host:     process.env.MARCON_HOST,
    port:     parseInt(process.env.MARCON_PORT || '5432'),
    database: process.env.MARCON_DB,
    user:     process.env.MARCON_USER,
    password: process.env.MARCON_PASSWORD,
  },
};

const UNIVERSAL_USER = {
  empresa:  'have',
  nome:     'Have Gestor',
  usuario:  'have',
  perfil:   'have',
};

async function upsertUser(companyKey, config, senhaHash) {
  const pool = new Pool({
    host:     config.host,
    port:     config.port,
    database: config.database,
    user:     config.user,
    password: config.password,
    ssl:      { rejectUnauthorized: false },
    connectionTimeoutMillis: 5000,
  });

  try {
    await pool.query('SELECT NOW()');
    await pool.query(`
      INSERT INTO usuarios (empresa, nome, usuario, senha_hash, perfil, ativo)
      VALUES ($1, $2, $3, $4, $5, TRUE)
      ON CONFLICT (empresa, usuario)
      DO UPDATE SET senha_hash = $4, ativo = TRUE, atualizado_em = NOW()
    `, [UNIVERSAL_USER.empresa, UNIVERSAL_USER.nome, UNIVERSAL_USER.usuario, senhaHash, UNIVERSAL_USER.perfil]);

    console.log(`  ✅ ${config.name}: usuário "have" criado/atualizado`);
  } catch (e) {
    console.log(`  ⚠️  ${config.name}: falhou — ${e.message}`);
  } finally {
    await pool.end();
  }
}

(async () => {
  console.log('\n🔐 Gerando hash bcrypt...');
  const senhaHash = await bcrypt.hash(password, 10);
  console.log('✅ Hash gerado\n');

  console.log('🔌 Conectando aos bancos...');
  for (const [key, config] of Object.entries(companies)) {
    if (!config.host) {
      console.log(`  ⏭️  ${config.name}: HOST não configurado no .env.local — pulando`);
      continue;
    }
    await upsertUser(key, config, senhaHash);
  }

  console.log('\n✅ Concluído!');
  console.log('   Usuário: have');
  console.log(`   Senha:   ${password}`);
  console.log('   Perfil:  have (acesso total)\n');
})();
