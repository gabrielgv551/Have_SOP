/**
 * script: list_bling_clients.js
 * 
 * Script Node.js para listar todos os clientes Bling V3 que estão
 * integrados e cadastrados no banco de dados CENTRAL de Bling.
 * 
 * Uso:
 *   node list_bling_clients.js <slug_da_empresa>
 * 
 * Exemplo:
 *   node list_bling_clients.js autoequip
 */

const { Pool } = require('pg');

// Configurações de conexão do cluster
const DB_HOST = process.env.AUTOEQUIP_HOST || '37.60.236.200';
const DB_PORT = process.env.AUTOEQUIP_PORT || 5432;
const DB_USER = process.env.AUTOEQUIP_USER || 'postgres';
const DB_PASS = process.env.AUTOEQUIP_PASSWORD || '131105Gv';

async function listBlingClients(companySlug) {
  if (!companySlug) {
    console.error('❌ Por favor, especifique o slug da empresa (ex: autoequip, lanzi, marcon).');
    console.error('Exemplo: node list_bling_clients.js autoequip');
    process.exit(1);
  }

  const slug = companySlug.toLowerCase().trim();
  console.log(`🔌 Conectando ao banco de dados CENTRAL [bling]...`);

  // Conecta diretamente no banco central 'bling'
  const pool = new Pool({
    host: DB_HOST,
    port: DB_PORT,
    user: DB_USER,
    password: DB_PASS,
    database: 'bling',
    ssl: { rejectUnauthorized: false }
  });

  try {
    // Busca os clientes vinculados a esta empresa
    const res = await pool.query(
      'SELECT id, nome, access_token, expires_at, last_sync, atualizado_em FROM clientes WHERE empresa = $1 ORDER BY nome',
      [slug]
    );

    if (res.rows.length === 0) {
      console.log(`\n⚠️ Nenhum cliente Bling cadastrado/conectado para a empresa [${slug}] no banco central.`);
      console.log('Insira os registros diretamente na tabela "clientes" do banco "bling".');
      return;
    }

    console.log(`\n✅ Encontrado(s) ${res.rows.length} cliente(s) Bling para [${slug}] no banco central:\n`);

    const tableData = res.rows.map(row => {
      const isTokenExpired = () => {
        const expStr = row.expires_at;
        if (!expStr) return 'Sem data / Inativo';
        const expDate = new Date(expStr);
        return expDate < new Date() ? 'Expirado 🔴' : 'Ativo 🟢';
      };

      return {
        'ID no Banco': row.id,
        'Apelido do Cliente': row.nome,
        'Status do Token': isTokenExpired(),
        'Expira em': row.expires_at ? new Date(row.expires_at).toLocaleString('pt-BR') : 'Sem expiração',
        'Última Sincronização': row.last_sync ? new Date(row.last_sync).toLocaleString('pt-BR') : 'Nunca sincronizado',
        'Última Atualização': new Date(row.atualizado_em).toLocaleString('pt-BR')
      };
    });

    console.table(tableData);

  } catch (error) {
    console.error('❌ Erro ao listar clientes Bling:', error.message);
  } finally {
    await pool.end();
  }
}

// Captura a empresa por argumento (caso não seja fornecido, o default é 'autoequip')
const targetCompanySlug = process.argv[2] || 'autoequip';
listBlingClients(targetCompanySlug);
