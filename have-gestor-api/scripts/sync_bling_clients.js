#!/usr/bin/env node
/**
 * script: sync_bling_clients.js
 * 
 * Sincroniza os clientes Bling com a tabela central de clientes.
 * Você pode adicionar clientes manualmente ou via este script.
 * 
 * Uso:
 *   # Listar clientes atuais
 *   node sync_bling_clients.js --list
 *   
 *   # Adicionar um novo cliente (interativo)
 *   node sync_bling_clients.js --add
 *   
 *   # Adicionar cliente com parametros
 *   node sync_bling_clients.js --add --nome "Meu Cliente" --empresa autoequip \
 *     --client-id "xxx" --client-secret "yyy" --access-token "zzz" \
 *     --refresh-token "aaa"
 *   
 *   # Remover cliente
 *   node sync_bling_clients.js --remove --id 1
 *   
 *   # Atualizar status de sincronização
 *   node sync_bling_clients.js --update-sync --id 1
 */

const { Pool } = require('pg');
const readline = require('readline');

const DB_HOST = process.env.BLING_HOST || '37.60.236.200';
const DB_PORT = process.env.BLING_PORT || 5432;
const DB_USER = process.env.BLING_USER || 'postgres';
const DB_PASS = process.env.BLING_PASSWORD || '131105Gv';
const DB_NAME = 'bling';

const pool = new Pool({
  host: DB_HOST,
  port: DB_PORT,
  user: DB_USER,
  password: DB_PASS,
  database: DB_NAME
});

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout
});

function prompt(question) {
  return new Promise((resolve) => {
    rl.question(question, (answer) => {
      resolve(answer.trim());
    });
  });
}

async function listClientes(empresa = null) {
  try {
    let query = 'SELECT id, nome, empresa, access_token, expires_at, last_sync, atualizado_em FROM clientes';
    const params = [];
    
    if (empresa) {
      query += ' WHERE empresa = $1';
      params.push(empresa);
    }
    query += ' ORDER BY empresa, nome';
    
    const result = await pool.query(query, params);
    
    if (result.rows.length === 0) {
      console.log('\n⚠️ Nenhum cliente cadastrado.');
      return;
    }
    
    console.log(`\n✅ Total de ${result.rows.length} cliente(s):\n`);
    
    const tableData = result.rows.map(row => {
      const isTokenExpired = () => {
        if (!row.expires_at) return '❓ Sem data';
        const expDate = new Date(row.expires_at);
        return expDate < new Date() ? '🔴 Expirado' : '🟢 Ativo';
      };
      
      return {
        'ID': row.id,
        'Nome': row.nome,
        'Empresa': row.empresa,
        'Status Token': isTokenExpired(),
        'Expira em': row.expires_at ? new Date(row.expires_at).toLocaleString('pt-BR') : '-',
        'Última Sync': row.last_sync ? new Date(row.last_sync).toLocaleString('pt-BR') : 'Nunca',
        'Atualizado': new Date(row.atualizado_em).toLocaleString('pt-BR')
      };
    });
    
    console.table(tableData);
  } catch (error) {
    console.error('❌ Erro ao listar clientes:', error.message);
  }
}

async function addCliente() {
  try {
    const nome = await prompt('\n📝 Nome do cliente (ex: "Autoequip Store"): ');
    const empresa = await prompt('📝 Empresa (ex: autoequip, lanzi, marcon): ');
    const clientId = await prompt('📝 Client ID (Bling): ');
    const clientSecret = await prompt('📝 Client Secret (Bling): ');
    const accessToken = await prompt('📝 Access Token: ');
    const refreshToken = await prompt('📝 Refresh Token: ');
    
    if (!nome || !empresa || !clientId || !clientSecret) {
      console.log('❌ Campos obrigatórios não preenchidos.');
      return;
    }
    
    const expiresAt = new Date(Date.now() + 30 * 60 * 1000); // +30 min por padrão
    
    const result = await pool.query(
      `INSERT INTO clientes (nome, empresa, client_id, client_secret, access_token, refresh_token, expires_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7)
       ON CONFLICT (empresa, nome) DO UPDATE 
       SET client_id = $3, 
           client_secret = $4, 
           access_token = $5, 
           refresh_token = $6, 
           expires_at = $7,
           atualizado_em = NOW()
       RETURNING id, nome, empresa`,
      [nome, empresa, clientId, clientSecret, accessToken, refreshToken, expiresAt]
    );
    
    const row = result.rows[0];
    console.log(`\n✅ Cliente [${row.nome}] adicionado/atualizado com sucesso! (ID: ${row.id})`);
  } catch (error) {
    console.error('❌ Erro ao adicionar cliente:', error.message);
  }
}

async function removeCliente(id) {
  try {
    const result = await pool.query('DELETE FROM clientes WHERE id = $1 RETURNING nome, empresa', [id]);
    
    if (result.rows.length === 0) {
      console.log(`\n⚠️ Cliente com ID ${id} não encontrado.`);
      return;
    }
    
    const row = result.rows[0];
    console.log(`\n✅ Cliente [${row.nome}] (${row.empresa}) removido com sucesso!`);
  } catch (error) {
    console.error('❌ Erro ao remover cliente:', error.message);
  }
}

async function updateSyncStatus(id) {
  try {
    const result = await pool.query(
      'UPDATE clientes SET last_sync = NOW() WHERE id = $1 RETURNING nome, last_sync',
      [id]
    );
    
    if (result.rows.length === 0) {
      console.log(`\n⚠️ Cliente com ID ${id} não encontrado.`);
      return;
    }
    
    const row = result.rows[0];
    console.log(`\n✅ Status de sincronização atualizado para [${row.nome}]`);
    console.log(`   Última sincronização: ${new Date(row.last_sync).toLocaleString('pt-BR')}`);
  } catch (error) {
    console.error('❌ Erro ao atualizar sincronização:', error.message);
  }
}

async function main() {
  const args = process.argv.slice(2);
  
  if (args.length === 0) {
    console.log('\n📋 Menu de Sincronização de Clientes Bling\n');
    const choice = await prompt('Escolha uma opção:\n  1) Listar clientes\n  2) Adicionar cliente\n  3) Remover cliente\n\nOpção (1-3): ');
    
    switch (choice) {
      case '1':
        await listClientes();
        break;
      case '2':
        await addCliente();
        break;
      case '3':
        const idToRemove = await prompt('ID do cliente a remover: ');
        await removeCliente(parseInt(idToRemove));
        break;
      default:
        console.log('❌ Opção inválida');
    }
  } else if (args[0] === '--list') {
    const empresa = args[1];
    await listClientes(empresa);
  } else if (args[0] === '--add') {
    await addCliente();
  } else if (args[0] === '--remove' && args[1] === '--id') {
    await removeCliente(parseInt(args[2]));
  } else if (args[0] === '--update-sync' && args[1] === '--id') {
    await updateSyncStatus(parseInt(args[2]));
  } else {
    console.log('❌ Comando não reconhecido');
  }
  
  await pool.end();
  rl.close();
}

main();
