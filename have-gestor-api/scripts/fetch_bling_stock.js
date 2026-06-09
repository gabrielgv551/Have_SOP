/**
 * script: fetch_bling_stock.js
 *
 * Script para buscar estoque (saldo) de produtos na API Bling V3 para múltiplos clientes/contas.
 * Suporta o fluxo completo de autenticação OAuth 2.0 com rotação automática de Refresh Token.
 *
 * Como funciona o ciclo de vida do Token do Bling V3:
 * 1. O Access Token expira a cada 30 minutos.
 * 2. O Refresh Token serve para renovar o Access Token, mas ele rotaciona (um novo Refresh Token é gerado a cada uso).
 * 3. Portanto, os novos tokens precisam ser persistidos a cada execução. Este script salva os tokens em `bling_tokens.json`.
 *
 * Como rodar:
 *   a) Autorizar um novo cliente:
 *      node fetch_bling_stock.js --authorize <nome_do_cliente> <codigo_authorization_code_recebido_na_url>
 *
 *   b) Buscar estoque de todos os clientes cadastrados:
 *      node fetch_bling_stock.js
 *
 *   c) Buscar estoque de um cliente específico:
 *      node fetch_bling_stock.js --client <nome_do_cliente>
 */

const fs = require('fs');
const path = require('path');

// ==========================================
// CONFIGURAÇÕES DA APLICAÇÃO (BLING V3)
// ==========================================
// Insira o Client ID e Client Secret da sua aplicação Bling registrada.
// O usuário informou as seguintes credenciais:
const CLIENT_ID = process.env.BLING_CLIENT_ID || '7d24d3e4ab13c4e803b0441f52170ddc261395b7';
const CLIENT_SECRET = process.env.BLING_CLIENT_SECRET || 'ebdf4f1c63020852537cef1e4bdd117175fe104b72a3ed3d9ac7aa66bb83';

// URL de redirecionamento cadastrada no Bling (deve ser idêntica à do cadastro no Bling)
const REDIRECT_URI = process.env.BLING_REDIRECT_URI || 'https://have-gestor-frontend.vercel.app/bling-callback';

const TOKENS_FILE_PATH = path.join(__dirname, 'bling_tokens.json');
const BLING_API_BASE = 'https://api.bling.com.br/Api/v3';
const BLING_TOKEN_URL = 'https://api.bling.com.br/oauth/token';

// Helper para aguardar (para respeitar o rate limit do Bling de 3 requisições por segundo)
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * Carrega os tokens salvos de cada cliente a partir do arquivo JSON local.
 */
function loadTokens() {
  if (!fs.existsSync(TOKENS_FILE_PATH)) {
    // Retorna modelo vazio se o arquivo não existir
    return {
      exemplo_cliente_1: {
        access_token: '',
        refresh_token: '',
        expires_at: ''
      }
    };
  }
  try {
    return JSON.parse(fs.readFileSync(TOKENS_FILE_PATH, 'utf8'));
  } catch (error) {
    console.error('⚠️ Erro ao ler o arquivo bling_tokens.json:', error.message);
    return {};
  }
}

/**
 * Salva as informações de tokens atualizadas no arquivo JSON local.
 */
function saveTokens(tokens) {
  try {
    fs.writeFileSync(TOKENS_FILE_PATH, JSON.stringify(tokens, null, 2), 'utf8');
    console.log(`💾 Tokens salvos com sucesso em: ${TOKENS_FILE_PATH}`);
  } catch (error) {
    console.error('❌ Erro ao salvar arquivo bling_tokens.json:', error.message);
  }
}

/**
 * Gera o cabeçalho Authorization Basic exigido pelo Bling
 */
function getBasicAuthHeader() {
  const credentials = `${CLIENT_ID}:${CLIENT_SECRET}`;
  const base64Credentials = Buffer.from(credentials).toString('base64');
  return `Basic ${base64Credentials}`;
}

/**
 * Realiza a troca do authorization_code por access_token e refresh_token (Primeiro Acesso)
 */
async function authorizeClient(clientName, code) {
  console.log(`\n🔑 Iniciando autorização inicial para o cliente [${clientName}]...`);
  
  try {
    const params = new URLSearchParams({
      grant_type: 'authorization_code',
      code: code,
      redirect_uri: REDIRECT_URI
    });

    const response = await fetch(BLING_TOKEN_URL, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Authorization': getBasicAuthHeader(),
        'Accept': '1.0'
      },
      body: params.toString()
    });

    const data = await response.json();

    if (!response.ok) {
      throw new Error(data.error_description || data.error || `Erro HTTP ${response.status}`);
    }

    const tokens = loadTokens();
    
    // Calcula expiração (geralmente expires_in é 1800 segundos = 30min)
    const expiresAt = new Date(Date.now() + (data.expires_in || 1800) * 1000).toISOString();

    tokens[clientName] = {
      access_token: data.access_token,
      refresh_token: data.refresh_token,
      expires_at: expiresAt,
      updated_at: new Date().toISOString()
    };

    saveTokens(tokens);
    console.log(`✅ Cliente [${clientName}] autorizado e conectado com sucesso!`);
    console.log(`💡 Token expira em: ${new Date(expiresAt).toLocaleString('pt-BR')}`);
  } catch (error) {
    console.error(`❌ Falha ao autorizar cliente [${clientName}]:`, error.message);
  }
}

/**
 * Atualiza o Access Token utilizando o Refresh Token (Rotação)
 */
async function refreshAccessToken(clientName, clientData) {
  console.log(`🔄 Atualizando token de acesso para [${clientName}]...`);
  
  if (!clientData.refresh_token) {
    throw new Error(`Refresh Token não encontrado para o cliente [${clientName}]. Use o comando --authorize.`);
  }

  try {
    const params = new URLSearchParams({
      grant_type: 'refresh_token',
      refresh_token: clientData.refresh_token
    });

    const response = await fetch(BLING_TOKEN_URL, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Authorization': getBasicAuthHeader(),
        'Accept': '1.0'
      },
      body: params.toString()
    });

    const data = await response.json();

    if (!response.ok) {
      throw new Error(data.error_description || data.error || `Erro HTTP ${response.status}`);
    }

    // Calcula expiração
    const expiresAt = new Date(Date.now() + (data.expires_in || 1800) * 1000).toISOString();

    const tokens = loadTokens();
    tokens[clientName] = {
      access_token: data.access_token,
      // O Bling rotaciona o Refresh Token, então salvamos o novo que foi recebido
      refresh_token: data.refresh_token || clientData.refresh_token,
      expires_at: expiresAt,
      updated_at: new Date().toISOString()
    };

    saveTokens(tokens);
    console.log(`✨ Token de acesso atualizado para [${clientName}].`);
    return tokens[clientName].access_token;
  } catch (error) {
    console.error(`❌ Falha ao renovar token do cliente [${clientName}]:`, error.message);
    console.error(`💡 Dica: Se o refresh_token expirou (válido por 30 dias) ou foi invalidado, será necessário gerar um novo authorization_code.`);
    throw error;
  }
}

/**
 * Retorna um token válido, renovando-o se estiver expirado ou prestes a expirar.
 */
async function getValidToken(clientName, clientData) {
  const expiresAt = new Date(clientData.expires_at || 0);
  
  // Se expirar em menos de 1 minuto, renova
  const bufferMs = 60 * 1000;
  const isExpired = (expiresAt.getTime() - bufferMs) < Date.now();

  if (isExpired || !clientData.access_token) {
    return await refreshAccessToken(clientName, clientData);
  }

  return clientData.access_token;
}

/**
 * Busca todos os produtos cadastrados para mapear ID para SKU/Código e Nome
 */
async function fetchAllProducts(accessToken) {
  console.log('📦 Buscando cadastro de produtos para mapear SKUs...');
  const productsMap = {}; // ID -> { codigo, nome }
  let pagina = 1;
  const limite = 100;
  let totalPaginas = 1;

  do {
    console.log(`   Página ${pagina}...`);
    const response = await fetch(`${BLING_API_BASE}/produtos?pagina=${pagina}&limite=${limite}`, {
      method: 'GET',
      headers: {
        'Authorization': `Bearer ${accessToken}`,
        'Accept': 'application/json'
      }
    });

    if (response.status === 429) {
      console.log('   ⚠️ Rate limit atingido. Aguardando 5 segundos...');
      await sleep(5000);
      continue;
    }

    if (!response.ok) {
      const errBody = await response.text();
      console.error(`   ❌ Erro ao buscar produtos: Código ${response.status}. Detalhe:`, errBody);
      break;
    }

    const resJson = await response.json();
    const data = resJson.data || [];

    for (const item of data) {
      productsMap[item.id] = {
        codigo: item.codigo || 'Sem SKU/Código',
        nome: item.nome || 'Sem Nome'
      };
    }

    // Como o Bling não envia sempre o cabeçalho/campo total de páginas,
    // se vierem menos itens que o limite, chegamos ao fim.
    if (data.length < limite) {
      break;
    }

    pagina++;
    // Respeita o limite de 3 requisições por segundo
    await sleep(400);
  } while (true);

  console.log(`✅ Cadastro de produtos carregado. ${Object.keys(productsMap).length} produtos mapeados.`);
  return productsMap;
}

/**
 * Busca os saldos de estoque e mescla com os dados dos produtos
 */
async function fetchStock(accessToken, productsMap) {
  console.log('📊 Buscando saldos de estoque (saldoFisico e saldoVirtual)...');
  const stockList = [];
  let pagina = 1;
  const limite = 100;

  do {
    console.log(`   Página ${pagina}...`);
    const response = await fetch(`${BLING_API_BASE}/estoques/saldos?pagina=${pagina}&limite=${limite}`, {
      method: 'GET',
      headers: {
        'Authorization': `Bearer ${accessToken}`,
        'Accept': 'application/json'
      }
    });

    if (response.status === 429) {
      console.log('   ⚠️ Rate limit atingido. Aguardando 5 segundos...');
      await sleep(5000);
      continue;
    }

    if (!response.ok) {
      const errBody = await response.text();
      console.error(`   ❌ Erro ao buscar saldos de estoque: Código ${response.status}. Detalhe:`, errBody);
      break;
    }

    const resJson = await response.json();
    const data = resJson.data || [];

    for (const item of data) {
      const prodId = item.produto.id;
      const productInfo = productsMap[prodId] || { codigo: 'Desconhecido', nome: 'Produto não mapeado' };

      stockList.push({
        id: prodId,
        sku: productInfo.codigo,
        nome: productInfo.nome,
        saldoFisicoTotal: item.saldoFisicoTotal,
        saldoVirtualTotal: item.saldoVirtualTotal,
        depositos: item.depositos || []
      });
    }

    if (data.length < limite) {
      break;
    }

    pagina++;
    // Respeita o limite de 3 requisições por segundo
    await sleep(400);
  } while (true);

  return stockList;
}

/**
 * Função principal para processar o estoque de um cliente
 */
async function processClientStock(clientName, clientData) {
  console.log(`\n======================================================`);
  console.log(`🔍 PROCESSANDO ESTOQUE PARA O CLIENTE: [${clientName}]`);
  console.log(`======================================================`);

  try {
    const token = await getValidToken(clientName, clientData);
    
    // 1. Carrega todos os produtos (para obter SKU/Código e Nome)
    const productsMap = await fetchAllProducts(token);
    
    // 2. Carrega estoque detalhado
    const stock = await fetchStock(token, productsMap);
    
    console.log(`\n📈 Resumo de Estoque para [${clientName}] (Total: ${stock.length} itens):`);
    
    // Exibe os 15 primeiros produtos como exemplo no console formatados em tabela
    const previewData = stock.slice(0, 15).map(item => ({
      'ID Produto': item.id,
      'SKU (Código)': item.sku,
      'Nome do Produto': item.nome.length > 30 ? item.nome.substring(0, 27) + '...' : item.nome,
      'Saldo Físico': item.saldoFisicoTotal,
      'Saldo Virtual': item.saldoVirtualTotal,
      'Nº Depósitos': item.depositos.length
    }));
    
    console.table(previewData);
    if (stock.length > 15) {
      console.log(`... e mais ${stock.length - 15} produtos.`);
    }

    // Exemplo de como você salvaria isso no seu banco de dados
    console.log(`\n💡 Integração: Aqui você percorreria a lista 'stock' para inserir/atualizar no banco de dados.`);
    console.log(`Exemplo de item de estoque completo:\n`, JSON.stringify(stock[0], null, 2));

  } catch (error) {
    console.error(`❌ Erro no processamento do estoque do cliente [${clientName}]:`, error.message);
  }
}

// ==========================================
// ORQUESTRADOR DOS COMANDOS DO SCRIPT
// ==========================================
async function main() {
  const args = process.argv.slice(2);

  // Comando: --authorize <client_name> <code>
  if (args[0] === '--authorize') {
    const clientName = args[1];
    const code = args[2];
    
    if (!clientName || !code) {
      console.log('❌ Parâmetros inválidos. Uso correto:');
      console.log('   node fetch_bling_stock.js --authorize <nome_do_cliente> <authorization_code>');
      process.exit(1);
    }
    
    await authorizeClient(clientName, code);
    process.exit(0);
  }

  // Comando normal: Executar sincronização
  const tokens = loadTokens();
  const clients = Object.keys(tokens).filter(k => k !== 'exemplo_cliente_1');

  if (clients.length === 0) {
    console.log('⚠️ Nenhum cliente cadastrado no arquivo bling_tokens.json.');
    console.log('\nComo conectar um novo cliente:');
    console.log(`1. Acesse no navegador a URL de Autorização:`);
    console.log(`   https://www.bling.com.br/Api/v3/oauth/authorize?response_type=code&client_id=${CLIENT_ID}&state=cliente1`);
    console.log(`2. Faça o login na conta do Bling do seu cliente e clique em Autorizar.`);
    console.log(`3. Você será redirecionado para a REDIRECT_URI com o código na URL (ex: ?code=ABC123XYZ).`);
    console.log(`4. Execute o comando de autorização:`);
    console.log(`   node fetch_bling_stock.js --authorize cliente1 <codigo_code>`);
    process.exit(0);
  }

  // Se o usuário especificou um cliente via argumento "--client <nome>"
  if (args[0] === '--client') {
    const specificClient = args[1];
    if (!specificClient || !tokens[specificClient]) {
      console.error(`❌ Cliente [${specificClient}] não cadastrado ou não encontrado em bling_tokens.json.`);
      console.log('Clientes disponíveis:', clients.join(', '));
      process.exit(1);
    }
    await processClientStock(specificClient, tokens[specificClient]);
  } else {
    // Roda para todos os clientes cadastrados sequencialmente
    for (const clientName of clients) {
      await processClientStock(clientName, tokens[clientName]);
      await sleep(1000); // Intervalo suave entre clientes
    }
  }
}

main().catch(err => {
  console.error('💥 Erro inesperado:', err);
});
