/**
 * API Endpoint: GET /api/bling-clientes
 * 
 * Retorna a lista de clientes Bling cadastrados no banco central (bling)
 * para uma empresa específica.
 * 
 * Query params:
 *   - empresa: slug da empresa (ex: "autoequip", "lanzi", "marcon")
 * 
 * Headers:
 *   - Authorization: Bearer <JWT_TOKEN> (obrigatório)
 * 
 * Resposta de sucesso (200):
 * [
 *   {
 *     "id": 1,
 *     "nome": "Autoequip Store",
 *     "empresa": "autoequip",
 *     "client_id": "7d24d3...",
 *     "access_token": "eyJ0e...",
 *     "refresh_token": "ebdf4f...",
 *     "expires_at": "2026-06-09T14:30:00.000Z",
 *     "last_sync": "2026-06-09T12:00:00.000Z",
 *     "atualizado_em": "2026-06-09T12:00:00.000Z",
 *     "token_status": "ativo" | "expirado"
 *   }
 * ]
 */

const { Pool } = require('pg');
const jwt = require('jsonwebtoken');

// Pool para banco central 'bling'
let blingPool = null;

function getBlingPool() {
  if (!blingPool) {
    blingPool = new Pool({
      host: process.env.BLING_HOST || '37.60.236.200',
      port: process.env.BLING_PORT || 5432,
      user: process.env.BLING_USER || 'postgres',
      password: process.env.BLING_PASSWORD || '131105Gv',
      database: 'bling',
      max: 2
    });
  }
  return blingPool;
}

function setCORSHeaders(res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
}

/**
 * Verifica e valida JWT token
 */
function verifyToken(req, res) {
  const authHeader = req.headers.authorization || '';
  const token = authHeader.split(' ')[1];

  if (!token) {
    res.status(401).json({ error: 'Token não fornecido' });
    return null;
  }

  try {
    const payload = jwt.verify(token, process.env.JWT_SECRET || 'test_secret');
    return payload;
  } catch (e) {
    res.status(401).json({ error: 'Token inválido ou expirado' });
    return null;
  }
}

/**
 * Calcula o status do token (ativo / expirado / sem data)
 */
function getTokenStatus(expiresAt) {
  if (!expiresAt) return 'desconhecido';
  const expDate = new Date(expiresAt);
  return expDate > new Date() ? 'ativo' : 'expirado';
}

async function handler(req, res) {
  setCORSHeaders(res);

  // Handle CORS preflight
  if (req.method === 'OPTIONS') {
    return res.status(200).end();
  }

  // Apenas GET permitido
  if (req.method !== 'GET') {
    return res.status(405).json({ error: 'Método não permitido' });
  }

  // Verificar token
  const payload = verifyToken(req, res);
  if (!payload) return;

  try {
    // Extrair empresa da query string ou do token
    const empresa = (req.query.empresa || payload.company || 'autoequip').toLowerCase().trim();

    console.log(`[BLING-CLIENTES] Listando clientes para empresa: ${empresa}`);

    const pool = getBlingPool();
    const result = await pool.query(
      `SELECT id, nome, empresa, client_id, access_token, refresh_token, expires_at, last_sync, atualizado_em
       FROM clientes
       WHERE empresa = $1
       ORDER BY nome ASC`,
      [empresa]
    );

    // Enriquecer resposta com status do token
    const clientes = result.rows.map(row => ({
      ...row,
      token_status: getTokenStatus(row.expires_at),
      // Mascarar tokens sensíveis para o frontend
      client_secret: undefined,
      refresh_token: row.refresh_token ? '***' : null
    }));

    console.log(`[BLING-CLIENTES] Encontrados ${clientes.length} clientes para ${empresa}`);
    res.json(clientes);

  } catch (error) {
    console.error('[BLING-CLIENTES] Erro:', error.message);
    res.status(500).json({ error: 'Erro ao listar clientes Bling' });
  }
}

module.exports = handler;
