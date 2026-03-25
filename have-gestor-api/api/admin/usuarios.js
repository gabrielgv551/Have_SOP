/**
 * GET /api/admin/usuarios - List all users
 * POST /api/admin/usuarios - Create new user
 */

const jwt = require('jsonwebtoken');
const auth = require('../../lib/auth');
const db = require('../../lib/db');

// CORS headers
function setCORSHeaders(res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
}

/**
 * Verify JWT token and extract admin payload
 */
function verifyAdminToken(req, res) {
  const authHeader = req.headers.authorization || '';
  const token = authHeader.split(' ')[1];

  if (!token) {
    res.status(401).json({ error: 'Token não fornecido' });
    return null;
  }

  try {
    const payload = jwt.verify(token, process.env.JWT_SECRET);

    if (!auth.isAdmin(payload)) {
      res.status(403).json({ error: 'Apenas admins podem acessar este recurso' });
      return null;
    }

    return payload;
  } catch (e) {
    res.status(401).json({ error: 'Token inválido ou expirado' });
    return null;
  }
}

/**
 * GET - List all users
 */
async function handleGetUsuarios(req, res) {
  const payload = verifyAdminToken(req, res);
  if (!payload) return;

  try {
    const users = await db.getAllUsers(payload.company);
    console.log(`[ADMIN] Listing ${users.length} users for company ${payload.company}`);
    res.json(users);
  } catch (e) {
    console.error('[ADMIN] Get usuarios failed:', e.message);
    res.status(500).json({ error: 'Erro ao listar usuários' });
  }
}

/**
 * POST - Create new user
 */
async function handleCreateUsuario(req, res) {
  const payload = verifyAdminToken(req, res);
  if (!payload) return;

  try {
    const { nome, usuario, password, perfil, empresa } = req.body;

    // Validation
    if (!nome || !usuario || !password || !perfil) {
      return res.status(400).json({ error: 'Campos obrigatórios: nome, usuario, password, perfil' });
    }

    if (!['admin', 'gestor', 'have'].includes(perfil)) {
      return res.status(400).json({ error: 'Perfil inválido' });
    }

    // Validate password strength
    const pwValidation = auth.validatePasswordStrength(password);
    if (!pwValidation.valid) {
      return res.status(400).json({ error: pwValidation.error });
    }

    // Check username uniqueness
    const isUnique = await auth.isUsernameUnique(
      db.getPool(payload.company),
      payload.company,
      usuario
    );
    if (!isUnique) {
      return res.status(409).json({ error: 'Nome de usuário já existe' });
    }

    // Hash password
    const senhaHash = await auth.hashPassword(password);

    // Create user
    const newUser = await db.createUser(payload.company, {
      nome,
      usuario,
      senha_hash: senhaHash,
      perfil,
      empresa: empresa || payload.company
    });

    console.log(`[ADMIN] User '${usuario}' created by ${payload.user}`);
    res.status(201).json({
      message: 'Usuário criado com sucesso',
      user: newUser
    });
  } catch (e) {
    console.error('[ADMIN] Create user failed:', e.message);
    res.status(500).json({ error: 'Erro ao criar usuário' });
  }
}

module.exports = async (req, res) => {
  setCORSHeaders(res);

  if (req.method === 'OPTIONS') {
    return res.status(200).end();
  }

  try {
    if (req.method === 'GET') {
      return await handleGetUsuarios(req, res);
    }
    if (req.method === 'POST') {
      return await handleCreateUsuario(req, res);
    }

    res.status(405).json({ error: 'Method not allowed' });
  } catch (e) {
    console.error('[ADMIN] Unhandled error:', e.message);
    res.status(500).json({ error: 'Internal server error' });
  }
};
