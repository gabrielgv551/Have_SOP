const jwt = require('jsonwebtoken');
const auth = require('../lib/auth');
const db = require('../lib/db');

// CORS headers
function setCORSHeaders(res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PATCH, DELETE, PUT, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
}

/**
 * Verify JWT token and extract admin payload
 * @returns {Object|null} - Verified payload or null
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

    // Check if user is admin
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
 * GET /api/admin/usuarios - List all users
 */
async function handleGetUsuarios(req, res) {
  const payload = verifyAdminToken(req, res);
  if (!payload) return;

  try {
    // Admin can see users of their own company or all companies
    // For now, return all users for the admin's company
    const users = await db.getAllUsers(payload.company);
    res.json(users);
  } catch (e) {
    console.error('[ADMIN] Get usuarios failed:', e.message);
    res.status(500).json({ error: 'Erro ao buscar usuários' });
  }
}

/**
 * POST /api/admin/usuarios - Create new user
 */
async function handleCreateUsuario(req, res) {
  const payload = verifyAdminToken(req, res);
  if (!payload) return;

  try {
    const { nome, usuario, password, perfil, empresa } = req.body;

    // Validate input
    if (!nome || !usuario || !password || !perfil) {
      return res.status(400).json({
        error: 'Campos obrigatórios: nome, usuario, password, perfil'
      });
    }

    if (!['admin', 'gestor', 'have'].includes(perfil)) {
      return res.status(400).json({ error: 'Perfil inválido' });
    }

    // Validate password strength
    const pwValidation = auth.validatePasswordStrength(password);
    if (!pwValidation.valid) {
      return res.status(400).json({ error: pwValidation.error });
    }

    // Use provided empresa or default to admin's company
    const targetCompany = empresa || payload.company;

    // Check if username is unique
    const isUnique = await auth.isUsernameUnique(
      db.getPool(targetCompany),
      targetCompany,
      usuario
    );

    if (!isUnique) {
      return res.status(409).json({
        error: `Usuário '${usuario}' já existe para a empresa ${targetCompany}`
      });
    }

    // Hash password
    const senhaHash = await auth.hashPassword(password);

    // Create user
    const newUser = await db.createUser(targetCompany, {
      nome,
      usuario,
      senha_hash: senhaHash,
      perfil
    });

    console.log(`[ADMIN] User '${usuario}' created by ${payload.user}`);
    res.status(201).json({
      message: 'Usuário criado com sucesso',
      user: newUser
    });
  } catch (e) {
    console.error('[ADMIN] Create usuario failed:', e.message);
    res.status(500).json({ error: 'Erro ao criar usuário' });
  }
}

/**
 * PATCH /api/admin/usuarios/:id - Update user
 */
async function handleUpdateUsuario(req, res) {
  const payload = verifyAdminToken(req, res);
  if (!payload) return;

  try {
    const { id } = req.params;
    const { nome, perfil, ativo } = req.body;

    if (!id) {
      return res.status(400).json({ error: 'User ID required' });
    }

    if (perfil && !['admin', 'gestor', 'have'].includes(perfil)) {
      return res.status(400).json({ error: 'Perfil inválido' });
    }

    const updates = {};
    if (nome !== undefined) updates.nome = nome;
    if (perfil !== undefined) updates.perfil = perfil;
    if (ativo !== undefined) updates.ativo = ativo;

    if (Object.keys(updates).length === 0) {
      return res.status(400).json({ error: 'Nenhum campo para atualizar' });
    }

    const updatedUser = await db.updateUser(payload.company, parseInt(id), updates);

    console.log(`[ADMIN] User ID ${id} updated by ${payload.user}`);
    res.json({
      message: 'Usuário atualizado com sucesso',
      user: updatedUser
    });
  } catch (e) {
    console.error('[ADMIN] Update usuario failed:', e.message);
    res.status(500).json({ error: 'Erro ao atualizar usuário' });
  }
}

/**
 * DELETE /api/admin/usuarios/:id - Deactivate user
 */
async function handleDeleteUsuario(req, res) {
  const payload = verifyAdminToken(req, res);
  if (!payload) return;

  try {
    const { id } = req.params;

    if (!id) {
      return res.status(400).json({ error: 'User ID required' });
    }

    // Soft delete: just set ativo = false
    const deactivatedUser = await db.deactivateUser(payload.company, parseInt(id));

    console.log(`[ADMIN] User ID ${id} deactivated by ${payload.user}`);
    res.json({
      message: 'Usuário desativado com sucesso',
      user: deactivatedUser
    });
  } catch (e) {
    console.error('[ADMIN] Delete usuario failed:', e.message);
    res.status(500).json({ error: 'Erro ao desativar usuário' });
  }
}

/**
 * PUT /api/admin/usuarios/:id/reset-password - Reset user password
 */
async function handleResetPassword(req, res) {
  const payload = verifyAdminToken(req, res);
  if (!payload) return;

  try {
    const { id } = req.params;
    const { tempPassword } = req.body;

    if (!id) {
      return res.status(400).json({ error: 'User ID required' });
    }

    if (!tempPassword) {
      return res.status(400).json({ error: 'tempPassword is required' });
    }

    // Validate password strength
    const pwValidation = auth.validatePasswordStrength(tempPassword);
    if (!pwValidation.valid) {
      return res.status(400).json({ error: pwValidation.error });
    }

    // Hash new password
    const senhaHash = await auth.hashPassword(tempPassword);

    // Update password
    const updatedUser = await db.resetPassword(payload.company, parseInt(id), senhaHash);

    console.log(`[ADMIN] Password reset for user ID ${id} by ${payload.user}`);
    res.json({
      message: 'Senha atualizada com sucesso',
      user: updatedUser
    });
  } catch (e) {
    console.error('[ADMIN] Reset password failed:', e.message);
    res.status(500).json({ error: 'Erro ao resetar senha' });
  }
}

/**
 * Main handler
 */
module.exports = async (req, res) => {
  setCORSHeaders(res);

  if (req.method === 'OPTIONS') {
    return res.status(200).end();
  }

  const path = req.url.split('?')[0]; // Remove query string
  const segments = path.split('/').filter(Boolean); // ['admin', 'usuarios', '1', 'reset-password']

  try {
    if (segments[0] !== 'admin') {
      return res.status(404).json({ error: 'Not found' });
    }

    // /api/admin/usuarios - GET (list) or POST (create)
    if (segments[1] === 'usuarios' && !segments[2]) {
      if (req.method === 'GET') {
        return await handleGetUsuarios(req, res);
      }
      if (req.method === 'POST') {
        return await handleCreateUsuario(req, res);
      }
    }

    // /api/admin/usuarios/:id - PATCH (update) or DELETE (deactivate)
    if (segments[1] === 'usuarios' && segments[2] && !segments[3]) {
      if (req.method === 'PATCH') {
        return await handleUpdateUsuario(req, res);
      }
      if (req.method === 'DELETE') {
        return await handleDeleteUsuario(req, res);
      }
    }

    // /api/admin/usuarios/:id/reset-password - PUT
    if (segments[1] === 'usuarios' && segments[2] && segments[3] === 'reset-password') {
      if (req.method === 'PUT') {
        return await handleResetPassword(req, res);
      }
    }

    // No match
    res.status(404).json({ error: 'Endpoint not found' });
  } catch (e) {
    console.error('[ADMIN] Unhandled error:', e.message);
    res.status(500).json({ error: 'Internal server error' });
  }
};
