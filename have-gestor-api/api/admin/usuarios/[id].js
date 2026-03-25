/**
 * PATCH /api/admin/usuarios/:id - Update user
 * DELETE /api/admin/usuarios/:id - Deactivate user
 */

const jwt = require('jsonwebtoken');
const auth = require('../../../lib/auth');
const db = require('../../../lib/db');

// CORS headers
function setCORSHeaders(res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'PATCH, DELETE, OPTIONS');
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
 * PATCH - Update user
 */
async function handleUpdateUsuario(req, res, id) {
  const payload = verifyAdminToken(req, res);
  if (!payload) return;

  try {
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
    console.error('[ADMIN] Update user failed:', e.message);
    res.status(500).json({ error: 'Erro ao atualizar usuário' });
  }
}

/**
 * DELETE - Deactivate user
 */
async function handleDeleteUsuario(req, res, id) {
  const payload = verifyAdminToken(req, res);
  if (!payload) return;

  try {
    if (!id) {
      return res.status(400).json({ error: 'User ID required' });
    }

    const deletedUser = await db.deactivateUser(payload.company, parseInt(id));

    console.log(`[ADMIN] User ID ${id} deactivated by ${payload.user}`);
    res.json({
      message: 'Usuário desativado com sucesso',
      user: deletedUser
    });
  } catch (e) {
    console.error('[ADMIN] Delete user failed:', e.message);
    res.status(500).json({ error: 'Erro ao desativar usuário' });
  }
}

module.exports = async (req, res) => {
  setCORSHeaders(res);

  if (req.method === 'OPTIONS') {
    return res.status(200).end();
  }

  const id = req.query.id;

  try {
    if (req.method === 'PATCH') {
      return await handleUpdateUsuario(req, res, id);
    }
    if (req.method === 'DELETE') {
      return await handleDeleteUsuario(req, res, id);
    }

    res.status(405).json({ error: 'Method not allowed' });
  } catch (e) {
    console.error('[ADMIN] Unhandled error:', e.message);
    res.status(500).json({ error: 'Internal server error' });
  }
};
