/**
 * PUT /api/admin/usuarios/:id/reset-password - Reset user password
 */

const jwt = require('jsonwebtoken');
const auth = require('../../../../lib/auth');
const db = require('../../../../lib/db');

// CORS headers
function setCORSHeaders(res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'PUT, OPTIONS');
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
 * PUT - Reset password
 */
async function handleResetPassword(req, res, userId) {
  const payload = verifyAdminToken(req, res);
  if (!payload) return;

  try {
    const { tempPassword } = req.body;

    if (!userId) {
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
    const updatedUser = await db.resetPassword(payload.company, parseInt(userId), senhaHash);

    console.log(`[ADMIN] Password reset for user ID ${userId} by ${payload.user}`);
    res.json({
      message: 'Senha atualizada com sucesso',
      user: updatedUser
    });
  } catch (e) {
    console.error('[ADMIN] Reset password failed:', e.message);
    res.status(500).json({ error: 'Erro ao resetar senha' });
  }
}

module.exports = async (req, res) => {
  setCORSHeaders(res);

  if (req.method === 'OPTIONS') {
    return res.status(200).end();
  }

  const userId = req.query.id;

  try {
    if (req.method === 'PUT') {
      return await handleResetPassword(req, res, userId);
    }

    res.status(405).json({ error: 'Method not allowed' });
  } catch (e) {
    console.error('[ADMIN] Unhandled error:', e.message);
    res.status(500).json({ error: 'Internal server error' });
  }
};
