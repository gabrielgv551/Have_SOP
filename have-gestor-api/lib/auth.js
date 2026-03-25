const jwt = require('jsonwebtoken');
const bcrypt = require('bcrypt');

function isAdmin(payload) {
  return payload.role === 'admin' || payload.user === 'admin';
}

function validatePasswordStrength(password) {
  if (!password || password.length < 8) {
    return { valid: false, error: 'Senha deve ter no mínimo 8 caracteres' };
  }
  return { valid: true };
}

async function isUsernameUnique(pool, company, username) {
  try {
    const res = await pool.query('SELECT 1 FROM usuarios WHERE usuario = $1', [username]);
    return res.rowCount === 0;
  } catch (e) {
    console.error('isUsernameUnique error:', e);
    return false;
  }
}

async function hashPassword(password) {
  return await bcrypt.hash(password, 10);
}

function protect(allowedRoles) {
  return function (handler) {
    return async function (req, res) {
      const authHeader = req.headers.authorization;
      if (!authHeader || !authHeader.startsWith('Bearer ')) {
        return res.status(401).json({ error: 'Token não fornecido ou mal formatado' });
      }

      const token = authHeader.split(' ')[1];

      try {
        const decoded = jwt.verify(token, process.env.JWT_SECRET);

        if (allowedRoles && !allowedRoles.includes(decoded.role)) {
          return res.status(403).json({ error: 'Acesso negado. Permissões insuficientes.' });
        }

        return handler(req, res, decoded);
      } catch (error) {
        return res.status(401).json({ error: 'Token inválido ou expirado' });
      }
    };
  };
}

module.exports = {
  isAdmin,
  validatePasswordStrength,
  isUsernameUnique,
  hashPassword,
  protect
};