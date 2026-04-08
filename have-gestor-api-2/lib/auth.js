const jwt = require('jsonwebtoken');

function verifyToken(req) {
  const auth = (req.headers.authorization || '').split(' ')[1];
  if (!auth) throw new Error('Token não fornecido');
  return jwt.verify(auth, process.env.JWT_SECRET);
}

module.exports = { verifyToken };
