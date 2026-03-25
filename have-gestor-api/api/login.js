const companies = require('../lib/companies');
const jwt = require('jsonwebtoken');
const auth = require('../lib/auth');
const db = require('../lib/db');

module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'POST') return res.status(405).end();

  const { company, user, password } = req.body || {};

  if (!company || !user || !password)
    return res.status(400).json({ error: 'Campos obrigatórios: company, user, password' });

  const co = companies[company];
  if (!co)
    return res.status(401).json({ error: 'Empresa não encontrada' });

  try {
    // Try to authenticate from database first
    const pool = db.getPool(company);
    const dbUser = await auth.validateUserInDB(pool, company, user, password);

    if (dbUser) {
      // User found in database and password matches
      const token = jwt.sign(
        { company, user: dbUser.usuario, perfil: dbUser.perfil, companyName: co.name },
        process.env.JWT_SECRET,
        { expiresIn: '8h' }
      );

      console.log(`[LOGIN] User '${user}' authenticated from database for company '${company}'`);
      return res.json({ token, companyName: co.name, user: dbUser.usuario, perfil: dbUser.perfil });
    }

    // Fall back to environment variables (backward compatibility)
    // Only try this if database lookup failed
    if (co.users[user] && co.users[user] === password) {
      // Determine perfil from env var key (default to 'gestor')
      let perfil = 'gestor';
      if (user === 'admin') perfil = 'admin';
      else if (user === 'have') perfil = 'have';

      const token = jwt.sign(
        { company, user, perfil, companyName: co.name },
        process.env.JWT_SECRET,
        { expiresIn: '8h' }
      );

      console.log(`[LOGIN] User '${user}' authenticated from env vars for company '${company}' (fallback)`);
      return res.json({ token, companyName: co.name, user, perfil });
    }

    // Neither database nor env vars worked
    console.warn(`[LOGIN] Failed login attempt for user '${user}' at company '${company}'`);
    return res.status(401).json({ error: 'Usuário ou senha incorretos' });
  } catch (e) {
    console.error('[LOGIN] Authentication error:', e.message);
    // On error, still allow fallback to env vars
    if (co.users[user] && co.users[user] === password) {
      let perfil = 'gestor';
      if (user === 'admin') perfil = 'admin';
      else if (user === 'have') perfil = 'have';

      const token = jwt.sign(
        { company, user, perfil, companyName: co.name },
        process.env.JWT_SECRET,
        { expiresIn: '8h' }
      );

      return res.json({ token, companyName: co.name, user, perfil });
    }

    return res.status(500).json({ error: 'Erro ao autenticar. Tente novamente.' });
  }
};
