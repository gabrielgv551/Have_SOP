const bcrypt = require('bcrypt');
const jwt = require('jsonwebtoken');
const companies = require('../lib/companies');
const { getPool } = require('../lib/db');

// Rate limiting em memória: máx 10 tentativas por IP em 15 minutos
const loginAttempts = {};
const RATE_LIMIT = 10;
const RATE_WINDOW_MS = 15 * 60 * 1000;
function checkRateLimit(ip) {
  const now = Date.now();
  if (!loginAttempts[ip] || now - loginAttempts[ip].firstAt > RATE_WINDOW_MS) {
    loginAttempts[ip] = { count: 1, firstAt: now };
    return true;
  }
  loginAttempts[ip].count++;
  return loginAttempts[ip].count <= RATE_LIMIT;
}

module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');

  if (req.method === 'OPTIONS') return res.status(200).end();

  // Routing interno para /api/user-companies
  const url = req.url || '';
  if (url.includes('/user-companies')) {
    return handleUserCompanies(req, res);
  }

  if (req.method !== 'POST') return res.status(405).json({ error: 'Método não permitido' });

  const ip = req.headers['x-forwarded-for'] || req.socket?.remoteAddress || 'unknown';
  if (!checkRateLimit(ip)) {
    return res.status(429).json({ error: 'Muitas tentativas. Aguarde 15 minutos.' });
  }

  let body = req.body || {};
  if (typeof body === 'string') {
     try { body = JSON.parse(body); } catch(e) {}
  }

  const { email, password, company = 'lanzi' } = body;
  const usuarioInput = (email || '').toLowerCase().trim();

  if (!usuarioInput || !password) {
    return res.status(400).json({ error: 'Usuário e senha são obrigatórios' });
  }

  try {
    // 1. TENTATIVA VIA BANCO DE DADOS (só se o HOST estiver configurado)
    const companyKey = (companies[company] && companies[company].dbEnvKey) || company.toUpperCase();
    const dbHost = process.env[`${companyKey}_HOST`];

    if (dbHost) {
      const pool = getPool(company);
      const result = await pool.query('SELECT * FROM usuarios WHERE LOWER(usuario) = $1 AND ativo = TRUE', [usuarioInput]);
      const dbUser = result.rows[0];

      if (dbUser) {
        const isPasswordValid = await bcrypt.compare(password, dbUser.senha_hash);
        if (isPasswordValid) {
          const token = jwt.sign(
            { userId: dbUser.id, email: dbUser.email, user: dbUser.usuario, role: dbUser.perfil, company: company, nav_permissoes: dbUser.nav_permissoes || null },
            process.env.JWT_SECRET,
            { expiresIn: '8h' }
          );
          return res.status(200).json({ 
            token, 
            userName: dbUser.nome, 
            user: dbUser.usuario, 
            userRole: dbUser.perfil, 
            companyName: company.charAt(0).toUpperCase() + company.slice(1) 
          });
        }
      }
    }

    // 2. FALLBACK VIA VARIÁVEIS DE AMBIENTE (Configuradas no Vercel)
    const companyConfig = companies[company];
    if (companyConfig && companyConfig.users) {
      // O campo email pode ser 'admin', 'gestor' ou 'have'
      const envPassword = companyConfig.users[usuarioInput];
      
      if (envPassword && password === envPassword) {
        const role = usuarioInput; // admin, gestor ou have
        const token = jwt.sign(
          { userId: 0, email: `${usuarioInput}@have.com.br`, user: usuarioInput, role: role, company: company },
          process.env.JWT_SECRET,
          { expiresIn: '8h' }
        );
        return res.status(200).json({ 
          token, 
          userName: usuarioInput.charAt(0).toUpperCase() + usuarioInput.slice(1), 
          user: usuarioInput, 
          userRole: role, 
          companyName: companyConfig.name 
        });
      }
    }

    // Se chegou aqui, nada funcionou
    return res.status(401).json({ error: 'Credenciais inválidas' });

  } catch (error) {
    console.error('Erro no login:', error);
    // Mesmo com erro no banco, tenta o fallback antes de desistir
    const companyConfig = companies[company];
    if (companyConfig && companyConfig.users[usuarioInput] === password) {
       const role = usuarioInput;
       const token = jwt.sign({ userId: 0, user: usuarioInput, role, company }, process.env.JWT_SECRET, { expiresIn: '8h' });
       return res.status(200).json({ token, userName: usuarioInput, user: usuarioInput, userRole: role, companyName: companyConfig.name });
    }
    res.status(500).json({ error: 'Erro interno no servidor de autenticação' });
  }
};

// ── user-companies (consolidado aqui para economizar função serverless) ──
async function handleUserCompanies(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'POST') return res.status(405).json({ error: 'Método não permitido' });

  let body = req.body || {};
  if (typeof body === 'string') {
    try { body = JSON.parse(body); } catch(e) {}
  }

  const email = (body.email || '').toLowerCase().trim();
  if (!email) {
    return res.status(400).json({ error: 'Email obrigatório' });
  }

  const results = [];
  for (const [key, config] of Object.entries(companies)) {
    if (config.users && config.users[email]) {
      results.push({ key, name: config.name, tag: 'S&OP · PostgreSQL', color: getCompanyColor(key), source: 'env' });
      continue;
    }
    const dbHost = process.env[`${config.dbEnvKey}_HOST`];
    if (!dbHost) continue;
    try {
      const pool = getPool(key);
      const result = await pool.query(
        'SELECT 1 FROM usuarios WHERE LOWER(usuario) = $1 AND ativo = TRUE LIMIT 1',
        [email]
      );
      if (result.rowCount > 0) {
        results.push({ key, name: config.name, tag: 'S&OP · PostgreSQL', color: getCompanyColor(key), source: 'db' });
      }
    } catch (e) {
      console.error(`[user-companies] Erro ao verificar ${key}:`, e.message);
    }
  }
  return res.status(200).json({ companies: results });
}

function getCompanyColor(key) {
  switch (key) {
    case 'lanzi': return 'var(--accent)';
    case 'supershop': return '#9b27af';
    case 'marcon': return '#e67e22';
    case 'shopgra': return '#28a745';
    case 'autoequip': return '#2980b9';
    default: return '#666';
  }
}