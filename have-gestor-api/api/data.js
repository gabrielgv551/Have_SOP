const companies = require('../lib/companies');
const jwt = require('jsonwebtoken');
const { Pool } = require('pg');

// Whitelist de tabelas permitidas — segurança contra SQL injection via nome de tabela
const TABELAS_PERMITIDAS = [
  'curva_abc',
  'ponto_pedido',
  'estoque_seguranca',
  'ppr_sku',
  'forecast_12m',
  'semana_pedidos',
];

// Cache simples de pools por empresa (evita criar nova conexão a cada request)
const pools = {};

function getPool(company) {
  if (pools[company]) return pools[company];
  const key = companies[company].dbEnvKey;
  pools[company] = new Pool({
    host:     process.env[`${key}_HOST`],
    port:     parseInt(process.env[`${key}_PORT`] || '5432'),
    database: process.env[`${key}_DB`],
    user:     process.env[`${key}_USER`],
    password: process.env[`${key}_PASSWORD`],
    ssl:      { rejectUnauthorized: false },
    max:      5,
  });
  return pools[company];
}

module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');

  if (req.method === 'OPTIONS') return res.status(200).end();

  // 1. Verificar token JWT
  const auth = (req.headers.authorization || '').split(' ')[1];
  if (!auth) return res.status(401).json({ error: 'Token não fornecido' });

  let payload;
  try {
    payload = jwt.verify(auth, process.env.JWT_SECRET);
  } catch {
    return res.status(401).json({ error: 'Token inválido ou expirado. Faça login novamente.' });
  }

  // 2. Verificar tabela (whitelist)
  const { tabela } = req.query;
  if (!tabela || !TABELAS_PERMITIDAS.includes(tabela))
    return res.status(400).json({ error: `Tabela '${tabela}' não permitida.` });

  // 3. Query no banco da empresa correta
  const pool = getPool(payload.company);

  try {
    const result = await pool.query(`SELECT * FROM ${tabela} LIMIT 5000`);
    res.json(result.rows);
  } catch (e) {
    console.error(`[ERRO] ${payload.company} / ${tabela}:`, e.message);
    res.status(500).json({ error: e.message });
  }
};
