const { Pool } = require('pg');

// Cache of pools per company (reused from data.js pattern)
const pools = {};

/**
 * Get or create a connection pool for a company
 * @param {string} company - Company slug (e.g., 'lanzi')
 * @returns {Pool} - PostgreSQL connection pool
 */
function getPool(company) {
  if (pools[company]) return pools[company];

  // Get company config (usuarios table is in the same database)
  // For simplicity, assume all users are in the main database for each company
  const key = company.toUpperCase();

  pools[company] = new Pool({
    host: process.env[`${key}_HOST`],
    port: parseInt(process.env[`${key}_PORT`] || '5432'),
    database: process.env[`${key}_DB`],
    user: process.env[`${key}_USER`],
    password: process.env[`${key}_PASSWORD`],
    ssl: { rejectUnauthorized: false },
    max: 5,
  });

  return pools[company];
}

/**
 * Execute query on usuarios table
 * @param {string} company - Company slug
 * @param {string} sql - SQL query with placeholders ($1, $2, etc)
 * @param {Array} params - Query parameters
 * @returns {Promise<Object>} - Query result { rows, rowCount }
 */
async function queryUsuarios(company, sql, params = []) {
  const pool = getPool(company);

  try {
    const result = await pool.query(sql, params);
    return result;
  } catch (e) {
    console.error(`[DB] Query failed for ${company}:`, e.message);
    throw new Error('Database query failed: ' + e.message);
  }
}

/**
 * Get all users for a company
 * @param {string} company - Company slug
 * @returns {Promise<Array>} - Array of user objects
 */
async function getAllUsers(company) {
  const result = await queryUsuarios(
    company,
    'SELECT id, nome, usuario, perfil, ativo, criado_em, atualizado_em FROM usuarios WHERE empresa = $1 ORDER BY criado_em DESC',
    [company]
  );
  return result.rows;
}

/**
 * Create a new user
 * @param {string} company - Company slug
 * @param {Object} userData - { nome, usuario, senha_hash, perfil }
 * @returns {Promise<Object>} - Created user object
 */
async function createUser(company, userData) {
  const { nome, usuario, senha_hash, perfil } = userData;

  if (!nome || !usuario || !senha_hash || !perfil) {
    throw new Error('Missing required fields: nome, usuario, senha_hash, perfil');
  }

  const result = await queryUsuarios(
    company,
    'INSERT INTO usuarios (empresa, nome, usuario, senha_hash, perfil, ativo) VALUES ($1, $2, $3, $4, $5, TRUE) RETURNING id, nome, usuario, perfil, ativo, criado_em',
    [company, nome, usuario, senha_hash, perfil]
  );

  if (result.rows.length === 0) {
    throw new Error('Failed to create user');
  }

  return result.rows[0];
}

/**
 * Update user
 * @param {string} company - Company slug
 * @param {number} userId - User ID
 * @param {Object} updates - Fields to update { nome, perfil, ativo, senha_hash }
 * @returns {Promise<Object>} - Updated user object
 */
async function updateUser(company, userId, updates) {
  const { nome, perfil, ativo, senha_hash } = updates;
  const fields = [];
  const params = [userId, company];
  let paramIndex = 3;

  if (nome !== undefined) {
    fields.push(`nome = $${paramIndex}`);
    params.push(nome);
    paramIndex++;
  }

  if (perfil !== undefined) {
    fields.push(`perfil = $${paramIndex}`);
    params.push(perfil);
    paramIndex++;
  }

  if (ativo !== undefined) {
    fields.push(`ativo = $${paramIndex}`);
    params.push(ativo);
    paramIndex++;
  }

  if (senha_hash !== undefined) {
    fields.push(`senha_hash = $${paramIndex}`);
    params.push(senha_hash);
    paramIndex++;
  }

  if (fields.length === 0) {
    throw new Error('No fields to update');
  }

  const sql = `
    UPDATE usuarios
    SET ${fields.join(', ')}
    WHERE id = $1 AND empresa = $2
    RETURNING id, nome, usuario, perfil, ativo, criado_em, atualizado_em
  `;

  const result = await queryUsuarios(company, sql, params);

  if (result.rows.length === 0) {
    throw new Error('User not found or update failed');
  }

  return result.rows[0];
}

/**
 * Deactivate user (soft delete)
 * @param {string} company - Company slug
 * @param {number} userId - User ID
 * @returns {Promise<Object>} - Deactivated user object
 */
async function deactivateUser(company, userId) {
  const result = await queryUsuarios(
    company,
    'UPDATE usuarios SET ativo = FALSE WHERE id = $1 AND empresa = $2 RETURNING id, nome, usuario, perfil, ativo, criado_em, atualizado_em',
    [userId, company]
  );

  if (result.rows.length === 0) {
    throw new Error('User not found');
  }

  return result.rows[0];
}

/**
 * Reset user password
 * @param {string} company - Company slug
 * @param {number} userId - User ID
 * @param {string} newPasswordHash - New bcrypt hash
 * @returns {Promise<Object>} - Updated user object
 */
async function resetPassword(company, userId, newPasswordHash) {
  const result = await queryUsuarios(
    company,
    'UPDATE usuarios SET senha_hash = $1 WHERE id = $2 AND empresa = $3 RETURNING id, nome, usuario, perfil, ativo',
    [newPasswordHash, userId, company]
  );

  if (result.rows.length === 0) {
    throw new Error('User not found');
  }

  return result.rows[0];
}

module.exports = {
  getPool,
  queryUsuarios,
  getAllUsers,
  createUser,
  updateUser,
  deactivateUser,
  resetPassword
};
