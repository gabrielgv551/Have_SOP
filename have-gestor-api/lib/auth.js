const bcrypt = require('bcrypt');
const { Pool } = require('pg');

// Constants
const BCRYPT_SALT_ROUNDS = 10;

/**
 * Hash a password using bcrypt
 * @param {string} password - Plain text password
 * @returns {Promise<string>} - Bcrypt hash
 */
async function hashPassword(password) {
  if (!password || password.length < 8) {
    throw new Error('Password must be at least 8 characters');
  }
  return await bcrypt.hash(password, BCRYPT_SALT_ROUNDS);
}

/**
 * Verify a password against its hash
 * @param {string} password - Plain text password
 * @param {string} hash - Bcrypt hash
 * @returns {Promise<boolean>} - True if password matches
 */
async function verifyPassword(password, hash) {
  try {
    return await bcrypt.compare(password, hash);
  } catch (e) {
    console.error('[AUTH] Password verification failed:', e.message);
    return false;
  }
}

/**
 * Validate user credentials in database
 * @param {Pool} pool - PostgreSQL connection pool
 * @param {string} empresa - Company name
 * @param {string} usuario - Username
 * @param {string} password - Plain text password
 * @returns {Promise<Object|null>} - User object with id, nome, perfil or null if invalid
 */
async function validateUserInDB(pool, empresa, usuario, password) {
  try {
    const result = await pool.query(
      'SELECT id, nome, usuario, perfil, senha_hash, ativo FROM usuarios WHERE empresa = $1 AND usuario = $2',
      [empresa, usuario]
    );

    if (result.rows.length === 0) {
      return null; // User not found
    }

    const user = result.rows[0];

    // Check if user is active
    if (!user.ativo) {
      console.error(`[AUTH] User ${usuario} is inactive`);
      return null;
    }

    // Verify password
    const passwordMatch = await verifyPassword(password, user.senha_hash);
    if (!passwordMatch) {
      console.error(`[AUTH] Invalid password for ${usuario}`);
      return null;
    }

    // Return user data without hash
    return {
      id: user.id,
      nome: user.nome,
      usuario: user.usuario,
      perfil: user.perfil,
      empresa
    };
  } catch (e) {
    console.error('[AUTH] Database validation failed:', e.message);
    return null;
  }
}

/**
 * Check if JWT payload is admin
 * @param {Object} payload - JWT payload { company, user, perfil, ... }
 * @returns {boolean} - True if user is admin
 */
function isAdmin(payload) {
  return payload && payload.perfil === 'admin';
}

/**
 * Get user from database by ID
 * @param {Pool} pool - PostgreSQL connection pool
 * @param {number} userId - User ID
 * @returns {Promise<Object|null>} - User object or null
 */
async function getUserById(pool, userId) {
  try {
    const result = await pool.query(
      'SELECT id, empresa, nome, usuario, perfil, ativo, criado_em, atualizado_em FROM usuarios WHERE id = $1',
      [userId]
    );
    return result.rows[0] || null;
  } catch (e) {
    console.error('[AUTH] Get user by ID failed:', e.message);
    return null;
  }
}

/**
 * Check if username is unique for a company
 * @param {Pool} pool - PostgreSQL connection pool
 * @param {string} empresa - Company name
 * @param {string} usuario - Username
 * @param {number|null} excludeId - User ID to exclude (for edit operations)
 * @returns {Promise<boolean>} - True if username is unique
 */
async function isUsernameUnique(pool, empresa, usuario, excludeId = null) {
  try {
    let query = 'SELECT COUNT(*) FROM usuarios WHERE empresa = $1 AND usuario = $2';
    let params = [empresa, usuario];

    if (excludeId) {
      query += ' AND id != $3';
      params.push(excludeId);
    }

    const result = await pool.query(query, params);
    return parseInt(result.rows[0].count) === 0;
  } catch (e) {
    console.error('[AUTH] Username uniqueness check failed:', e.message);
    return false;
  }
}

/**
 * Validate password strength
 * @param {string} password - Password to validate
 * @returns {Object} - { valid: boolean, error: string|null }
 */
function validatePasswordStrength(password) {
  if (!password) {
    return { valid: false, error: 'Password is required' };
  }
  if (password.length < 8) {
    return { valid: false, error: 'Password must be at least 8 characters' };
  }
  if (password.length > 128) {
    return { valid: false, error: 'Password must be less than 128 characters' };
  }
  return { valid: true, error: null };
}

module.exports = {
  hashPassword,
  verifyPassword,
  validateUserInDB,
  isAdmin,
  getUserById,
  isUsernameUnique,
  validatePasswordStrength
};
