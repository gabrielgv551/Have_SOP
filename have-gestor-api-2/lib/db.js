const { Pool } = require('pg');
const companies = require('./companies');

const pools = {};

function getPool(company) {
  if (pools[company]) return pools[company];
  const key = companies[company].dbEnvKey;
  const e = k => (process.env[`${key}_${k}`] || '').trim();
  pools[company] = new Pool({
    host:     e('HOST'),
    port:     parseInt(e('PORT') || '5432'),
    database: e('DB'),
    user:     e('USER'),
    password: e('PASSWORD'),
    ssl:      { rejectUnauthorized: false },
    max:      5,
  });
  return pools[company];
}

module.exports = { getPool };
