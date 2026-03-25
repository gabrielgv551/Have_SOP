#!/usr/bin/env node
/**
 * Test script for admin user management API
 * Run: node test-admin.js (requires vercel dev running on localhost:3000)
 */

const http = require('http');

const API_BASE = 'http://localhost:3000';
let adminToken = null;

async function makeRequest(method, path, body = null, token = null) {
  return new Promise((resolve, reject) => {
    const url = new URL(API_BASE + path);
    const options = {
      hostname: url.hostname,
      port: url.port,
      path: url.pathname + url.search,
      method: method,
      headers: {
        'Content-Type': 'application/json'
      }
    };

    if (token) {
      options.headers['Authorization'] = `Bearer ${token}`;
    }

    const req = http.request(options, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        resolve({
          status: res.statusCode,
          body: data ? JSON.parse(data) : null
        });
      });
    });

    req.on('error', reject);
    if (body) req.write(JSON.stringify(body));
    req.end();
  });
}

function printTest(name, passed, detail = '') {
  const icon = passed ? '✅' : '❌';
  console.log(`${icon} ${name}`);
  if (detail) console.log(`   ${detail}`);
}

async function runTests() {
  console.log('🧪 Testing Admin User Management API\n');

  try {
    // Test 1: Login as admin
    console.log('=== Test 1: Login as admin ===');
    let res = await makeRequest('POST', '/api/login', {
      company: 'lanzi',
      user: 'admin',
      password: 'lanzi2024'
    });

    if (res.status === 200 && res.body.token && res.body.perfil === 'admin') {
      printTest('Admin login', true, `Token: ${res.body.token.substring(0, 20)}...`);
      adminToken = res.body.token;
    } else {
      printTest('Admin login', false, `Status: ${res.status}, Body: ${JSON.stringify(res.body)}`);
      console.log('\n⚠️  Skipping remaining tests - admin login failed');
      console.log('Make sure vercel dev is running and env vars are set correctly');
      return;
    }

    // Test 2: List users (requires admin)
    console.log('\n=== Test 2: Get usuarios list ===');
    res = await makeRequest('GET', '/api/admin/usuarios', null, adminToken);

    if (res.status === 200 && Array.isArray(res.body)) {
      printTest('Get usuarios', true, `Found ${res.body.length} users`);
    } else {
      printTest('Get usuarios', false, `Status: ${res.status}`);
    }

    // Test 3: Create new user
    console.log('\n=== Test 3: Create new user ===');
    const newUser = {
      nome: 'Test User',
      usuario: `testuser_${Date.now()}`,
      password: 'TestPassword123',
      perfil: 'gestor',
      empresa: 'lanzi'
    };

    res = await makeRequest('POST', '/api/admin/usuarios', newUser, adminToken);

    let createdUserId = null;
    if (res.status === 201 && res.body.user && res.body.user.id) {
      printTest('Create user', true, `ID: ${res.body.user.id}, Usuario: ${res.body.user.usuario}`);
      createdUserId = res.body.user.id;
    } else {
      printTest('Create user', false, `Status: ${res.status}, Body: ${JSON.stringify(res.body)}`);
    }

    // Test 4: Try to create duplicate user
    console.log('\n=== Test 4: Reject duplicate username ===');
    res = await makeRequest('POST', '/api/admin/usuarios', newUser, adminToken);

    if (res.status === 409) {
      printTest('Duplicate prevention', true, 'Correctly rejected');
    } else {
      printTest('Duplicate prevention', false, `Status: ${res.status} (expected 409)`);
    }

    // Test 5: Create user without admin token
    console.log('\n=== Test 5: Reject non-admin ===');
    res = await makeRequest('POST', '/api/admin/usuarios', {
      nome: 'Hacker',
      usuario: 'hacker',
      password: 'password123',
      perfil: 'admin'
    }, null); // No token

    if (res.status === 401) {
      printTest('Missing token rejection', true);
    } else {
      printTest('Missing token rejection', false, `Status: ${res.status}`);
    }

    // Test 6: Update user
    if (createdUserId) {
      console.log('\n=== Test 6: Update user ===');
      res = await makeRequest('PATCH', `/api/admin/usuarios/${createdUserId}`, {
        nome: 'Updated Name',
        perfil: 'admin'
      }, adminToken);

      if (res.status === 200 && res.body.user.nome === 'Updated Name') {
        printTest('Update user', true, `Updated nome and perfil`);
      } else {
        printTest('Update user', false, `Status: ${res.status}`);
      }
    }

    // Test 7: Reset password
    if (createdUserId) {
      console.log('\n=== Test 7: Reset password ===');
      res = await makeRequest('PUT', `/api/admin/usuarios/${createdUserId}/reset-password`, {
        tempPassword: 'NewPassword123'
      }, adminToken);

      if (res.status === 200) {
        printTest('Reset password', true);
      } else {
        printTest('Reset password', false, `Status: ${res.status}`);
      }
    }

    // Test 8: Deactivate user
    if (createdUserId) {
      console.log('\n=== Test 8: Deactivate user ===');
      res = await makeRequest('DELETE', `/api/admin/usuarios/${createdUserId}`, null, adminToken);

      if (res.status === 200 && res.body.user.ativo === false) {
        printTest('Deactivate user', true);
      } else {
        printTest('Deactivate user', false, `Status: ${res.status}`);
      }
    }

    // Test 9: Login with new user (old password)
    console.log('\n=== Test 9: New user can login ===');
    res = await makeRequest('POST', '/api/login', {
      company: 'lanzi',
      user: newUser.usuario,
      password: 'TestPassword123'
    });

    if (res.status === 200 && res.body.token) {
      printTest('New user login', true, 'Successfully authenticated');
    } else {
      printTest('New user login', false, `Status: ${res.status}`);
    }

    // Test 10: Invalid JWT should be rejected
    console.log('\n=== Test 10: Invalid token rejection ===');
    res = await makeRequest('GET', '/api/admin/usuarios', null, 'invalid.token.here');

    if (res.status === 401) {
      printTest('Invalid token rejection', true);
    } else {
      printTest('Invalid token rejection', false, `Status: ${res.status}`);
    }

    console.log('\n✅ All tests completed!\n');
    console.log('Next steps:');
    console.log('  1. Run migration: psql -h 37.60.236.200 -U postgres -d Lanzi -f migrations/001_create_usuarios.sql');
    console.log('  2. Generate bcrypt hashes for initial users (admin/gestor/have)');
    console.log('  3. Deploy to Vercel: git push');
    console.log('  4. Test frontend admin panel');

  } catch (e) {
    console.error('❌ ERRO:', e.message);
    console.log('\nVerifique:');
    console.log('  1. vercel dev está rodando em localhost:3000?');
    console.log('  2. .env.local tem as variáveis de ambiente?');
    console.log('  3. PostgreSQL 37.60.236.200 está acessível?');
  }
}

runTests();
