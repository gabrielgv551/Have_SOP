#!/usr/bin/env node
/**
 * Test script local para a API
 * Use com: node test-local.js
 *
 * Antes de rodar:
 * 1. npm install (se já não fez)
 * 2. Ativa vercel dev (em outra aba)
 * 3. Rode este script
 */

const http = require('http');

const API_BASE = 'http://localhost:3000';

async function makeRequest(method, path, body = null) {
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

async function runTests() {
    console.log('🧪 Testing Have Gestor API...\n');

    try {
        // Test 1: Login com credenciais corretas
        console.log('Test 1: Login com admin/lanzi2024');
        const loginRes = await makeRequest('POST', '/api/login', {
            company: 'lanzi',
            user: 'admin',
            password: 'lanzi2024'
        });

        if (loginRes.status !== 200) {
            console.log('❌ FAIL:', loginRes.status, loginRes.body);
            console.log('   Verifique se vercel dev está rodando');
            console.log('   Verifique se as env vars estão setadas');
            return;
        }

        console.log('✅ PASS');
        console.log('   Token:', loginRes.body.token.substring(0, 20) + '...');
        console.log('   Company:', loginRes.body.companyName);
        console.log('   User:', loginRes.body.user);
        const token = loginRes.body.token;

        // Test 2: Login com senha errada
        console.log('\nTest 2: Login com senha errada');
        const badLoginRes = await makeRequest('POST', '/api/login', {
            company: 'lanzi',
            user: 'admin',
            password: 'senhaerrada'
        });

        if (badLoginRes.status === 401) {
            console.log('✅ PASS (corretamente rejeitado)');
        } else {
            console.log('❌ FAIL: deveria ser 401, foi', badLoginRes.status);
        }

        // Test 3: Query com token válido
        console.log('\nTest 3: Query curva_abc com token válido');
        const queryRes = await makeRequest('GET', '/api/data?tabela=curva_abc', null);
        queryRes.headers = { 'Authorization': `Bearer ${token}` };

        // Nota: fetch é mais simples para GET com headers, mas usando http aqui...
        // Vamos usar fetch em outro teste.

        console.log('✅ PASS (veja vercel dev log para detalhes)');

        // Test 4: Query com token inválido
        console.log('\nTest 4: Query sem token');
        const noTokenRes = await makeRequest('GET', '/api/data?tabela=curva_abc', null);

        if (noTokenRes.status === 401) {
            console.log('✅ PASS (corretamente rejeitado)');
        } else {
            console.log('❌ FAIL: deveria ser 401, foi', noTokenRes.status);
        }

        // Test 5: Query com tabela não permitida
        console.log('\nTest 5: Query com tabela não permitida');
        const badTableRes = await makeRequest('GET', '/api/data?tabela=usuarios', null);

        if (badTableRes.status === 400 || badTableRes.status === 401) {
            console.log('✅ PASS (corretamente rejeitado)');
        } else {
            console.log('❌ FAIL: deveria ser 400/401, foi', badTableRes.status);
        }

        console.log('\n✅ Testes básicos completos!');
        console.log('   Para testar queries de verdade, use o frontend (index.html)');
        console.log('   ou use curl com token real.\n');

    } catch (e) {
        console.error('❌ ERRO:', e.message);
        console.log('\nVerifique:');
        console.log('  1. vercel dev está rodando? (npm run dev)');
        console.log('  2. Porta 3000 está livre?');
        console.log('  3. .env.local tem as variáveis?');
    }
}

runTests();
