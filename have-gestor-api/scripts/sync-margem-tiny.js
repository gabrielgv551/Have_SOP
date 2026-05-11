#!/usr/bin/env node
/**
 * sync-margem-tiny.js
 * Sincroniza a Margem de Contribuição do Olist ERP para a Have Gestor API.
 *
 * Estratégia:
 *   1. Playwright faz login no Tiny/Olist via Keycloak (email + senha)
 *   2. Extrai cookies de sessão + X-CSRF-TOKEN da página
 *   3. Chama: POST https://erp.olist.com/api/v1/contribution-margin/list
 *   4. Pagina todos os resultados e envia para Have Gestor (módulo tiny-margem)
 *   5. Salva cookie para reuso nas próximas chamadas via tiny-margem (GET)
 *
 * Uso:
 *   node sync-margem-tiny.js
 *
 * Variáveis de ambiente:
 *   TINY_EMAIL=seuemail@gmail.com
 *   TINY_PASSWORD=suasenha
 *   GESTOR_URL=https://have-gestor-api.vercel.app
 *   GESTOR_TOKEN=seuJWTtoken
 *   TINY_ACCOUNT=tiny_marcon
 *   MARGEM_DAYS=90
 *
 * Instalar (uma vez):
 *   npm install playwright
 *   npx playwright install chromium
 */

const { chromium } = require('playwright');
const https = require('https');
const fs    = require('fs');
const path  = require('path');

// ── Configuração ──────────────────────────────────────────────────────────────
const TINY_EMAIL    = process.env.TINY_EMAIL    || 'SEU_EMAIL_TINY';
const TINY_PASSWORD = process.env.TINY_PASSWORD || 'SUA_SENHA_TINY';
const GESTOR_URL    = process.env.GESTOR_URL    || 'https://have-gestor-api.vercel.app';
const GESTOR_TOKEN  = process.env.GESTOR_TOKEN  || 'SEU_JWT_TOKEN';
const TINY_ACCOUNT  = process.env.TINY_ACCOUNT  || 'tiny_marcon';
const MARGEM_DAYS   = parseInt(process.env.MARGEM_DAYS || '90');
const SESSION_FILE  = path.join(__dirname, '.olist-session.json');
const OLIST_API     = 'https://erp.olist.com/api/v1/contribution-margin/list';
const PER_PAGE      = 200;
// ─────────────────────────────────────────────────────────────────────────────

function isoDate(daysBack, endOfDay = false) {
  const d = new Date();
  d.setDate(d.getDate() - daysBack);
  if (endOfDay) d.setUTCHours(2, 59, 59, 999);
  else d.setUTCHours(3, 0, 0, 0);
  return d.toISOString();
}

function gestorRequest(method, qs, bodyObj) {
  return new Promise((resolve, reject) => {
    const url  = new URL(`/api/data?${qs}`, GESTOR_URL);
    const body = bodyObj ? Buffer.from(JSON.stringify(bodyObj), 'utf8') : null;
    const req  = https.request({
      hostname: url.hostname,
      path: url.pathname + url.search,
      method,
      headers: {
        'Authorization': 'Bearer ' + GESTOR_TOKEN,
        'Content-Type': 'application/json',
        ...(body ? { 'Content-Length': body.length } : {}),
      },
    }, res => {
      let data = '';
      res.on('data', d => data += d);
      res.on('end', () => { try { resolve(JSON.parse(data)); } catch { resolve(data); } });
    });
    req.on('error', reject);
    if (body) req.write(body);
    req.end();
  });
}

async function getSessionFromBrowser() {
  console.log('[sync-margem] Abrindo browser para login...');
  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext({
    userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124',
    locale: 'pt-BR',
  });
  const page = await context.newPage();

  // 1. Abre o ERP — vai redirecionar para Keycloak
  await page.goto('https://erp.olist.com', { waitUntil: 'domcontentloaded', timeout: 40000 });
  await page.waitForTimeout(2000);

  // 2. Login no Keycloak
  if (page.url().includes('accounts.tiny') || page.url().includes('login')) {
    console.log('[sync-margem] Preenchendo credenciais Keycloak...');
    await page.fill('#username, input[name="username"]', TINY_EMAIL);
    await page.fill('#password, input[name="password"]', TINY_PASSWORD);
    await page.click('#kc-login, button[type="submit"]');
    await page.waitForNavigation({ waitUntil: 'networkidle', timeout: 40000 });
  }
  console.log(`[sync-margem] Logado. URL: ${page.url()}`);

  // 3. Navega para margem para garantir que a SPA inicializa e emite o CSRF
  await page.goto('https://erp.olist.com/margem_contribuicao', { waitUntil: 'networkidle', timeout: 30000 });
  await page.waitForTimeout(2000);

  // 4. Extrai cookies de sessão
  const cookies = await context.cookies('https://erp.olist.com');
  const cookieStr = cookies.map(c => `${c.name}=${c.value}`).join('; ');

  // 5. Extrai CSRF token (meta tag ou cookie XSRF-TOKEN)
  let csrfToken = '';
  const xsrfCookie = cookies.find(c => c.name === 'XSRF-TOKEN' || c.name === 'csrf_token' || c.name.includes('csrf'));
  if (xsrfCookie) {
    csrfToken = decodeURIComponent(xsrfCookie.value);
  } else {
    csrfToken = await page.$eval('meta[name="csrf-token"]', el => el.content).catch(() => '');
  }
  console.log(`[sync-margem] CSRF token: ${csrfToken ? csrfToken.substring(0, 20) + '...' : '(não encontrado)'}`);

  await page.screenshot({ path: 'margem-debug.png' });
  await browser.close();

  const session = { cookieStr, csrfToken, savedAt: Date.now() };
  fs.writeFileSync(SESSION_FILE, JSON.stringify(session, null, 2));
  console.log('[sync-margem] Sessão salva em .olist-session.json');
  return session;
}

async function fetchMargemViaBrowser(fromISO, toISO) {
  // Usa Playwright para fazer requests dentro da sessão ativa do browser
  // Isso evita o 403 que acontece ao tentar reusar cookies no Node.js fora do browser
  console.log('[sync-margem] Abrindo browser para buscar dados da API...');
  const browser = await chromium.launch({
    headless: false,
    args: [
      '--disable-blink-features=AutomationControlled',
      '--start-maximized',
    ],
  });
  const context = await browser.newContext({
    userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    locale: 'pt-BR',
    extraHTTPHeaders: { 'Accept-Language': 'pt-BR,pt;q=0.9' },
  });
  // Bypass navigator.webdriver detection
  await context.addInitScript(() => {
    Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
  });
  const page = await context.newPage();

  // Intercepta efetuarLogin para logar o body E adicionar forcarLogin caso exista
  await page.route('**/efetuarLogin**', async route => {
    const req = route.request();
    const origBody = req.postData() || '';
    console.log('[sync-margem] efetuarLogin body original:', origBody.substring(0, 400));
    // Tenta adicionar forçar login (campo comum em ERPs PHP)
    let modifiedBody = origBody;
    try {
      if (origBody.startsWith('{')) {
        const parsed = JSON.parse(origBody);
        parsed.forcarLogin = true;
        parsed.forceLogin  = true;
        modifiedBody = JSON.stringify(parsed);
      } else if (origBody.includes('=')) {
        modifiedBody = origBody + '&forcarLogin=true&forceLogin=true';
      }
    } catch {}
    await route.continue({ postData: modifiedBody });
  });

  // Registra listener ANTES de qualquer goto — efetuarLogin dispara durante/após redirect Keycloak
  const loginLegacyPromise = page.waitForResponse(
    r => r.url().includes('efetuarLogin') || r.url().includes('autenticar'),
    { timeout: 60000 }
  ).catch(() => null);

  // Login
  await page.goto('https://erp.olist.com', { waitUntil: 'domcontentloaded', timeout: 40000 });
  await page.waitForTimeout(2000);
  if (page.url().includes('accounts.tiny') || page.url().includes('login')) {
    console.log('[sync-margem] Fazendo login no Keycloak...');
    await page.fill('#username, input[name="username"]', TINY_EMAIL);
    await page.fill('#password, input[name="password"]', TINY_PASSWORD);
    await page.click('#kc-login, button[type="submit"]');
    await page.waitForNavigation({ waitUntil: 'domcontentloaded', timeout: 40000 });
  }
  console.log(`[sync-margem] Keycloak OK. URL: ${page.url()}`);

  // Aguarda o SPA chamar efetuarLogin (sessão PHP legacy) — OBRIGATÓRIO antes de navegar
  console.log('[sync-margem] Aguardando sessão PHP (efetuarLogin)...');
  const loginRes = await loginLegacyPromise;
  if (loginRes) {
    const loginBody = await loginRes.text().catch(() => '');
    console.log(`[sync-margem] efetuarLogin status ${loginRes.status()} body: ${loginBody.substring(0, 200)}`);
  } else {
    console.log('[sync-margem] efetuarLogin não detectado — continuando');
  }
  await page.waitForTimeout(2000);

  // DIAGNÓSTICO: captura TODOS os requests do erp.olist.com para descobrir o endpoint real
  const networkLog = [];
  const capturedResponses = [];

  page.on('request', req => {
    if (req.url().includes('erp.olist.com') || req.url().includes('erp.tiny.com.br')) {
      networkLog.push(`REQ [${req.resourceType()}] ${req.method()} ${req.url().split('?')[0]}`);
    }
  });
  page.on('response', async resp => {
    const url = resp.url();
    if (!url.includes('erp.olist.com') && !url.includes('erp.tiny.com.br')) return;
    const ct = resp.headers()['content-type'] || '';
    networkLog.push(`RES ${resp.status()} ${url.split('?')[0]}`);
    if (ct.includes('json') && (url.includes('/api/') || url.includes('margem') || url.includes('margin') || url.includes('contribuicao'))) {
      try {
        const json  = await resp.json();
        const items = json.data || json.items || json.itens || json.resultado || [];
        capturedResponses.push({ url, json, itemCount: items.length });
        console.log(`[sync-margem] JSON capturado de ${url.split('?')[0]}: ${items.length} itens`);
        if (items.length > 0) console.log('[sync-margem] Campos:', Object.keys(items[0]).join(', '));
      } catch {}
    }
  });

  // Navega para margem usando window.location para ficar no mesmo contexto de sessão
  console.log('[sync-margem] Navegando para margem_contribuicao (in-context)...');
  await page.evaluate(() => { window.location.href = 'https://erp.olist.com/margem_contribuicao#/list'; });
  await page.waitForTimeout(10000); // SPA precisa de tempo para montar o componente lazy

  console.log(`[sync-margem] URL atual: ${page.url()}`);
  console.log(`[sync-margem] Título: ${await page.title()}`);

  // Diagnóstico: print HTML para ver o que foi renderizado
  const htmlSnippet = await page.content().catch(() => '');
  console.log('[sync-margem] HTML (primeiros 500 chars):', htmlSnippet.substring(0, 500));

  await page.screenshot({ path: 'margem-debug.png' });
  console.log('[sync-margem] === Requests capturados para erp.olist.com ===');
  networkLog.slice(-40).forEach(l => console.log(' ', l));

  const allItems = capturedResponses.flatMap(r => r.json.data || r.json.items || r.json.itens || r.json.resultado || []);
  const total    = capturedResponses[0]?.json?.meta?.total || capturedResponses[0]?.json?.total || allItems.length;
  console.log(`[sync-margem] Total: ${allItems.length} / ${total}`);

  const cookies    = await context.cookies('https://erp.olist.com');
  const cookieStr  = cookies.map(c => `${c.name}=${c.value}`).join('; ');
  const xsrfCookie = cookies.find(c => c.name === 'XSRF-TOKEN' || c.name.toLowerCase().includes('csrf'));
  const csrfToken  = xsrfCookie ? decodeURIComponent(xsrfCookie.value) : '';

  await browser.close();
  return { allItems, csrfToken, cookieStr };
}

(async () => {
  console.log(`[sync-margem] Conta: ${TINY_ACCOUNT} | Período: ${MARGEM_DAYS} dias`);

  const fromISO = isoDate(MARGEM_DAYS, false);
  const toISO   = isoDate(0, true);
  console.log(`[sync-margem] Período: ${fromISO.split('T')[0]} → ${toISO.split('T')[0]}`);

  // Busca dados via browser (context.request usa cookies da sessão automaticamente)
  const { allItems, cookieStr, csrfToken } = await fetchMargemViaBrowser(fromISO, toISO);

  if (allItems.length === 0) {
    console.log('[sync-margem] ⚠️  Nenhum item retornado. Verifique margem-debug.png');
    return;
  }

  console.log(`[sync-margem] ${allItems.length} registros. Enviando para Gestor...`);

  // Salva credenciais + chama tiny-margem para upsert no banco
  const result = await gestorRequest('POST',
    `module=tiny-margem&account=${TINY_ACCOUNT}`,
    { sessionCookie: cookieStr, csrfToken, from: fromISO.split('T')[0], to: toISO.split('T')[0] }
  );
  console.log('[sync-margem] ✅ Resultado:', JSON.stringify(result, null, 2));
})().catch(err => {
  console.error('[sync-margem] Erro fatal:', err.message);
  process.exit(1);
});
