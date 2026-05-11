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

async function fetchMargem(session, fromISO, toISO) {
  const headers = {
    'Content-Type': 'application/json',
    'Accept': 'application/json, text/plain, */*',
    'Cookie': session.cookieStr,
    'Origin': 'https://erp.olist.com',
    'Referer': 'https://erp.olist.com/margem_contribuicao',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124',
    'X-CSRF-TOKEN': session.csrfToken,
    'X-Requested-With': 'XMLHttpRequest',
  };

  let page = 1, allItems = [], totalPages = 1;
  do {
    const body = JSON.stringify({
      page, perPage: PER_PAGE, sort: 'date', order: 'desc',
      filters: {
        period: { start: fromISO, end: toISO },
        search: '', channels: [], products: [], categories: [], tags: [],
      },
    });
    const r = await fetch(OLIST_API, { method: 'POST', headers, body });
    if (!r.ok) {
      const txt = await r.text();
      throw new Error(`Olist API ${r.status}: ${txt.substring(0, 200)}`);
    }
    const json = await r.json();
    const items = json.data || json.items || json.itens || [];
    allItems.push(...items);
    if (page === 1) {
      const total = json.meta?.total || json.pagination?.total || json.total || items.length;
      totalPages  = Math.ceil(total / PER_PAGE);
      console.log(`[sync-margem] Total de registros: ${total} (${totalPages} páginas)`);
      if (items.length > 0) console.log('[sync-margem] Campos da API:', Object.keys(items[0]).join(', '));
    }
    console.log(`[sync-margem] Página ${page}/${totalPages}: ${items.length} itens`);
    page++;
    if (items.length < PER_PAGE) break;
    await new Promise(r => setTimeout(r, 300));
  } while (page <= totalPages);

  return allItems;
}

(async () => {
  console.log(`[sync-margem] Conta: ${TINY_ACCOUNT} | Período: ${MARGEM_DAYS} dias`);

  // Tenta reusar sessão salva (válida por 8h)
  let session = null;
  if (fs.existsSync(SESSION_FILE)) {
    try {
      const saved = JSON.parse(fs.readFileSync(SESSION_FILE, 'utf8'));
      if (Date.now() - saved.savedAt < 8 * 3600 * 1000) {
        session = saved;
        console.log('[sync-margem] Reutilizando sessão salva.');
      }
    } catch {}
  }
  if (!session) session = await getSessionFromBrowser();

  // Busca dados da API interna
  const fromISO = isoDate(MARGEM_DAYS, false);
  const toISO   = isoDate(0, true);
  console.log(`[sync-margem] Buscando ${fromISO.split('T')[0]} → ${toISO.split('T')[0]}`);

  let items;
  try {
    items = await fetchMargem(session, fromISO, toISO);
  } catch (e) {
    console.log(`[sync-margem] Erro com sessão salva (${e.message}). Renovando login...`);
    fs.unlinkSync(SESSION_FILE);
    session = await getSessionFromBrowser();
    items = await fetchMargem(session, fromISO, toISO);
  }

  if (items.length === 0) {
    console.log('[sync-margem] ⚠️  Nenhum item retornado. Verifique margem-debug.png');
    return;
  }

  console.log(`[sync-margem] ${items.length} registros obtidos. Enviando para Gestor...`);

  // Envia para o módulo tiny-margem do Gestor (que faz o upsert no banco)
  const result = await gestorRequest('POST',
    `module=tiny-margem&account=${TINY_ACCOUNT}`,
    {
      sessionCookie: session.cookieStr,
      csrfToken: session.csrfToken,
      from: fromISO.split('T')[0],
      to: toISO.split('T')[0],
    }
  );
  console.log('[sync-margem] ✅ Resultado:', JSON.stringify(result, null, 2));
})().catch(err => {
  console.error('[sync-margem] Erro fatal:', err.message);
  process.exit(1);
});
