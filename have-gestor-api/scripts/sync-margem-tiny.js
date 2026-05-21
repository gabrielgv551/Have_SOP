#!/usr/bin/env node
/**
 * sync-margem-tiny.js
 * Sincroniza a Margem de Contribuição do Olist ERP para a Have Gestor API.
 *
 * Estratégia (sem email/senha):
 *   1. Copia cookies do Chrome local (erp.olist.com + accounts.tiny.com.br)
 *   2. Playwright abre contexto com esses cookies → SSO Keycloak silencioso
 *   3. Aguarda a página do Olist ERP carregar autenticada
 *   4. Chama: POST https://erp.olist.com/api/v1/contribution-margin/list
 *      de dentro do browser (credentials:include → envia PHPSESSID + Bearer)
 *   5. Pagina todos os resultados e envia para Have Gestor (módulo tiny-margem)
 *
 * Pré-requisito: Chrome deve ter o Olist ERP aberto/logado ao menos uma vez.
 * Os cookies Keycloak são reutilizados automaticamente (sem digitar senha).
 *
 * Uso:
 *   node sync-margem-tiny.js
 *
 * Variáveis de ambiente:
 *   GESTOR_URL=https://have-gestor-api.vercel.app
 *   GESTOR_TOKEN=seuJWTtoken
 *   TINY_ACCOUNT=tiny_marcon
 *   MARGEM_DAYS=90
 *   TINY_EMAIL=email@gmail.com    (opcional — fallback se cookies expirarem)
 *   TINY_PASSWORD=suasenha        (opcional — fallback se cookies expirarem)
 *
 * Instalar (uma vez):
 *   npm install playwright
 *   npx playwright install chromium
 */

const { chromium } = require('playwright');
const https = require('https');
const fs    = require('fs');
const path  = require('path');
const os    = require('os');

// ── Configuração ──────────────────────────────────────────────────────────────
const TINY_EMAIL    = process.env.TINY_EMAIL    || '';
const TINY_PASSWORD = process.env.TINY_PASSWORD || '';
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
  const { spawnSync } = require('child_process');
  const os = require('os');

  // Verifica se Chrome está rodando
  const chromeRunning = spawnSync('tasklist', ['/FI', 'IMAGENAME eq chrome.exe', '/NH', '/FO', 'CSV'], { encoding: 'utf8', shell: false });
  const chromeWasRunning = chromeRunning.stdout.toLowerCase().includes('chrome.exe');
  if (chromeWasRunning) {
    console.log('[sync-margem] Chrome detectado. Fechando temporariamente para sync...');
    spawnSync('taskkill', ['/F', '/IM', 'chrome.exe'], { shell: false });
    await new Promise(r => setTimeout(r, 2000));
    console.log('[sync-margem] Chrome fechado.');
  }

  // Tenta copiar cookies do Chrome (agora desbloqueado)
  const chromeDataDir = path.join(process.env.LOCALAPPDATA || '', 'Google', 'Chrome', 'User Data');
  const profileDir    = path.join(chromeDataDir, 'Default');
  const tempDir       = path.join(os.tmpdir(), `pw-olist-${Date.now()}`);
  const tempDefault   = path.join(tempDir, 'Default');
  fs.mkdirSync(path.join(tempDefault, 'Network'), { recursive: true });

  let usePersistentContext = false;
  const cookieSrc = path.join(profileDir, 'Network', 'Cookies');
  const cookieDst = path.join(tempDefault, 'Network', 'Cookies');

  if (fs.existsSync(cookieSrc)) {
    try {
      fs.copyFileSync(cookieSrc, cookieDst);
      for (const ext of ['-wal', '-shm']) {
        if (fs.existsSync(cookieSrc + ext)) fs.copyFileSync(cookieSrc + ext, cookieDst + ext);
      }
      const lsSrc = path.join(chromeDataDir, 'Local State');
      if (fs.existsSync(lsSrc)) fs.copyFileSync(lsSrc, path.join(tempDir, 'Local State'));
      usePersistentContext = true;
      console.log('[sync-margem] Cookies do Chrome copiados para contexto temporário.');
    } catch (e) {
      console.log(`[sync-margem] Falha ao copiar cookies: ${e.code}. Prosseguindo com login fresh.`);
    }
  }

  let context, browser;
  if (usePersistentContext) {
    context = await chromium.launchPersistentContext(tempDir, {
      headless: true,
      args: ['--disable-blink-features=AutomationControlled', '--no-sandbox'],
      userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
      locale: 'pt-BR',
    });
  } else {
    browser = await chromium.launch({ headless: true, args: ['--disable-blink-features=AutomationControlled', '--no-sandbox'] });
    context = await browser.newContext({
      userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
      locale: 'pt-BR',
    });
  }
  await context.addInitScript(() => { Object.defineProperty(navigator, 'webdriver', { get: () => undefined }); });
  const page = await context.newPage();

  // Intercepta efetuarLogin ANTES da navegação
  let efetuarLoginStatus = null;
  page.on('response', async resp => {
    if (resp.url().includes('efetuarLogin')) {
      efetuarLoginStatus = resp.status();
      const body = await resp.text().catch(() => '');
      console.log(`[sync-margem] efetuarLogin [${resp.status()}]: ${body.substring(0, 200)}`);
    }
  });

  // Se temos cookies existentes do Chrome, usa-os para fazer logout (invalidar sessão PHP no servidor)
  // Sem isso, o efetuarLogin falha com "usuário já está logado em outro navegador"
  if (usePersistentContext) {
    console.log('[sync-margem] Fazendo logout da sessão PHP existente...');
    // Navega para /sair que invalida TINYSESSID no servidor
    const logoutRes = await page.goto('https://erp.olist.com/sair', { waitUntil: 'domcontentloaded', timeout: 15000 }).catch(() => null);
    console.log(`[sync-margem] Logout via /sair: ${page.url()}`);
    // Remove cookies de sessão do contexto para forçar login fresco
    const allCookies = await context.cookies('https://erp.olist.com');
    const sessionCookies = allCookies.filter(c =>
      c.name === 'TINYSESSID' || c.name === 'PHPSESSID' ||
      c.name.toLowerCase().includes('sessid') || c.name.toLowerCase().includes('session')
    );
    if (sessionCookies.length > 0) {
      await context.clearCookies();
      // Restaura apenas cookies não-sessão (analytics, etc.)
      const nonSessionCookies = allCookies.filter(c =>
        c.name !== 'TINYSESSID' && c.name !== 'PHPSESSID' &&
        !c.name.toLowerCase().includes('sessid')
      );
      if (nonSessionCookies.length > 0) await context.addCookies(nonSessionCookies);
      console.log(`[sync-margem] ${sessionCookies.length} cookie(s) de sessão removidos do contexto.`);
    }
    await page.waitForTimeout(1000);
  }

  // Login Keycloak (sessão PHP agora foi invalidada — efetuarLogin deve funcionar)
  await page.goto('https://erp.olist.com', { waitUntil: 'domcontentloaded', timeout: 40000 });
  await page.waitForTimeout(2000);
  if (page.url().includes('accounts.tiny') || page.url().includes('login')) {
    console.log('[sync-margem] Login Keycloak...');
    await page.fill('#username, input[name="username"]', TINY_EMAIL);
    await page.fill('#password, input[name="password"]', TINY_PASSWORD);
    await page.click('#kc-login, button[type="submit"]');
    await page.waitForURL('**/erp.olist.com/**', { timeout: 40000 }).catch(() => {});
  }
  console.log(`[sync-margem] URL: ${page.url()}`);

  // Aguarda efetuarLogin terminar (pode ter sido chamado já durante redirect)
  if (efetuarLoginStatus === null) {
    await page.waitForResponse(r => r.url().includes('efetuarLogin'), { timeout: 15000 }).catch(() => null);
  }
  await page.waitForTimeout(2000);

  // Diagnóstico de cookies disponíveis no contexto
  const ctxCookies = await context.cookies('https://erp.olist.com');
  const phpSessId = ctxCookies.find(c => c.name.toLowerCase().includes('phpsessid') || c.name.toLowerCase().includes('session'));
  const xsrfCookie = ctxCookies.find(c => c.name === 'XSRF-TOKEN' || c.name.toLowerCase().includes('csrf') || c.name.toLowerCase().includes('xsrf'));
  console.log(`[sync-margem] Cookies erp.olist.com: [${ctxCookies.map(c => c.name).join(', ')}]`);
  console.log(`[sync-margem] PHPSESSID: ${phpSessId ? phpSessId.name + '=' + phpSessId.value.substring(0, 10) + '...' : 'não encontrado'}`);
  const csrfVal = xsrfCookie ? decodeURIComponent(xsrfCookie.value) : '';
  if (csrfVal) console.log(`[sync-margem] CSRF: ${csrfVal.substring(0, 20)}...`);

  console.log('[sync-margem] Chamando API de margem via browser (IP local)...');
  const apiResult = await page.evaluate(async ({ apiUrl, token, csrfToken, fromISO, toISO, perPage }) => {
    const allItems = [];
    let page = 1;
    let totalApi = 0;
    while (true) {
      const body = {
        page, perPage,
        sort: 'date', order: 'desc',
        filters: {
          period: { start: fromISO, end: toISO },
          search: '', channels: [], products: [], categories: [], tags: [],
        },
      };
      const headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Origin': 'https://erp.olist.com',
        'Referer': 'https://erp.olist.com/margem_contribuicao',
      };
      if (token) headers['Authorization'] = 'Bearer ' + token;
      if (csrfToken) headers['X-CSRF-TOKEN'] = csrfToken;

      let resp;
      try { resp = await fetch(apiUrl, { method: 'POST', headers, body: JSON.stringify(body), credentials: 'include' }); }
      catch (e) { return { error: e.message }; }

      if (!resp.ok) {
        const errText = await resp.text().catch(() => '');
        return { error: `HTTP ${resp.status}`, detail: errText.substring(0, 300), page };
      }
      const json = await resp.json().catch(e => ({ error: e.message }));
      if (json.error) return json;

      const items = json.data || json.items || json.itens || [];
      totalApi = json.meta?.total || json.total || totalApi || items.length;
      allItems.push(...items);
      if (items.length === 0 || items.length < perPage) break;
      page++;
      if (page > 50) break; // segurança: max 10k registros
    }
    return { allItems, total: totalApi };
  }, { apiUrl: OLIST_API, token: null, csrfToken: csrfVal, fromISO, toISO, perPage: PER_PAGE });

  await context.close();
  if (browser) await browser.close().catch(() => {});
  try { fs.rmSync(tempDir, { recursive: true, force: true }); } catch {}

  // Reabre o Chrome se estava aberto antes do sync
  if (chromeWasRunning) {
    console.log('[sync-margem] Reabrindo Chrome...');
    require('child_process').spawnSync('cmd', ['/c', 'start', 'chrome'], { shell: false, detached: true });
  }

  if (apiResult.error) {
    throw new Error(`API Olist: ${apiResult.error} — ${apiResult.detail || ''} (página ${apiResult.page || 1})`);
  }

  const { allItems, total } = apiResult;
  console.log(`[sync-margem] Total: ${allItems.length} / ${total}`);
  if (allItems.length > 0) console.log('[sync-margem] Campos:', Object.keys(allItems[0]).join(', '));

  return { allItems, csrfToken: '', cookieStr: '' };
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

  // Envia os itens já buscados para upsert no banco
  const result = await gestorRequest('POST',
    `module=tiny-margem&account=${TINY_ACCOUNT}`,
    {
      items: allItems,
      from: fromISO.split('T')[0],
      to:   toISO.split('T')[0],
      ...(cookieStr ? { sessionCookie: cookieStr, csrfToken } : {}),
    }
  );
  console.log('[sync-margem] ✅ Resultado:', JSON.stringify(result, null, 2));
})().catch(err => {
  console.error('[sync-margem] Erro fatal:', err.message);
  process.exit(1);
});
