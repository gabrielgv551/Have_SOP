#!/usr/bin/env node
/**
 * tiny_compras_scraper.js
 * Extrai Pedidos de Compra do Olist ERP via Playwright.
 * Copia cookies do Chrome local para autenticacao (estratgia do sync-margem).
 */

const { chromium } = require('playwright');
const fs = require('fs');
const path = require('path');
const os = require('os');

const TINY_EMAIL    = process.env.TINY_EMAIL    || '';
const TINY_PASSWORD = process.env.TINY_PASSWORD || '';
const COMPRAS_URL   = process.env.COMPRAS_URL   || '/pedidos_compra';
const HEADLESS      = process.env.HEADED !== '1';

async function main() {
  if (!TINY_EMAIL || !TINY_PASSWORD) {
    console.error('[compras] Defina TINY_EMAIL e TINY_PASSWORD');
    process.exit(1);
  }

  const { spawnSync } = require('child_process');

  // Fecha Chrome se estiver aberto
  const chromeRunning = spawnSync('tasklist', ['/FI', 'IMAGENAME eq chrome.exe', '/NH', '/FO', 'CSV'], { encoding: 'utf8' });
  const chromeWasRunning = chromeRunning.stdout.toLowerCase().includes('chrome.exe');
  if (chromeWasRunning) {
    console.log('[compras] Chrome detectado. Fechando...');
    spawnSync('taskkill', ['/F', '/IM', 'chrome.exe'], { shell: false });
    await new Promise(r => setTimeout(r, 5000));
    console.log('[compras] Chrome fechado.');
  }

  // Copia cookies do Chrome para contexto temporario
  const chromeDataDir = path.join(process.env.LOCALAPPDATA || '', 'Google', 'Chrome', 'User Data');
  const profileDir    = path.join(chromeDataDir, 'Default');
  const tempDir       = path.join(os.tmpdir(), `pw-compras-${Date.now()}`);
  const tempDefault   = path.join(tempDir, 'Default');
  fs.mkdirSync(path.join(tempDefault, 'Network'), { recursive: true });

  const cookieSrc = path.join(profileDir, 'Network', 'Cookies');
  const cookieDst = path.join(tempDefault, 'Network', 'Cookies');

  let usePersistent = false;
  if (fs.existsSync(cookieSrc)) {
    try {
      fs.copyFileSync(cookieSrc, cookieDst);
      for (const ext of ['-wal', '-shm']) {
        const src = cookieSrc + ext;
        if (fs.existsSync(src)) fs.copyFileSync(src, cookieDst + ext);
      }
      const lsSrc = path.join(chromeDataDir, 'Local State');
      if (fs.existsSync(lsSrc)) fs.copyFileSync(lsSrc, path.join(tempDir, 'Local State'));
      usePersistent = true;
      console.log('[compras] Cookies do Chrome copiados.');
    } catch (e) {
      console.log(`[compras] Falha ao copiar cookies: ${e.message}`);
    }
  }

  // Abre browser
  let context, browser, page;
  if (usePersistent) {
    context = await chromium.launchPersistentContext(tempDir, {
      headless: HEADLESS,
      args: ['--disable-blink-features=AutomationControlled', '--no-sandbox'],
      userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36',
      locale: 'pt-BR',
    });
    page = context.pages()[0] || await context.newPage();
  } else {
    browser = await chromium.launch({ headless: HEADLESS, args: ['--disable-blink-features=AutomationControlled', '--no-sandbox'] });
    context = await browser.newContext({ userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124', locale: 'pt-BR' });
    page = await context.newPage();
  }
  await context.addInitScript(() => { Object.defineProperty(navigator, 'webdriver', { get: () => undefined }); });

  // Intercepta efetuarLogin
  let efetuarLoginStatus = null;
  page.on('response', async resp => {
    if (resp.url().includes('efetuarLogin')) {
      efetuarLoginStatus = resp.status();
      const body = await resp.text().catch(() => '');
      console.log(`[compras] efetuarLogin [${resp.status()}]: ${body.substring(0, 200)}`);
    }
  });

  // Logout da sessao PHP existente (com cookies do Chrome, vai invalidar no servidor)
  console.log('[compras] Invalidando sessao anterior...');
  await page.goto('https://erp.olist.com/sair', { waitUntil: 'domcontentloaded', timeout: 15000 }).catch(() => null);
  console.log(`[compras] URL apos sair: ${page.url()}`);

  // Remove cookies de sessao do contexto
  const allCookies = await context.cookies('https://erp.olist.com');
  const sessionCookies = allCookies.filter(c =>
    c.name === 'TINYSESSID' || c.name === 'PHPSESSID' ||
    c.name.toLowerCase().includes('sessid') || c.name.toLowerCase().includes('session')
  );
  if (sessionCookies.length > 0) {
    await context.clearCookies();
    const nonSession = allCookies.filter(c =>
      c.name !== 'TINYSESSID' && c.name !== 'PHPSESSID' &&
      !c.name.toLowerCase().includes('sessid')
    );
    if (nonSession.length > 0) await context.addCookies(nonSession);
    console.log(`[compras] ${sessionCookies.length} cookie(s) de sessao removidos`);
  }
  await page.waitForTimeout(2000);

  // Login
  console.log('[compras] Navegando para ERP...');
  await page.goto('https://erp.olist.com', { waitUntil: 'domcontentloaded', timeout: 40000 });
  await page.waitForTimeout(2000);

  if (page.url().includes('accounts.tiny') || page.url().includes('login')) {
    console.log('[compras] Login Keycloak...');
    await page.fill('#username, input[name="username"]', TINY_EMAIL);
    await page.fill('#password, input[name="password"]', TINY_PASSWORD);
    await page.click('#kc-login, button[type="submit"]');
    await page.waitForURL('**/erp.olist.com/**', { timeout: 40000 }).catch(() => {});
  }
  console.log(`[compras] URL apos Keycloak: ${page.url()}`);

  // Aguarda efetuarLogin
  if (efetuarLoginStatus === null) {
    console.log('[compras] Aguardando efetuarLogin...');
    await page.waitForResponse(r => r.url().includes('efetuarLogin'), { timeout: 15000 }).catch(() => null);
  }
  await page.waitForTimeout(3000);

  // Diagnostico
  const cookies = await context.cookies('https://erp.olist.com');
  const sessionCookie = cookies.find(c => c.name.toLowerCase().includes('sessid') || c.name.toLowerCase().includes('session'));
  console.log(`[compras] Cookies: [${cookies.map(c => c.name).join(', ')}]`);
  console.log(`[compras] Sessao: ${sessionCookie ? sessionCookie.name + '=' + sessionCookie.value.substring(0, 10) + '...' : 'nao encontrado'}`);

  // Navega para Compras
  const comprasUrl = `https://erp.olist.com${COMPRAS_URL}`;
  console.log(`[compras] Navegando para ${comprasUrl}...`);

  const apiResponses = [];
  page.on('response', async resp => {
    const url = resp.url();
    if (url.includes('api') && (url.includes('pedido') || url.includes('compra') || url.includes('fornecedor'))) {
      try { const body = await resp.json().catch(() => null); if (body) apiResponses.push({ url: url.split('?')[0], body }); } catch {}
    }
  });

  await page.goto(comprasUrl, { waitUntil: 'networkidle', timeout: 30000 });
  await page.waitForTimeout(5000);
  console.log(`[compras] URL final: ${page.url()}`);

  if (page.url().includes('login') || page.url().includes('accounts.tiny')) {
    console.log('[compras] ERRO: Ainda em login apos efetuarLogin');
    await page.screenshot({ path: 'compras-erro.png', fullPage: true });
    await context.close();
    if (browser) await browser.close().catch(() => {});
    try { fs.rmSync(tempDir, { recursive: true, force: true }); } catch {}
    process.exit(1);
  }

  await page.screenshot({ path: 'compras-screenshot.png', fullPage: true });

  // Extrai dados
  const extracao = await page.evaluate(() => {
    const resultado = { url: window.location.href, titulo: document.title, tabelasEncontradas: 0, linhas: [], rotasDisponiveis: [] };
    const links = Array.from(document.querySelectorAll('a[href^="/"]'));
    resultado.rotasDisponiveis = [...new Set(links.map(a => a.getAttribute('href')))].slice(0, 30);

    const tabelas = document.querySelectorAll('table');
    resultado.tabelasEncontradas = tabelas.length;

    for (const tabela of tabelas) {
      const texto = tabela.textContent.toLowerCase();
      const temCompra = /compra|fornecedor|ordem|pedido|produto|quantidade|valor|numero|data|previsao/.test(texto);
      if (!temCompra && tabelas.length > 1) continue;

      const ths = tabela.querySelectorAll('thead th, tr:first-child th');
      const cabecalhos = Array.from(ths).map(th => th.textContent.trim());
      const corpo = tabela.querySelectorAll('tbody tr, tr');

      for (const tr of corpo) {
        if (tr.querySelector('th')) continue;
        const celulas = Array.from(tr.querySelectorAll('td'));
        if (celulas.length < 2) continue;
        const linha = {};
        celulas.forEach((td, i) => { linha[cabecalhos[i] || `col_${i}`] = td.textContent.trim(); });
        if (Object.keys(linha).length > 0) resultado.linhas.push(linha);
      }
      if (resultado.linhas.length > 0) break;
    }
    return resultado;
  });

  console.log('\n[compras] -- Resultado --');
  console.log(`URL: ${extracao.url}`);
  console.log(`Titulo: ${extracao.titulo}`);
  console.log(`Tabelas: ${extracao.tabelasEncontradas}`);
  console.log(`Linhas: ${extracao.linhas.length}`);

  if (extracao.linhas.length === 0) {
    console.log('\nNenhuma linha. Rotas disponiveis:');
    extracao.rotasDisponiveis.forEach(r => console.log(`  -> ${r}`));
  } else {
    console.log('\nPrimeiras linhas:');
    console.log(JSON.stringify(extracao.linhas.slice(0, 5), null, 2));
    fs.writeFileSync('compras_resultado.json', JSON.stringify(extracao, null, 2));
    console.log('\nSalvo em compras_resultado.json');
  }

  if (apiResponses.length > 0) {
    console.log(`\nAPIs interceptadas: ${apiResponses.length}`);
    apiResponses.forEach(api => console.log(`  -> ${api.url}`));
    fs.writeFileSync('compras_api_raw.json', JSON.stringify(apiResponses, null, 2));
  }

  await context.close();
  if (browser) await browser.close().catch(() => {});
  try { fs.rmSync(tempDir, { recursive: true, force: true }); } catch {}

  if (chromeWasRunning) {
    console.log('[compras] Reabrindo Chrome...');
    spawnSync('cmd', ['/c', 'start', 'chrome'], { shell: false, detached: true });
  }
}

main().catch(err => { console.error('[compras] Erro:', err.message); process.exit(1); });
