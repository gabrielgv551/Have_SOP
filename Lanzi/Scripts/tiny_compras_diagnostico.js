#!/usr/bin/env node
/**
 * Diagnóstico — descobre rotas de Compras no Olist ERP
 */

const { chromium } = require('playwright');
const fs = require('fs');

const TINY_EMAIL    = process.env.TINY_EMAIL    || '';
const TINY_PASSWORD = process.env.TINY_PASSWORD || '';
const HEADLESS      = process.env.HEADED !== '1';

async function main() {
  if (!TINY_EMAIL || !TINY_PASSWORD) {
    console.error('Defina TINY_EMAIL e TINY_PASSWORD');
    process.exit(1);
  }

  const browser = await chromium.launch({ headless: HEADLESS });
  const context = await browser.newContext({
    userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124',
    locale: 'pt-BR',
  });
  await context.addInitScript(() => {
    Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
  });
  const page = await context.newPage();

  // Login
  console.log('[diag] Login...');
  await page.goto('https://erp.olist.com', { waitUntil: 'domcontentloaded', timeout: 40000 });
  await page.waitForTimeout(2000);

  if (page.url().includes('accounts.tiny') || page.url().includes('login')) {
    await page.fill('#username, input[name="username"]', TINY_EMAIL);
    await page.fill('#password, input[name="password"]', TINY_PASSWORD);
    await page.click('#kc-login, button[type="submit"]');
    await page.waitForURL('**/erp.olist.com/**', { timeout: 40000 }).catch(() => {});
  }
  console.log(`[diag] Logado: ${page.url()}`);
  await page.waitForTimeout(3000);

  // Screenshot da home
  await page.screenshot({ path: 'diag-home.png', fullPage: true });
  console.log('[diag] Screenshot home salvo: diag-home.png');

  // Extrai links de navegação (menu lateral)
  const menuLinks = await page.evaluate(() => {
    const links = [];
    // Seletores comuns de menu ERP
    const selectors = [
      'a[href^="/"]', 'a.menu-item', '[class*="menu"] a',
      '[class*="nav"] a', 'nav a', '.sidebar a',
      '[data-testid] a', 'a[routerLink]'
    ];
    for (const sel of selectors) {
      document.querySelectorAll(sel).forEach(a => {
        const href = a.getAttribute('href');
        const text = (a.textContent || a.getAttribute('title') || '').trim();
        if (href && href.startsWith('/') && text) {
          links.push({ href, text: text.substring(0, 60) });
        }
      });
    }
    // Remove duplicatas
    const seen = new Set();
    return links.filter(l => {
      const key = l.href + '|' + l.text;
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });
  });

  console.log('\n[diag] ── Links do menu ──');
  menuLinks.forEach(l => console.log(`  ${l.href.padEnd(30)} → ${l.text}`));

  // Filtra links relacionados a compras
  const comprasLinks = menuLinks.filter(l =>
    /compra|fornecedor|ordem|pedido.*compra|necessidade|estoque|produto/i.test(l.text + ' ' + l.href)
  );
  console.log('\n[diag] ── Links de Compras/Fornecedor ──');
  comprasLinks.forEach(l => console.log(`  ${l.href.padEnd(30)} → ${l.text}`));

  // Testa rotas comuns
  const rotasComuns = [
    '/compras', '/ordens_compra', '/ordem_compra', '/pedidos_compra',
    '/pedido_compra', '/necessidade_compra', '/necessidade', '/fornecedores',
    '/produtos', '/estoque', '/relatorios', '/dashboard'
  ];

  console.log('\n[diag] ── Testando rotas ──');
  for (const rota of rotasComuns) {
    const testPage = await context.newPage();
    try {
      const resp = await testPage.goto(`https://erp.olist.com${rota}`, { waitUntil: 'domcontentloaded', timeout: 15000 });
      const url = testPage.url();
      const status = resp ? resp.status() : '?';
      const titulo = await testPage.title().catch(() => '');
      const is404 = /não encontrada|404|not found/i.test(titulo);
      console.log(`  ${rota.padEnd(25)} → HTTP ${status} | ${is404 ? '404' : 'OK'} | "${titulo.substring(0, 40)}"`);
      if (!is404 && status !== 404) {
        await testPage.screenshot({ path: `diag-${rota.replace(/\//g, '_')}.png`, fullPage: true });
      }
    } catch(e) {
      console.log(`  ${rota.padEnd(25)} → ERRO: ${e.message.substring(0, 50)}`);
    }
    await testPage.close();
  }

  await browser.close();
  console.log('\n[diag] ✅ Diagnóstico completo.');
}

main().catch(err => {
  console.error('[diag] Erro:', err.message);
  process.exit(1);
});
