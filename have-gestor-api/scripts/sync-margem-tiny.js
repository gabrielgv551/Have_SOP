#!/usr/bin/env node
/**
 * sync-margem-tiny.js
 * Exporta automaticamente a Margem de Contribuição do Tiny ERP
 * e envia para a Have Gestor API.
 *
 * Uso:
 *   node sync-margem-tiny.js
 *
 * Configuração (variáveis de ambiente ou editar as constantes abaixo):
 *   TINY_EMAIL=seuemail@gmail.com
 *   TINY_PASSWORD=suasenha
 *   GESTOR_URL=https://have-gestor-api.vercel.app
 *   GESTOR_TOKEN=seuJWTtoken
 *   TINY_ACCOUNT=tiny_marcon
 *   MARGEM_DAYS=90   (quantos dias de margem exportar, padrão: 90)
 *
 * Instalar dependências (uma vez):
 *   npm install playwright
 *   npx playwright install chromium
 */

const { chromium } = require('playwright');
const https = require('https');

// ── Configuração ──────────────────────────────────────────────────────────────
const TINY_EMAIL    = process.env.TINY_EMAIL    || 'SEU_EMAIL_TINY';
const TINY_PASSWORD = process.env.TINY_PASSWORD || 'SUA_SENHA_TINY';
const GESTOR_URL    = process.env.GESTOR_URL    || 'https://have-gestor-api.vercel.app';
const GESTOR_TOKEN  = process.env.GESTOR_TOKEN  || 'SEU_JWT_TOKEN';
const TINY_ACCOUNT  = process.env.TINY_ACCOUNT  || 'tiny_marcon';
const MARGEM_DAYS   = parseInt(process.env.MARGEM_DAYS || '90');
// ─────────────────────────────────────────────────────────────────────────────

function dateStr(daysBack) {
  const d = new Date();
  d.setDate(d.getDate() - daysBack);
  return d.toISOString().slice(0, 10).replace(/-/g, '/');
}

async function postToGestor(csvText) {
  return new Promise((resolve, reject) => {
    const url = new URL(`/api/data?module=import-margem&account=${TINY_ACCOUNT}`, GESTOR_URL);
    const body = Buffer.from(csvText, 'utf8');
    const options = {
      hostname: url.hostname,
      path: url.pathname + url.search,
      method: 'POST',
      headers: {
        'Authorization': 'Bearer ' + GESTOR_TOKEN,
        'Content-Type': 'text/plain; charset=utf-8',
        'Content-Length': body.length,
      },
    };
    const req = https.request(options, res => {
      let data = '';
      res.on('data', d => data += d);
      res.on('end', () => {
        try { resolve(JSON.parse(data)); } catch { resolve(data); }
      });
    });
    req.on('error', reject);
    req.write(body);
    req.end();
  });
}

(async () => {
  console.log(`[sync-margem] Iniciando para conta: ${TINY_ACCOUNT}`);
  console.log(`[sync-margem] Período: últimos ${MARGEM_DAYS} dias`);

  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext({
    userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124',
    locale: 'pt-BR',
  });
  const page = await context.newPage();

  // Captura interceptação de requests para achar a API interna
  let apiUrl = null;
  const capturedData = [];

  page.on('response', async response => {
    const url = response.url();
    if (!url.includes('erp.olist.com')) return;
    // Ignora assets estáticos
    if (/\.(css|js|png|jpg|svg|ico|woff|woff2|ttf)/.test(url)) return;
    if (url.includes('gtm') || url.includes('analytics') || url.includes('clarity')) return;
    const ct = response.headers()['content-type'] || '';
    if (ct.includes('json') || ct.includes('csv')) {
      try {
        const body = await response.text();
        if (body.length > 50 && (body.includes('faturamento') || body.includes('margem') || body.includes('comissao') || body.includes('contribuicao') || body.includes('pedido'))) {
          console.log(`[sync-margem] ✅ URL de dados encontrada: ${url}`);
          apiUrl = url;
          capturedData.push({ url, contentType: ct, body });
        }
      } catch {}
    }
  });

  // 1. Ir para o Tiny ERP
  console.log('[sync-margem] Abrindo erp.olist.com...');
  await page.goto('https://erp.olist.com', { waitUntil: 'networkidle', timeout: 30000 });

  // 2. Login — vai redirecionar para o Keycloak
  await page.waitForTimeout(2000);
  const currentUrl = page.url();
  console.log(`[sync-margem] URL após load: ${currentUrl}`);

  if (currentUrl.includes('accounts.tiny.com.br') || currentUrl.includes('keycloak') || currentUrl.includes('login')) {
    console.log('[sync-margem] Fazendo login no Keycloak...');
    await page.fill('input[name="username"], input[type="email"], #username', TINY_EMAIL);
    await page.fill('input[name="password"], input[type="password"], #password', TINY_PASSWORD);
    await page.click('button[type="submit"], input[type="submit"], #kc-login, .btn-primary');
    await page.waitForNavigation({ waitUntil: 'networkidle', timeout: 30000 });
    console.log(`[sync-margem] URL após login: ${page.url()}`);
  }

  // 3. Navegar para margem de contribuição
  console.log('[sync-margem] Navegando para margem de contribuição...');
  await page.goto('https://erp.olist.com/margem_contribuicao', { waitUntil: 'networkidle', timeout: 30000 });
  await page.waitForTimeout(3000);

  // 4. Aplicar filtro de data
  const dataInicio = dateStr(MARGEM_DAYS);
  const dataFim    = dateStr(0);
  console.log(`[sync-margem] Aplicando filtro: ${dataInicio} a ${dataFim}`);
  console.log(`[sync-margem] URL atual: ${page.url()}`);

  // Aguarda a SPA carregar
  await page.waitForTimeout(4000);

  // Tenta preencher campos de data de várias formas
  const dateInputSelectors = [
    'input[type="date"]',
    'input[placeholder*="nicio"]',
    'input[placeholder*="Data"]',
    'input[name*="inicio"]',
    'input[name*="dataInicial"]',
  ];
  for (const sel of dateInputSelectors) {
    try {
      const el = await page.$(sel);
      if (el) { await el.fill(dataInicio); break; }
    } catch {}
  }
  const dateEndSelectors = [
    'input[type="date"]:nth-of-type(2)',
    'input[placeholder*="inal"]',
    'input[name*="fim"]',
    'input[name*="dataFinal"]',
  ];
  for (const sel of dateEndSelectors) {
    try {
      const el = await page.$(sel);
      if (el) { await el.fill(dataFim); break; }
    } catch {}
  }
  // Clica em buscar
  try {
    await page.click('button:has-text("Buscar"), button:has-text("Filtrar"), button:has-text("Aplicar")');
    await page.waitForTimeout(4000);
  } catch {}

  // 5. Tentar exportar CSV
  console.log('[sync-margem] Tentando exportar CSV...');
  let csvData = null;
  try {
    const [ download ] = await Promise.all([
      page.waitForEvent('download', { timeout: 15000 }),
      page.click('button:has-text("Exportar"), a:has-text("Exportar"), button:has-text("CSV"), a:has-text("CSV")'),
    ]);
    const path = await download.path();
    const fs = require('fs');
    csvData = fs.readFileSync(path, 'latin1');
    console.log(`[sync-margem] ✅ CSV baixado: ${csvData.length} bytes`);
  } catch(e) {
    console.log('[sync-margem] Exportação via botão falhou:', e.message);
  }

  // 6. Se não conseguiu CSV via botão, usa dados JSON capturados nas requests
  if (!csvData && capturedData.length > 0) {
    console.log(`[sync-margem] ${capturedData.length} resposta(s) capturada(s) das requests`);
    // Prefere CSV se houver
    const csvCapture = capturedData.find(d => d.contentType?.includes('csv'));
    if (csvCapture) {
      csvData = csvCapture.body;
      console.log('[sync-margem] Usando dados CSV capturados');
    } else {
      // Converte JSON para CSV se necessário
      const jsonCapture = capturedData[0];
      try {
        const parsed = JSON.parse(jsonCapture.body);
        const rows = Array.isArray(parsed) ? parsed : (parsed.itens || parsed.data || parsed.registros || [parsed]);
        if (rows.length > 0) {
          const headers = Object.keys(rows[0]);
          csvData = headers.join(';') + '\n' + rows.map(r => headers.map(h => r[h] ?? '').join(';')).join('\n');
          console.log(`[sync-margem] Convertido JSON→CSV: ${rows.length} linhas`);
        }
      } catch {
        csvData = jsonCapture.body;
      }
    }
  }

  // 7. Captura screenshot para debug
  await page.screenshot({ path: 'margem-debug.png', fullPage: false });
  console.log('[sync-margem] Screenshot salva: margem-debug.png');

  // 8. Log da URL da API descoberta — guardamos para chamadas futuras sem browser
  if (apiUrl) {
    console.log(`\n[sync-margem] 🔑 URL da API interna:\n  ${apiUrl}\n`);
    const fs = require('fs');
    fs.writeFileSync('margem-api-url.txt', apiUrl + '\n' + JSON.stringify(capturedData.map(d=>d.url), null, 2));
  } else {
    console.log('[sync-margem] Nenhuma URL de dados capturada. Verifique margem-debug.png');
  }

  await browser.close();

  // 9. Envia para Gestor
  if (csvData && csvData.length > 100) {
    console.log('[sync-margem] Enviando para Gestor API...');
    const result = await postToGestor(csvData);
    console.log('[sync-margem] ✅ Resultado:', JSON.stringify(result, null, 2));
  } else {
    console.log('[sync-margem] ⚠️  Nenhum dado capturado. Verifique margem-debug.png');
    if (capturedData.length > 0) {
      console.log('[sync-margem] Requests capturadas:', capturedData.map(d => d.url));
    }
  }

  console.log('[sync-margem] Concluído.');
})().catch(err => {
  console.error('[sync-margem] Erro:', err);
  process.exit(1);
});
