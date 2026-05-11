const { Pool } = require('pg');
const companies = require('../lib/companies');

function getPool(c) {
  const cfg = companies[c];
  if (!cfg) throw new Error('Empresa não encontrada: ' + c);
  return new Pool({ connectionString: cfg.db_url, ssl: { rejectUnauthorized: false } });
}

const TINY_TOKEN_URL = 'https://accounts.tiny.com.br/realms/tiny/protocol/openid-connect/token';
const TINY_API       = 'https://erp.tiny.com.br/public-api/v3';

async function getToken(pool, company, account, cfg) {
  let token = cfg[account + '_token'];
  if (!token) return null;
  const exp = cfg[account + '_exp'] ? new Date(cfg[account + '_exp']) : null;
  if (exp && new Date() >= exp) {
    const tr = await fetch(TINY_TOKEN_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams({
        grant_type: 'refresh_token',
        client_id: cfg['tiny_client_id'] || '',
        client_secret: cfg['tiny_client_secret'] || '',
        refresh_token: cfg[account + '_refresh'] || '',
      }),
    });
    if (tr.ok) {
      const nt = await tr.json();
      token = nt.access_token;
      const newExp = new Date(Date.now() + (nt.expires_in || 300) * 1000).toISOString();
      for (const [k, v] of [[account + '_token', token], [account + '_refresh', nt.refresh_token || cfg[account + '_refresh']], [account + '_exp', newExp]])
        await pool.query(`INSERT INTO configuracoes (empresa,chave,valor,atualizado_em) VALUES ($1,$2,$3,NOW()) ON CONFLICT (empresa,chave) DO UPDATE SET valor=EXCLUDED.valor,atualizado_em=NOW()`, [company, k, v]);
    }
  }
  return token;
}

async function tinyPages(endpoint, params, token) {
  const items = [];
  let offset = 0;
  while (true) {
    const qs = new URLSearchParams({ ...params, limit: '100', offset: String(offset) }).toString();
    const r  = await fetch(`${TINY_API}${endpoint}?${qs}`, { headers: { Authorization: 'Bearer ' + token } });
    if (!r.ok) break;
    const d  = await r.json();
    const pg = d.itens || d.data || [];
    items.push(...pg);
    const total = d.paginacao?.total ?? d.total ?? 0;
    offset += pg.length;
    if (pg.length === 0 || offset >= total) break;
  }
  return items;
}

module.exports = async (req, res) => {
  const log = [];
  const today      = new Date().toISOString().split('T')[0];
  const twoDaysAgo = new Date(Date.now() - 2 * 86400000).toISOString().split('T')[0];

  for (const company of Object.keys(companies)) {
    try {
      const pool   = getPool(company);
      const cfgRes = await pool.query(`SELECT chave, valor FROM configuracoes WHERE empresa=$1`, [company]);
      const cfg    = {};
      for (const r of cfgRes.rows) cfg[r.chave] = r.valor;

      const accounts = Object.keys(cfg)
        .filter(k => k.endsWith('_token') && !k.includes('_refresh') && !k.includes('_exp'))
        .map(k => k.replace(/_token$/, ''))
        .filter(k => k.startsWith('tiny'));

      for (const account of accounts) {
        const token = await getToken(pool, company, account, cfg);
        if (!token) continue;
        const safeName = account.replace(/[^a-z0-9_]/gi, '_').toLowerCase();

        // ── 1. SYNC: novos pedidos (últimos 2 dias) ──
        const pedidos = await tinyPages('/pedidos', { dataInicial: twoDaysAgo, dataFinal: today }, token);
        if (pedidos.length > 0) {
          const rows = pedidos.map(p => [
            String(p.id || ''),
            p.numeroPedido ? String(p.numeroPedido) : null,
            p.ecommerce?.numeroPedidoEcommerce || null,
            p.ecommerce?.numeroPedidoCanalVenda || null,
            p.dataCriacao ? p.dataCriacao.split('T')[0] : null,
            p.dataPrevisao ? p.dataPrevisao.split('T')[0] : null,
            typeof p.situacao === 'number' ? p.situacao : null,
            p.cliente?.nome || null, p.cliente?.cpfCnpj || null, p.cliente?.tipoPessoa || null,
            p.cliente?.email || null, p.cliente?.telefone || null, p.cliente?.celular || null,
            p.cliente?.codigo ? String(p.cliente.codigo) : null,
            p.cliente?.endereco?.uf || null, p.cliente?.endereco?.municipio || null,
            p.cliente?.endereco?.bairro || null, p.cliente?.endereco?.cep || null,
            p.cliente?.endereco?.endereco || null,
            parseFloat(p.valor || 0) || 0,
            p.ecommerce?.nome || null, p.ecommerce?.canalVenda || null,
            p.transportador?.nome || null, p.transportador?.formaEnvio?.nome || null,
            p.transportador?.fretePorConta || null, p.transportador?.codigoRastreamento || null,
            p.transportador?.urlRastreamento || null,
            p.vendedor?.nome || null,
            typeof p.origemPedido === 'number' ? p.origemPedido : null,
          ]);
          await pool.query(`CREATE TABLE IF NOT EXISTS bd_pedidos_tiny_${safeName} (
            id_tiny TEXT PRIMARY KEY, numero TEXT, numero_ecommerce TEXT, numero_canal_venda TEXT,
            data_criacao DATE, data_previsao DATE, situacao INT,
            nome_cliente TEXT, cpf_cnpj TEXT, tipo_pessoa TEXT,
            email_cliente TEXT, telefone_cliente TEXT, celular_cliente TEXT, codigo_cliente TEXT,
            uf TEXT, municipio TEXT, bairro TEXT, cep TEXT, endereco TEXT,
            total_pedido NUMERIC DEFAULT 0, marketplace TEXT, canal_venda TEXT,
            transportadora TEXT, forma_envio TEXT, frete_por_conta TEXT,
            codigo_rastreamento TEXT, url_rastreamento TEXT,
            vendedor TEXT, origem_pedido INT, atualizado_em TIMESTAMP DEFAULT NOW()
          )`);
          await pool.query(`
            INSERT INTO bd_pedidos_tiny_${safeName}
              (id_tiny,numero,numero_ecommerce,numero_canal_venda,data_criacao,data_previsao,situacao,
               nome_cliente,cpf_cnpj,tipo_pessoa,email_cliente,telefone_cliente,celular_cliente,codigo_cliente,
               uf,municipio,bairro,cep,endereco,total_pedido,marketplace,canal_venda,
               transportadora,forma_envio,frete_por_conta,codigo_rastreamento,url_rastreamento,
               vendedor,origem_pedido,atualizado_em)
            SELECT * FROM UNNEST(
              $1::text[],$2::text[],$3::text[],$4::text[],
              $5::date[],$6::date[],$7::int[],
              $8::text[],$9::text[],$10::text[],$11::text[],$12::text[],$13::text[],$14::text[],
              $15::text[],$16::text[],$17::text[],$18::text[],$19::text[],
              $20::numeric[],$21::text[],$22::text[],
              $23::text[],$24::text[],$25::text[],$26::text[],$27::text[],
              $28::text[],$29::int[],
              (SELECT array_agg(NOW()) FROM generate_series(1,$30))
            )
            ON CONFLICT (id_tiny) DO UPDATE SET
              situacao=EXCLUDED.situacao, total_pedido=EXCLUDED.total_pedido,
              data_previsao=EXCLUDED.data_previsao, marketplace=EXCLUDED.marketplace,
              codigo_rastreamento=EXCLUDED.codigo_rastreamento,
              url_rastreamento=EXCLUDED.url_rastreamento, atualizado_em=NOW()
          `, [
            rows.map(r=>r[0]),  rows.map(r=>r[1]),  rows.map(r=>r[2]),  rows.map(r=>r[3]),
            rows.map(r=>r[4]),  rows.map(r=>r[5]),  rows.map(r=>r[6]),
            rows.map(r=>r[7]),  rows.map(r=>r[8]),  rows.map(r=>r[9]),  rows.map(r=>r[10]),
            rows.map(r=>r[11]), rows.map(r=>r[12]), rows.map(r=>r[13]),
            rows.map(r=>r[14]), rows.map(r=>r[15]), rows.map(r=>r[16]), rows.map(r=>r[17]), rows.map(r=>r[18]),
            rows.map(r=>r[19]), rows.map(r=>r[20]), rows.map(r=>r[21]),
            rows.map(r=>r[22]), rows.map(r=>r[23]), rows.map(r=>r[24]), rows.map(r=>r[25]), rows.map(r=>r[26]),
            rows.map(r=>r[27]), rows.map(r=>r[28]), rows.length,
          ]);
        }

        // ── 2. ENRICH: enriquecer novos pedidos (últimos 3 dias) ──
        await pool.query(`ALTER TABLE bd_pedidos_tiny_${safeName} ADD COLUMN IF NOT EXISTS financeiro_ok BOOLEAN DEFAULT FALSE`).catch(() => {});
        const financialCols = [
          ['total_produtos', 'NUMERIC'], ['total_desconto', 'NUMERIC'], ['total_frete', 'NUMERIC'],
          ['total_impostos', 'NUMERIC'], ['total_outras_despesas', 'NUMERIC'], ['frete_pago', 'NUMERIC'],
          ['custo_produtos', 'NUMERIC'], ['margem_contribuicao', 'NUMERIC'], ['margem_pct', 'NUMERIC'],
          ['qtd_itens', 'INT'],
        ];
        for (const [col, type] of financialCols)
          await pool.query(`ALTER TABLE bd_pedidos_tiny_${safeName} ADD COLUMN IF NOT EXISTS ${col} ${type}`).catch(() => {});

        const threeDaysAgo = new Date(Date.now() - 3 * 86400000).toISOString().split('T')[0];
        const toEnrich = await pool.query(
          `SELECT id_tiny FROM bd_pedidos_tiny_${safeName} WHERE financeiro_ok IS NOT TRUE AND data_criacao >= $1 ORDER BY data_criacao DESC LIMIT 60`,
          [threeDaysAgo]
        );

        let enriched = 0;
        for (const { id_tiny } of toEnrich.rows) {
          try {
            const r = await fetch(`${TINY_API}/pedidos/${id_tiny}`, { headers: { Authorization: 'Bearer ' + token } });
            if (r.status === 429) { await new Promise(res => setTimeout(res, 5000)); continue; }
            if (!r.ok) { await pool.query(`UPDATE bd_pedidos_tiny_${safeName} SET financeiro_ok=TRUE WHERE id_tiny=$1`, [id_tiny]); continue; }
            const d = await r.json();
            const p = d.data || d;
            const tp  = parseFloat(p.totalProdutos || 0) || 0;
            const td  = parseFloat(p.totalDesconto || 0) || 0;
            const tf  = parseFloat(p.totalFrete || 0) || 0;
            const ti  = parseFloat(p.totalImpostos || 0) || 0;
            const to2 = parseFloat(p.totalOutrasDespesas || 0) || 0;
            const fp  = parseFloat(p.fretePago || p.transportador?.valorFrete || 0) || 0;
            let custo = 0, qtd = 0;
            if (Array.isArray(p.itens)) for (const item of p.itens) {
              custo += (parseFloat(item.precoCusto || item.produto?.precoCusto || 0) || 0) * (parseFloat(item.quantidade || 1) || 1);
              qtd   += parseFloat(item.quantidade || 1) || 1;
            }
            const receita = tp - td + tf;
            const margem  = receita - custo - fp - ti - to2;
            await pool.query(`UPDATE bd_pedidos_tiny_${safeName} SET
              total_produtos=$2,total_desconto=$3,total_frete=$4,total_impostos=$5,
              total_outras_despesas=$6,frete_pago=$7,custo_produtos=$8,
              margem_contribuicao=$9,margem_pct=$10,qtd_itens=$11,financeiro_ok=TRUE,atualizado_em=NOW()
              WHERE id_tiny=$1`,
              [id_tiny, tp, td, tf, ti, to2, fp, custo, margem, receita > 0 ? Math.round(margem / receita * 10000) / 100 : 0, qtd]);
            enriched++;
            await new Promise(res => setTimeout(res, 1000));
          } catch { /* skip */ }
        }

        // Salvar timestamp de sync
        await pool.query(`INSERT INTO configuracoes (empresa,chave,valor,atualizado_em) VALUES ($1,$2,$3,NOW()) ON CONFLICT (empresa,chave) DO UPDATE SET valor=EXCLUDED.valor,atualizado_em=NOW()`,
          [company, account + '_token_sync', new Date().toLocaleString('pt-BR')]);

        log.push({ company, account, synced: pedidos.length, enriched });
      }
    } catch (e) {
      log.push({ company, error: e.message });
    }
  }

  return res.json({ ok: true, ran_at: new Date().toISOString(), log });
};
