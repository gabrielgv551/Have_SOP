const jwt = require('jsonwebtoken');
const { Pool } = require('pg');
const companies = require('../lib/companies');

const pools = {};
function getPool(company) {
  if (pools[company]) return pools[company];
  const key = (companies[company] && companies[company].dbEnvKey) || company.toUpperCase();
  pools[company] = new Pool({
    host: process.env[`${key}_HOST`], port: parseInt(process.env[`${key}_PORT`] || '5432'),
    database: process.env[`${key}_DB`], user: process.env[`${key}_USER`],
    password: process.env[`${key}_PASSWORD`], ssl: { rejectUnauthorized: false }, max: 1,
  });
  return pools[company];
}

function verifyToken(req, res) {
  const auth = (req.headers.authorization || '').split(' ')[1];
  if (!auth) { res.status(401).json({ error: 'Token nao fornecido' }); return null; }
  try { return jwt.verify(auth, process.env.JWT_SECRET); }
  catch { res.status(401).json({ error: 'Token invalido' }); return null; }
}

const BR_FERIADOS = new Set(['01-01','04-21','05-01','09-07','10-12','11-02','11-15','11-20','12-25']);
function nextBizDay(d) {
  let dt = (d instanceof Date) ? new Date(d.getTime()) : new Date(String(d).slice(0,10) + 'T12:00:00Z');
  for (let i = 0; i < 7; i++) {
    const dow = dt.getUTCDay();
    const mmdd = String(dt.getUTCMonth()+1).padStart(2,'0') + '-' + String(dt.getUTCDate()).padStart(2,'0');
    if (dow !== 0 && dow !== 6 && !BR_FERIADOS.has(mmdd)) break;
    dt.setUTCDate(dt.getUTCDate() + 1);
  }
  return dt;
}

const DEFAULT_CATEGORIAS = [
  { nome: 'SALDO INICIAL DO CAIXA', tipo: 'saldo_ini', parent: null, ordem: 0 },

  // ── ATIVIDADES OPERACIONAIS ─────────────────────────────────
  { nome: 'ATIVIDADES OPERACIONAIS', tipo: 'section', parent: null, ordem: 10 },

  { nome: 'ENTRADAS', tipo: 'section', parent: 'ATIVIDADES OPERACIONAIS', ordem: 11 },
  { nome: 'MERCADO LIVRE', tipo: 'item', parent: 'ENTRADAS', ordem: 111 },
  { nome: 'SHOPEE', tipo: 'item', parent: 'ENTRADAS', ordem: 112 },
  { nome: 'AMAZON', tipo: 'item', parent: 'ENTRADAS', ordem: 113 },
  { nome: 'MAGALU', tipo: 'item', parent: 'ENTRADAS', ordem: 114 },
  { nome: 'TIK TOK', tipo: 'item', parent: 'ENTRADAS', ordem: 115 },
  { nome: 'ALI EXPRESS', tipo: 'item', parent: 'ENTRADAS', ordem: 116 },
  { nome: 'TEMU', tipo: 'item', parent: 'ENTRADAS', ordem: 117 },
  { nome: 'KWAI', tipo: 'item', parent: 'ENTRADAS', ordem: 118 },
  { nome: 'DAFITI', tipo: 'item', parent: 'ENTRADAS', ordem: 119 },
  { nome: 'B2B', tipo: 'item', parent: 'ENTRADAS', ordem: 120 },
  { nome: 'OUTRAS ENTRADAS', tipo: 'item', parent: 'ENTRADAS', ordem: 121 },

  { nome: 'SAÍDAS', tipo: 'section', parent: 'ATIVIDADES OPERACIONAIS', ordem: 20 },
  { nome: 'FORNECEDORES', tipo: 'item', parent: 'SAÍDAS', ordem: 201 },
  { nome: 'MATERIAL DE EMBALAGEM', tipo: 'item', parent: 'SAÍDAS', ordem: 202 },
  { nome: 'FRETE DE COMPRA', tipo: 'item', parent: 'SAÍDAS', ordem: 203 },
  { nome: 'FRETE DE VENDA', tipo: 'item', parent: 'SAÍDAS', ordem: 204 },
  { nome: 'MARKETING', tipo: 'item', parent: 'SAÍDAS', ordem: 205 },
  { nome: 'PESSOAL - SALÁRIOS E ENCARGOS', tipo: 'item', parent: 'SAÍDAS', ordem: 206 },
  { nome: 'PESSOAL - BENEFÍCIOS', tipo: 'item', parent: 'SAÍDAS', ordem: 207 },
  { nome: 'BONIFICAÇÕES', tipo: 'item', parent: 'SAÍDAS', ordem: 208 },
  { nome: 'RETIRADA SÓCIOS', tipo: 'item', parent: 'SAÍDAS', ordem: 209 },
  { nome: 'COMBUSTÍVEL', tipo: 'item', parent: 'SAÍDAS', ordem: 210 },
  { nome: 'ALUGUEL', tipo: 'item', parent: 'SAÍDAS', ordem: 211 },
  { nome: 'ENERGIA', tipo: 'item', parent: 'SAÍDAS', ordem: 212 },
  { nome: 'ÁGUA', tipo: 'item', parent: 'SAÍDAS', ordem: 213 },
  { nome: 'MANUTENÇÃO', tipo: 'item', parent: 'SAÍDAS', ordem: 214 },
  { nome: 'LIMPEZA', tipo: 'item', parent: 'SAÍDAS', ordem: 215 },
  { nome: 'MATERIAIS DE CONSUMO', tipo: 'item', parent: 'SAÍDAS', ordem: 216 },
  { nome: 'INTERNET', tipo: 'item', parent: 'SAÍDAS', ordem: 217 },
  { nome: 'SISTEMAS', tipo: 'item', parent: 'SAÍDAS', ordem: 218 },
  { nome: 'PRESTAÇÃO DE SERVIÇOS', tipo: 'item', parent: 'SAÍDAS', ordem: 219 },
  { nome: 'CARTÃO DE CRÉDITO', tipo: 'item', parent: 'SAÍDAS', ordem: 220 },
  { nome: 'IMPOSTOS ESTADUAIS', tipo: 'item', parent: 'SAÍDAS', ordem: 221 },
  { nome: 'IMPOSTOS FEDERAIS', tipo: 'item', parent: 'SAÍDAS', ordem: 222 },
  { nome: 'OUTRAS SAÍDAS', tipo: 'item', parent: 'SAÍDAS', ordem: 223 },

  // ── ATIVIDADES NÃO OPERACIONAIS ─────────────────────────────
  { nome: 'ATIVIDADES NÃO OPERACIONAIS', tipo: 'section', parent: null, ordem: 50 },

  { nome: 'ANO ENTRADAS', tipo: 'section', parent: 'ATIVIDADES NÃO OPERACIONAIS', ordem: 51 },
  { nome: 'RECEITAS FINANCEIRAS', tipo: 'item', parent: 'ANO ENTRADAS', ordem: 511 },
  { nome: 'CAPTAÇÃO DE EMPRÉSTIMOS', tipo: 'item', parent: 'ANO ENTRADAS', ordem: 512 },
  { nome: 'RESGATE DE APLICAÇÕES', tipo: 'item', parent: 'ANO ENTRADAS', ordem: 513 },
  { nome: 'OUTRAS ENTRADAS / APLICAÇÕES', tipo: 'item', parent: 'ANO ENTRADAS', ordem: 514 },

  { nome: 'ANO SAÍDAS', tipo: 'section', parent: 'ATIVIDADES NÃO OPERACIONAIS', ordem: 54 },
  { nome: 'IMOBILIZADO', tipo: 'item', parent: 'ANO SAÍDAS', ordem: 541 },
  { nome: 'INVESTIMENTOS', tipo: 'item', parent: 'ANO SAÍDAS', ordem: 542 },
  { nome: 'PARTICIPAÇÕES', tipo: 'item', parent: 'ANO SAÍDAS', ordem: 543 },
  { nome: 'PAGAMENTO DE EMPRÉSTIMOS', tipo: 'item', parent: 'ANO SAÍDAS', ordem: 544 },
  { nome: 'JUROS', tipo: 'item', parent: 'ANO SAÍDAS', ordem: 545 },
  { nome: 'DESPESAS BANCÁRIAS', tipo: 'item', parent: 'ANO SAÍDAS', ordem: 546 },
  { nome: 'DIVIDENDOS', tipo: 'item', parent: 'ANO SAÍDAS', ordem: 547 },
  { nome: 'OUTRAS SAÍDAS NÃO OPERACIONAIS', tipo: 'item', parent: 'ANO SAÍDAS', ordem: 548 },

  { nome: 'SALDO FINAL', tipo: 'saldo_fin', parent: null, ordem: 99 },
];

async function seedDefaults(pool, company) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    for (const cat of DEFAULT_CATEGORIAS) {
      await client.query(
        `INSERT INTO caixa_categorias (empresa, nome, tipo, parent, ordem)
         VALUES ($1,$2,$3,$4,$5) ON CONFLICT (empresa, nome) DO NOTHING`,
        [company, cat.nome, cat.tipo, cat.parent, cat.ordem]
      );
    }
    await client.query('COMMIT');
  } catch (e) { await client.query('ROLLBACK'); throw e; }
  finally { client.release(); }
}

module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, DELETE, PATCH, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  if (req.method === 'OPTIONS') return res.status(200).end();

  const payload = verifyToken(req, res);
  if (!payload) return;
  const company = payload.company || 'lanzi';
  const pool = getPool(company);

  try {
    await pool.query(`ALTER TABLE pedidos_compra ADD COLUMN IF NOT EXISTS linha_fluxo VARCHAR(200)`);

    if (req.method === 'GET') {
      const { ano, mes } = req.query;

      // Ensure defaults exist
      const countR = await pool.query('SELECT COUNT(*) FROM caixa_categorias WHERE empresa=$1', [company]);
      if (parseInt(countR.rows[0].count) === 0) await seedDefaults(pool, company);

      const catsR = await pool.query(
        'SELECT id, nome, tipo, parent, ordem FROM caixa_categorias WHERE empresa=$1 ORDER BY ordem',
        [company]
      );
      const categorias = catsR.rows;

      if (!ano || !mes) return res.json({ categorias });

      const { apenas_futuros } = req.query;
      const somenteF = apenas_futuros !== 'false'; // default true

      // Get de-para mappings (extrato)
      const dpR = await pool.query(
        "SELECT palavra_chave, categoria_nome FROM caixa_de_para WHERE empresa=$1 AND tipo='extrato'",
        [company]
      );
      const depara = dpR.rows; // [{palavra_chave, categoria_nome}]

      // Get extract rows for the period
      const extR = await pool.query(
        'SELECT dia, descricao, valor FROM caixa_extrato WHERE empresa=$1 AND ano=$2 AND mes=$3',
        [company, parseInt(ano), parseInt(mes)]
      );
      const extractRows = extR.rows;

      // Compute: for each (dia, categoria_nome) → sum of matched extract rows
      const valores = {}; // { categoria_nome: { dia: total_centavos } }
      for (const extRow of extractRows) {
        const descLower = (extRow.descricao || '').toLowerCase();
        for (const dp of depara) {
          if (descLower.includes(dp.palavra_chave.toLowerCase())) {
            const catNome = dp.categoria_nome;
            if (!valores[catNome]) valores[catNome] = {};
            const dia = parseInt(extRow.dia);
            valores[catNome][dia] = (valores[catNome][dia] || 0) + parseInt(extRow.valor);
            break; // first matching keyword wins
          }
        }
      }

      // ── PREVISÃO: Contas a Pagar ────────────────────────────────────────
      // Get contas_pagar de-para mappings
      const cpDpR = await pool.query(
        "SELECT palavra_chave, categoria_nome FROM caixa_de_para WHERE empresa=$1 AND tipo='contas_pagar'",
        [company]
      );
      const cpDepara = cpDpR.rows; // [{palavra_chave, categoria_nome}]

      const valores_previsao = {}; // { categoria_nome: { dia: total_centavos } }

      if (cpDepara.length > 0) {
        // Build date bounds: mes/ano first and last day
        const a = parseInt(ano), m = parseInt(mes);
        const mesStart = `${a}-${String(m).padStart(2,'0')}-01`;
        const daysInMonth = new Date(a, m, 0).getDate();
        const mesEnd = `${a}-${String(m).padStart(2,'0')}-${String(daysInMonth).padStart(2,'0')}`;
        const today = new Date().toISOString().slice(0, 10);

        let cpQuery = `SELECT fornecedor, saldo, data_vencimento
          FROM contas_pagar
          WHERE data_vencimento IS NOT NULL
            AND data_vencimento::date BETWEEN $1::date AND $2::date`;
        const cpParams = [mesStart, mesEnd];

        if (somenteF) {
          cpQuery += ` AND data_vencimento::date > $3::date`;
          cpParams.push(today);
        }

        const cpR = await pool.query(cpQuery, cpParams);

        for (const cpRow of cpR.rows) {
          const fornLower = (cpRow.fornecedor || '').toLowerCase();
          for (const dp of cpDepara) {
            if (fornLower.includes(dp.palavra_chave.toLowerCase())) {
              const bizD = nextBizDay(cpRow.data_vencimento);
              if (bizD.getUTCMonth() + 1 !== m) break;
              const catNome = dp.categoria_nome;
              if (!valores_previsao[catNome]) valores_previsao[catNome] = {};
              const dia = bizD.getUTCDate();
              // saldo is in reais (NUMERIC), convert to centavos negative (it's a payment)
              const centavos = -Math.round(parseFloat(cpRow.saldo || 0) * 100);
              valores_previsao[catNome][dia] = (valores_previsao[catNome][dia] || 0) + centavos;
              break; // first matching keyword wins
            }
          }
        }
      }

      // ── PREVISÃO: Pedidos de Compra ─────────────────────────────────────
      {
        const a = parseInt(ano), m = parseInt(mes);
        const mesStart = `${a}-${String(m).padStart(2,'0')}-01`;
        const daysInMonth = new Date(a, m, 0).getDate();
        const mesEnd = `${a}-${String(m).padStart(2,'0')}-${String(daysInMonth).padStart(2,'0')}`;
        const today = new Date().toISOString().slice(0, 10);

        let pcQuery = `SELECT valor, vencimento, vencimento_ajustado, linha_fluxo
          FROM pedidos_compra
          WHERE empresa=$1
            AND linha_fluxo IS NOT NULL
            AND COALESCE(vencimento_ajustado, vencimento)::date BETWEEN $2::date AND $3::date`;
        const pcParams = [company, mesStart, mesEnd];

        if (somenteF) {
          pcQuery += ` AND COALESCE(vencimento_ajustado, vencimento)::date > $4::date`;
          pcParams.push(today);
        }

        const pcR = await pool.query(pcQuery, pcParams);
        for (const row of pcR.rows) {
          const effDate = (row.vencimento_ajustado || row.vencimento);
          const bizD = nextBizDay(effDate);
          if (bizD.getUTCMonth() + 1 !== m) continue;
          const catNome = row.linha_fluxo;
          const dia = bizD.getUTCDate();
          const centavos = -Math.round(parseFloat(row.valor || 0) * 100);
          if (!valores_previsao[catNome]) valores_previsao[catNome] = {};
          valores_previsao[catNome][dia] = (valores_previsao[catNome][dia] || 0) + centavos;
        }
      }

      // ── PREVISÃO: Receitas de Vendas (A Receber) ────────────────────────
      {
        const a = parseInt(ano), m = parseInt(mes);
        const mesStart = `${a}-${String(m).padStart(2,'0')}-01`;
        const daysInMonth = new Date(a, m, 0).getDate();
        const mesEnd = `${a}-${String(m).padStart(2,'0')}-${String(daysInMonth).padStart(2,'0')}`;
        const today = new Date().toISOString().slice(0, 10);

        const vdpR = await pool.query(
          `SELECT palavra_chave, categoria_nome FROM caixa_de_para WHERE empresa=$1 AND tipo='vendas'`,
          [company]
        );
        const vendasDepara = vdpR.rows;

        if (vendasDepara.length > 0) {
          // Build canal→group map for resolution
          const grpR = await pool.query(
            `SELECT grupo, canal FROM vendas_grupos_canais WHERE empresa=$1`, [company]
          );
          const canalToGrupo = {};
          grpR.rows.forEach(({ grupo, canal }) => { canalToGrupo[canal.toLowerCase()] = grupo; });

          let vQuery = `
            SELECT
              (bv."Data"::date + COALESCE(v.lead_time_dias, 3)) AS vencimento,
              COALESCE(NULLIF(TRIM(bv."Canal Apelido"::text), ''), TRIM(bv."Canal de venda"), 'Sem canal') AS canal,
              SUM(COALESCE(bv."Repasse Financeiro"::numeric, 0)) AS repasse
            FROM bd_vendas bv
            LEFT JOIN vendas_canais_config v
              ON v.canal = COALESCE(NULLIF(TRIM(bv."Canal Apelido"::text), ''), TRIM(bv."Canal de venda"), 'Sem canal')
              AND v.empresa = $1
            WHERE bv."Data" IS NOT NULL
              AND bv."Status" !~* '(cancel|devol|n[aã]o.?pago)'
              AND (bv."Data"::date + COALESCE(v.lead_time_dias, 3)) BETWEEN $2::date AND $3::date`;
          const vParams = [company, mesStart, mesEnd];

          if (somenteF) {
            vQuery += ` AND (bv."Data"::date + COALESCE(v.lead_time_dias, 3)) > $4::date`;
            vParams.push(today);
          }

          vQuery += `
            GROUP BY 1, COALESCE(NULLIF(TRIM(bv."Canal Apelido"::text), ''), TRIM(bv."Canal de venda"), 'Sem canal')
            ORDER BY 1`;

          const vR = await pool.query(vQuery, vParams);
          for (const row of vR.rows) {
            const canalLower = (row.canal || '').toLowerCase();
            // Resolve to group name if canal belongs to a group
            const chave = (canalToGrupo[canalLower] || row.canal).toLowerCase();
            const dp = vendasDepara.find(d => d.palavra_chave.toLowerCase() === chave);
            if (!dp) continue;
            const bizD = nextBizDay(row.vencimento);
            if (bizD.getUTCMonth() + 1 !== m) continue;
            const catNome = dp.categoria_nome;
            const dia = bizD.getUTCDate();
            const centavos = Math.round(parseFloat(row.repasse || 0) * 100);
            if (!valores_previsao[catNome]) valores_previsao[catNome] = {};
            valores_previsao[catNome][dia] = (valores_previsao[catNome][dia] || 0) + centavos;
          }
        }
      }

      // ── PREVISÃO: Receitas de Vendas Futuras (Forecast) ─────────────────
      // Only for future months (entire month is after today) — avoids double-counting with A Receber
      {
        const a = parseInt(ano), m = parseInt(mes);
        const mesStart = `${a}-${String(m).padStart(2,'0')}-01`;
        const daysInMonth = new Date(a, m, 0).getDate();
        const mesEnd = `${a}-${String(m).padStart(2,'0')}-${String(daysInMonth).padStart(2,'0')}`;
        const today = new Date().toISOString().slice(0, 10);

        if (mesStart > today) {
          // Re-use the vendas de-para already loaded above
          const vdpR2 = await pool.query(
            `SELECT palavra_chave, categoria_nome FROM caixa_de_para WHERE empresa=$1 AND tipo='vendas'`,
            [company]
          );
          const vendasDepara2 = vdpR2.rows;

          if (vendasDepara2.length > 0) {
            const grpR2 = await pool.query(
              `SELECT grupo, canal FROM vendas_grupos_canais WHERE empresa=$1`, [company]
            );
            const canalToGrupo2 = {};
            grpR2.rows.forEach(({ grupo, canal }) => { canalToGrupo2[canal.toLowerCase()] = grupo; });

            // Avg repasse per sku×canal
            const repR = await pool.query(`
              SELECT
                TRIM("Sku"::text) AS sku,
                COALESCE(NULLIF(TRIM("Canal Apelido"::text),''), TRIM("Canal de venda"), 'Sem canal') AS canal,
                ROUND(AVG(COALESCE("Repasse Financeiro"::numeric,0) / NULLIF("Quantidade Vendida"::numeric,0))::numeric,4) AS rep_und
              FROM bd_vendas
              WHERE "Status" !~* '(cancel|devol|n[aã]o.?pago)'
                AND "Quantidade Vendida"::numeric > 0
                AND "Sku" IS NOT NULL AND TRIM("Sku"::text) != ''
              GROUP BY 1, 2
            `);
            const repMap = {};
            repR.rows.forEach(({ sku, canal, rep_und }) => { repMap[sku + '§§' + canal] = parseFloat(rep_und || 0); });
            const repSkuMap = {};
            repR.rows.forEach(({ sku, rep_und }) => { if (!repSkuMap[sku]) repSkuMap[sku] = parseFloat(rep_und || 0); });

            // Forecast diário with lead time → receipt date
            const fcR = await pool.query(`
              SELECT
                fd.canal,
                (fd.data::date + COALESCE(v.lead_time_dias, 3)) AS vencimento,
                fd.sku,
                SUM(fd.quantidade_prevista) AS qtd
              FROM forecast_diario fd
              LEFT JOIN vendas_canais_config v ON v.canal = fd.canal AND v.empresa = $1
              WHERE (fd.data::date + COALESCE(v.lead_time_dias, 3)) BETWEEN $2::date AND $3::date
              GROUP BY fd.canal, (fd.data::date + COALESCE(v.lead_time_dias, 3)), fd.sku
              ORDER BY (fd.data::date + COALESCE(v.lead_time_dias, 3))
            `, [company, mesStart, mesEnd]);

            for (const row of fcR.rows) {
              const qtd = parseFloat(row.qtd || 0);
              if (!qtd) continue;
              const rep = repMap[row.sku + '§§' + row.canal] || repSkuMap[row.sku] || 0;
              if (!rep) continue;
              const canalLower = (row.canal || '').toLowerCase();
              const chave = (canalToGrupo2[canalLower] || row.canal).toLowerCase();
              const dp = vendasDepara2.find(d => d.palavra_chave.toLowerCase() === chave);
              if (!dp) continue;
              const bizD = nextBizDay(row.vencimento);
              if (bizD.getUTCMonth() + 1 !== m) continue;
              const catNome = dp.categoria_nome;
              const dia = bizD.getUTCDate();
              const centavos = Math.round(qtd * rep * 100);
              if (!valores_previsao[catNome]) valores_previsao[catNome] = {};
              valores_previsao[catNome][dia] = (valores_previsao[catNome][dia] || 0) + centavos;
            }
          }
        }
      }

      return res.json({ categorias, valores, valores_previsao });
    }

    if (req.method === 'POST') {
      const { action } = req.body;

      if (action === 'add') {
        const { nome, tipo, parent } = req.body;
        if (!nome) return res.status(400).json({ error: 'Nome obrigatorio' });
        // Get max ordem
        const maxR = await pool.query('SELECT MAX(ordem) as m FROM caixa_categorias WHERE empresa=$1', [company]);
        const ordem = (parseInt(maxR.rows[0].m) || 0) + 1;
        const r = await pool.query(
          `INSERT INTO caixa_categorias (empresa, nome, tipo, parent, ordem)
           VALUES ($1,$2,$3,$4,$5) ON CONFLICT (empresa, nome) DO UPDATE SET tipo=EXCLUDED.tipo, parent=EXCLUDED.parent
           RETURNING id, nome, tipo, parent, ordem`,
          [company, nome.substring(0, 100), tipo || 'item', parent || null, ordem]
        );
        return res.json({ ok: true, categoria: r.rows[0] });
      }

      if (action === 'reset_defaults') {
        const client = await pool.connect();
        try {
          await client.query('BEGIN');
          await client.query('DELETE FROM caixa_categorias WHERE empresa=$1', [company]);
          await client.query('COMMIT');
        } catch (e) { await client.query('ROLLBACK'); throw e; }
        finally { client.release(); }
        await seedDefaults(pool, company);
        return res.json({ ok: true, count: DEFAULT_CATEGORIAS.length });
      }

      if (action === 'bulk_save') {
        const { categorias } = req.body;
        if (!Array.isArray(categorias)) return res.status(400).json({ error: 'categorias deve ser array' });
        const client = await pool.connect();
        try {
          await client.query('BEGIN');
          for (const cat of categorias) {
            if (!cat.id) continue;
            await client.query(
              'UPDATE caixa_categorias SET nome=$1, parent=$2, ordem=$3 WHERE id=$4 AND empresa=$5',
              [String(cat.nome).substring(0,100), cat.parent||null, parseInt(cat.ordem)||0, parseInt(cat.id), company]
            );
          }
          await client.query('COMMIT');
        } catch (e) { await client.query('ROLLBACK'); throw e; }
        finally { client.release(); }
        return res.json({ ok: true });
      }

      return res.status(400).json({ error: 'action invalida' });
    }

    if (req.method === 'DELETE') {
      const { id } = req.query;
      if (!id) return res.status(400).json({ error: 'Informe id' });
      await pool.query('DELETE FROM caixa_categorias WHERE id=$1 AND empresa=$2', [parseInt(id), company]);
      return res.json({ ok: true });
    }

    res.status(405).json({ error: 'Method not allowed' });
  } catch (e) {
    console.error('[CAIXA-MODELO]', e.message);
    res.status(500).json({ error: e.message });
  }
};
