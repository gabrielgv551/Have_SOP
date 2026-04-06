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
    password: process.env[`${key}_PASSWORD`], ssl: { rejectUnauthorized: false }, max: 5,
  });
  return pools[company];
}

function verifyToken(req, res) {
  const auth = (req.headers.authorization || '').split(' ')[1];
  if (!auth) { res.status(401).json({ error: 'Token nao fornecido' }); return null; }
  try { return jwt.verify(auth, process.env.JWT_SECRET); }
  catch { res.status(401).json({ error: 'Token invalido' }); return null; }
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

      // Get de-para mappings
      const dpR = await pool.query(
        'SELECT palavra_chave, categoria_nome FROM caixa_de_para WHERE empresa=$1',
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

      return res.json({ categorias, valores });
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
