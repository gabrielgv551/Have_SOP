const jwt = require('jsonwebtoken');
const { getPool } = require('../lib/db');

function verifyToken(req, res) {
  const auth = (req.headers.authorization || '').split(' ')[1];
  if (!auth) { res.status(401).json({ error: 'Token nao fornecido' }); return null; }
  try { return jwt.verify(auth, process.env.JWT_SECRET); }
  catch { res.status(401).json({ error: 'Token invalido' }); return null; }
}

module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, DELETE, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  if (req.method === 'OPTIONS') return res.status(200).end();

  const payload = verifyToken(req, res);
  if (!payload) return;
  const company = payload.company || 'lanzi';
  const pool = getPool(company);

  try {
    if (req.method === 'GET') {
      const { source, ano, mes, tipo } = req.query;

      // Return unique descriptions from bank extract not yet mapped (tipo='extrato')
      if (source === 'extract') {
        const query = ano && mes
          ? 'SELECT DISTINCT descricao, razao_social FROM caixa_extrato WHERE empresa=$1 AND ano=$2 AND mes=$3 ORDER BY descricao'
          : 'SELECT DISTINCT descricao, razao_social FROM caixa_extrato WHERE empresa=$1 ORDER BY descricao';
        const params = ano && mes ? [company, parseInt(ano), parseInt(mes)] : [company];
        const r = await pool.query(query, params);
        const dpR = await pool.query(
          "SELECT palavra_chave, razao_social, cnpj FROM caixa_de_para WHERE empresa=$1 AND tipo='extrato'",
          [company]
        );
        const rules = dpR.rows;

        // Helper: extract RS from description if not stored (data imported before migration 026)
        const extractRS = (row) => {
          if (row.razao_social) return row.razao_social;
          const parts = (row.descricao || '').split(' \u00b7 ');
          return parts.length > 1 ? parts.slice(1).join(' \u00b7 ').trim() : null;
        };

        // Group rows by effective razao_social (primary) or descricao (fallback)
        const groupMap = new Map();
        for (const row of r.rows) {
          const rs = extractRS(row);
          const key = rs ? `__RS__${rs}` : `__D__${row.descricao}`;
          if (!groupMap.has(key)) {
            groupMap.set(key, { razao_social: rs, descricoes: [] });
          }
          groupMap.get(key).descricoes.push(row.descricao);
        }
        const groups = [...groupMap.values()];

        const isGroupClassified = (g) => {
          const rsLower = (g.razao_social || '').toLowerCase();
          return rules.some(dp => {
            const dpCNPJ = (dp.cnpj || '').replace(/\D/g, '');
            if (dpCNPJ) return false; // cnpj rules only match via openfinance source
            if (dp.razao_social) {
              return rsLower && rsLower.includes(dp.razao_social.toLowerCase());
            }
            return dp.palavra_chave && g.descricoes.some(d =>
              d.toLowerCase().includes(dp.palavra_chave.toLowerCase()));
          });
        };

        const naoClassificadas = groups.filter(g => !isGroupClassified(g));
        return res.json({ nao_classificadas: naoClassificadas });
      }

      // Return unclassified Open Finance (Pluggy) transactions grouped by CNPJ/RS
      if (source === 'openfinance') {
        const r = await pool.query(
          `SELECT DISTINCT counterparty_document, razao_social, descricao
           FROM caixa_extrato
           WHERE empresa=$1 AND belvo_tx_id IS NOT NULL
           ORDER BY razao_social NULLS LAST, descricao`,
          [company]
        );
        const dpR = await pool.query(
          "SELECT palavra_chave, razao_social, cnpj FROM caixa_de_para WHERE empresa=$1 AND tipo='extrato'",
          [company]
        );
        const rules = dpR.rows;

        // Group by CNPJ (primary) or RS (secondary) or description
        const groupMap = new Map();
        for (const row of r.rows) {
          const cnpjRaw = (row.counterparty_document || '').replace(/\D/g, '');
          const rs = row.razao_social || '';
          const key = cnpjRaw ? `__CNPJ__${cnpjRaw}` : rs ? `__RS__${rs}` : `__D__${row.descricao}`;
          if (!groupMap.has(key)) {
            groupMap.set(key, { cnpj: cnpjRaw || null, razao_social: rs || null, descricoes: [] });
          }
          const g = groupMap.get(key);
          if (!g.descricoes.includes(row.descricao)) g.descricoes.push(row.descricao);
        }
        const groups = [...groupMap.values()];

        const isClassified = (g) => {
          const rsLower = (g.razao_social || '').toLowerCase();
          const cnpjDoc = (g.cnpj || '').replace(/\D/g, '');
          return rules.some(dp => {
            const dpCNPJ = (dp.cnpj || '').replace(/\D/g, '');
            if (dpCNPJ && cnpjDoc) return cnpjDoc === dpCNPJ;
            if (dp.razao_social) return rsLower && rsLower.includes(dp.razao_social.toLowerCase());
            return dp.palavra_chave && g.descricoes.some(d =>
              d.toLowerCase().includes(dp.palavra_chave.toLowerCase()));
          });
        };

        return res.json({ nao_classificadas: groups.filter(g => !isClassified(g)) });
      }

      // Return groups + ungrouped canals for mapping (tipo='vendas')
      if (source === 'canais') {
        // Canals from bd_vendas (realized sales)
        const cR = await pool.query(
          `SELECT DISTINCT COALESCE(NULLIF(TRIM("Canal Apelido"::text),''), TRIM("Canal de venda"), 'Sem canal') AS canal
           FROM bd_vendas
           WHERE "Canal de venda" IS NOT NULL AND TRIM("Canal de venda") != ''`
        );
        // Canals from forecast_diario (future forecast)
        const fcR = await pool.query(
          `SELECT DISTINCT TRIM(canal::text) AS canal FROM forecast_diario
           WHERE canal IS NOT NULL AND TRIM(canal::text) != ''`
        ).catch(() => ({ rows: [] }));
        // Groups from vendas_grupos_canais
        const gR = await pool.query(
          `SELECT grupo, canal FROM vendas_grupos_canais WHERE empresa=$1`, [company]
        );
        // Build canal→group map
        const canalToGrupo = {};
        gR.rows.forEach(({ grupo, canal }) => { canalToGrupo[canal.toLowerCase()] = grupo; });
        // Keys = groups + ungrouped canals (from both realized and forecast)
        const keys = new Set();
        [...cR.rows, ...fcR.rows].forEach(({ canal }) => {
          const g = canalToGrupo[canal.toLowerCase()];
          keys.add(g || canal);
        });
        // Already mapped
        const dpR = await pool.query(
          `SELECT palavra_chave FROM caixa_de_para WHERE empresa=$1 AND tipo='vendas'`, [company]
        );
        const mapped = new Set(dpR.rows.map(x => x.palavra_chave.toLowerCase()));
        const allKeys = [...keys].sort();
        return res.json({
          canais: allKeys,
          nao_mapeados: allKeys.filter(k => !mapped.has(k.toLowerCase()))
        });
      }

      // Return unique fornecedores from pedidos_compra not yet mapped (tipo='pedidos_compra')
      if (source === 'pedidos_compra') {
        const r = await pool.query(
          "SELECT DISTINCT fornecedor FROM pedidos_compra WHERE empresa=$1 AND fornecedor IS NOT NULL AND fornecedor <> '' ORDER BY fornecedor",
          [company]
        );
        const dpR = await pool.query(
          "SELECT palavra_chave FROM caixa_de_para WHERE empresa=$1 AND tipo='pedidos_compra'",
          [company]
        );
        const mapped = new Set(dpR.rows.map(x => x.palavra_chave.toLowerCase()));
        return res.json({
          fornecedores: r.rows.map(x => x.fornecedor),
          nao_classificados: r.rows.map(x => x.fornecedor).filter(f => !mapped.has(f.toLowerCase()))
        });
      }

      // Return unique fornecedores from contas_pagar not yet mapped (tipo='contas_pagar')
      if (source === 'contas_pagar') {
        const r = await pool.query(
          "SELECT DISTINCT TRIM(fornecedor) AS fornecedor FROM contas_pagar WHERE fornecedor IS NOT NULL AND fornecedor <> '' ORDER BY 1"
        );
        const dpR = await pool.query(
          "SELECT palavra_chave FROM caixa_de_para WHERE empresa=$1 AND tipo='contas_pagar'",
          [company]
        );
        const rules = dpR.rows.map(x => (x.palavra_chave || '').trim().toLowerCase()).filter(Boolean);
        const isClassified = (f) => {
          const fl = f.trim().toLowerCase();
          return rules.some(pk => fl.includes(pk) || pk.includes(fl));
        };
        const fornecedores = r.rows.map(x => x.fornecedor);
        return res.json({
          fornecedores,
          nao_classificados: fornecedores.filter(f => !isClassified(f))
        });
      }

      // Return mappings, optionally filtered by tipo
      const tipoFilter = tipo ? ' AND tipo=$2' : '';
      const params = tipo ? [company, tipo] : [company];
      const r = await pool.query(
        `SELECT id, palavra_chave, razao_social, cnpj, categoria_nome, tipo FROM caixa_de_para WHERE empresa=$1${tipoFilter} ORDER BY tipo, categoria_nome, palavra_chave`,
        params
      );
      return res.json({ mappings: r.rows });
    }

    if (req.method === 'POST') {
      const { palavra_chave, razao_social, cnpj, categoria_nome, tipo } = req.body;
      const pkVal   = palavra_chave ? String(palavra_chave).trim().substring(0, 500) : null;
      const rsVal   = razao_social  ? String(razao_social).trim().substring(0, 300)  : null;
      const cnpjVal = cnpj          ? String(cnpj).replace(/\D/g, '').substring(0, 20) || null : null;
      if ((!pkVal && !rsVal && !cnpjVal) || !categoria_nome)
        return res.status(400).json({ error: 'Informe palavra_chave, razao_social ou cnpj, e categoria_nome' });
      const tipoVal = (tipo === 'contas_pagar') ? 'contas_pagar' : (tipo === 'vendas') ? 'vendas' : (tipo === 'pedidos_compra') ? 'pedidos_compra' : 'extrato';
      const r = await pool.query(
        `INSERT INTO caixa_de_para (empresa, palavra_chave, razao_social, cnpj, categoria_nome, tipo)
         VALUES ($1,$2,$3,$4,$5,$6)
         ON CONFLICT (empresa, tipo, COALESCE(razao_social,''), COALESCE(palavra_chave,''), COALESCE(cnpj,'')) DO UPDATE
           SET categoria_nome=EXCLUDED.categoria_nome, razao_social=EXCLUDED.razao_social,
               palavra_chave=EXCLUDED.palavra_chave, cnpj=EXCLUDED.cnpj
         RETURNING id, palavra_chave, razao_social, cnpj, categoria_nome, tipo`,
        [company, pkVal, rsVal, cnpjVal, categoria_nome.substring(0, 100), tipoVal]
      );
      return res.json({ ok: true, mapping: r.rows[0] });
    }

    if (req.method === 'DELETE') {
      const { id } = req.query;
      if (!id) return res.status(400).json({ error: 'Informe id' });
      await pool.query('DELETE FROM caixa_de_para WHERE id=$1 AND empresa=$2', [parseInt(id), company]);
      return res.json({ ok: true });
    }

    res.status(405).json({ error: 'Method not allowed' });
  } catch (e) {
    console.error('[CAIXA-DEPARA]', e.message);
    res.status(500).json({ error: e.message });
  }
};
