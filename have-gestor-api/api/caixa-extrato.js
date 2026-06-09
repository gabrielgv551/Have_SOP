const jwt = require('jsonwebtoken');
const { getPool, getCompanyPool } = require('../lib/db');

const PLUGGY_BASE = 'https://api.pluggy.ai';


function verifyToken(req, res) {
  const auth = (req.headers.authorization || '').split(' ')[1];
  if (!auth) { res.status(401).json({ error: 'Token nao fornecido' }); return null; }
  try { return jwt.verify(auth, process.env.JWT_SECRET); }
  catch { res.status(401).json({ error: 'Token invalido' }); return null; }
}

async function pluggyGetApiKey() {
  const r = await fetch(`${PLUGGY_BASE}/auth`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ clientId: process.env.PLUGGY_CLIENT_ID, clientSecret: process.env.PLUGGY_CLIENT_SECRET })
  });
  if (!r.ok) throw new Error(`Pluggy auth error ${r.status}: ${await r.text()}`);
  const data = await r.json();
  return data.apiKey;
}

function pluggyHeaders(apiKey) {
  return { 'X-API-KEY': apiKey, 'Content-Type': 'application/json' };
}

async function pluggyGet(apiKey, path) {
  return fetch(`${PLUGGY_BASE}${path}`, { headers: pluggyHeaders(apiKey) });
}

async function pluggyPost(apiKey, path, body = {}) {
  return fetch(`${PLUGGY_BASE}${path}`, { method: 'POST', headers: pluggyHeaders(apiKey), body: JSON.stringify(body) });
}

async function pluggyDelete(apiKey, path) {
  return fetch(`${PLUGGY_BASE}${path}`, { method: 'DELETE', headers: pluggyHeaders(apiKey) });
}

async function fetchAllExtratosTransactions(link_id, date_from, date_to) {
  const { Client } = require('pg');
  const client = new Client({
    host: process.env.EXTRATOS_HOST || '37.60.236.200',
    port: process.env.EXTRATOS_PORT || 5432,
    database: process.env.EXTRATOS_DB || 'extratos',
    user: process.env.EXTRATOS_USER || 'postgres',
    password: process.env.EXTRATOS_PASSWORD || '131105Gv',
  });
  await client.connect();
  try {
    const res = await client.query(`
      SELECT id, date, description, type, amount
      FROM transactions
      WHERE pluggy_item_id = $1
        AND date::date >= $2::date
        AND date::date <= $3::date

      UNION ALL

      SELECT id, date, description, type, amount
      FROM credit_transactions
      WHERE pluggy_item_id = $1
        AND date::date >= $2::date
        AND date::date <= $3::date

      ORDER BY date ASC
    `, [link_id, date_from, date_to]);
    return res.rows.map(tx => ({
      id: tx.id,
      date: tx.date,
      description: tx.description,
      type: tx.type, // 'DEBIT' or 'CREDIT'
      amount: tx.amount
    }));
  } finally {
    await client.end();
  }
}

async function handleBancos(req, res, pool, company) {
  if (req.method === 'GET') {
    const r = await pool.query(
      `SELECT b.id, b.nome, b.cor, b.icone, b.formato, b.criado_em,
              COUNT(DISTINCT (e.ano, e.mes))::int AS meses_com_dados,
              COUNT(e.id)::int AS total_lancamentos,
              MAX(e.atualizado_em) AS ultima_atualizacao
         FROM caixa_bancos b
         LEFT JOIN caixa_extrato e ON e.banco_id = b.id AND e.empresa = b.empresa
        WHERE b.empresa = $1
        GROUP BY b.id ORDER BY b.criado_em ASC`,
      [company]
    );
    return res.json({ bancos: r.rows });
  }

  if (req.method === 'POST') {
    const { nome, cor, icone, formato } = req.body;
    if (!nome) return res.status(400).json({ error: 'nome obrigatorio' });
    const r = await pool.query(
      `INSERT INTO caixa_bancos (empresa, nome, cor, icone, formato)
       VALUES ($1, $2, $3, $4, $5)
       ON CONFLICT (empresa, nome) DO UPDATE
         SET cor = EXCLUDED.cor, icone = EXCLUDED.icone, formato = EXCLUDED.formato
       RETURNING id, nome, cor, icone, formato`,
      [company, nome.substring(0, 100), (cor || '#007cdc').substring(0, 20), (icone || '🏦').substring(0, 10), (formato || 'generico').substring(0, 50)]
    );
    return res.json({ ok: true, banco: r.rows[0] });
  }

  if (req.method === 'DELETE') {
    const { id } = req.query;
    if (!id) return res.status(400).json({ error: 'id obrigatorio' });
    await pool.query('DELETE FROM caixa_bancos WHERE id=$1 AND empresa=$2', [parseInt(id), company]);
    return res.json({ ok: true });
  }

  res.status(405).json({ error: 'Method not allowed' });
}

async function handlePluggy(req, res, pool, company) {
  if (!process.env.PLUGGY_CLIENT_ID || !process.env.PLUGGY_CLIENT_SECRET)
    return res.status(503).json({ error: 'Pluggy nao configurado. Adicione PLUGGY_CLIENT_ID e PLUGGY_CLIENT_SECRET nas variaveis de ambiente.' });

  if (req.method === 'GET') {
    const r = await pool.query(
      'SELECT id, link_id, institution, account_type, ultimo_sync, ativo, criado_em FROM belvo_links WHERE empresa=$1 ORDER BY criado_em DESC',
      [company]
    );
    return res.json({ links: r.rows });
  }

  if (req.method === 'POST') {
    const { action } = req.body;

    if (action === 'widget_token') {
      const apiKey = await pluggyGetApiKey();
      const r = await pluggyPost(apiKey, '/connect_token');
      if (!r.ok) return res.status(r.status).json({ error: `Pluggy: ${await r.text()}` });
      const data = await r.json();
      return res.json({ access: data.accessToken });
    }

    if (action === 'register_link') {
      const { link_id, institution, account_type } = req.body;
      if (!link_id) return res.status(400).json({ error: 'link_id obrigatorio' });
      const r = await pool.query(
        `INSERT INTO belvo_links (empresa, link_id, institution, account_type)
         VALUES ($1,$2,$3,$4)
         ON CONFLICT (empresa, link_id) DO UPDATE
           SET institution=EXCLUDED.institution, account_type=EXCLUDED.account_type, ativo=true
         RETURNING id, link_id, institution, account_type, ultimo_sync, ativo, criado_em`,
        [company, link_id, institution || null, account_type || null]
      );
      return res.json({ ok: true, link: r.rows[0] });
    }

    if (action === 'sync') {
      const { link_id, date_from, date_to } = req.body;
      if (!link_id || !date_from || !date_to)
        return res.status(400).json({ error: 'link_id, date_from e date_to sao obrigatorios' });

      const transactions = await fetchAllExtratosTransactions(link_id, date_from, date_to);
      await pool.query('UPDATE belvo_links SET ultimo_sync=NOW() WHERE empresa=$1 AND link_id=$2', [company, link_id]);
      
      if (!transactions.length)
        return res.json({ ok: true, count: 0, message: 'Nenhuma transacao encontrada no periodo' });

      const client = await pool.connect();
      try {
        await client.query('BEGIN');
        
        // Find banco_id
        let bancoId = null;
        const linkRes = await client.query('SELECT institution FROM belvo_links WHERE empresa=$1 AND link_id=$2', [company, link_id]);
        if (linkRes.rows.length) {
          const inst = linkRes.rows[0].institution;
          if (inst) {
            const bancoRes = await client.query('SELECT id FROM caixa_bancos WHERE empresa=$1 AND nome=$2', [company, inst]);
            if (bancoRes.rows.length) {
              bancoId = bancoRes.rows[0].id;
            } else {
              const newBanco = await client.query(
                'INSERT INTO caixa_bancos (empresa, nome) VALUES ($1, $2) ON CONFLICT (empresa, nome) DO UPDATE SET nome=EXCLUDED.nome RETURNING id',
                [company, inst.substring(0, 100)]
              );
              bancoId = newBanco.rows[0].id;
            }
          }
        }

        let imported = 0;
        for (const tx of transactions) {
          let rawDateStr = tx.date;
          if (tx.date instanceof Date) {
              rawDateStr = tx.date.toISOString();
          }
          if (!rawDateStr) continue;
          const d = new Date(rawDateStr);
          const ano = d.getUTCFullYear(), mes = d.getUTCMonth() + 1, dia = d.getUTCDate();
          const descricao = String(tx.description || tx.descriptionRaw || '').substring(0, 500);
          const sinal = (tx.type === 'DEBIT') ? -1 : 1;
          const valor = Math.round((parseFloat(tx.amount) || 0) * 100) * sinal;
          
          await client.query(
            `INSERT INTO caixa_extrato (empresa, ano, mes, dia, descricao, valor, belvo_tx_id, banco_id)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
             ON CONFLICT (empresa, belvo_tx_id) DO UPDATE
               SET ano=EXCLUDED.ano, mes=EXCLUDED.mes, dia=EXCLUDED.dia,
                   descricao=EXCLUDED.descricao, valor=EXCLUDED.valor, banco_id=EXCLUDED.banco_id, atualizado_em=CURRENT_TIMESTAMP`,
            [company, ano, mes, dia, descricao, valor, String(tx.id), bancoId]
          );
          imported++;
        }
        await client.query('COMMIT');
        return res.json({ ok: true, count: imported });
      } catch (e) { await client.query('ROLLBACK'); throw e; }
      finally { client.release(); }
    }

    return res.status(400).json({ error: 'action invalida' });
  }

  if (req.method === 'DELETE') {
    const { link_id } = req.query;
    if (!link_id) return res.status(400).json({ error: 'Informe link_id' });
    try {
      const apiKey = await pluggyGetApiKey();
      await pluggyDelete(apiKey, `/items/${link_id}`);
    } catch(e) { console.error('[PLUGGY DELETE]', e.message); }
    await pool.query('UPDATE belvo_links SET ativo=false WHERE empresa=$1 AND link_id=$2', [company, link_id]);
    return res.json({ ok: true });
  }

  res.status(405).json({ error: 'Method not allowed' });
}

async function handleSubempresas(req, res, pool, company) {
  if (req.method === 'GET') {
    const [subR, bancosR, canaisR, cpFornR] = await Promise.all([
      pool.query('SELECT id, nome, cnpj, cor FROM subempresas WHERE empresa=$1 ORDER BY nome', [company]),
      pool.query('SELECT id, nome, cor, icone, subempresa_id FROM caixa_bancos WHERE empresa=$1 ORDER BY nome', [company]),
      pool.query('SELECT canal, grupo, subempresa_id FROM vendas_grupos_canais WHERE empresa=$1 ORDER BY grupo, canal', [company]),
      pool.query(`SELECT empresa AS valor, COUNT(*)::int AS count, ROUND(SUM(COALESCE(saldo,0))::numeric,2) AS total_saldo FROM contas_pagar WHERE empresa IS NOT NULL AND empresa <> '' GROUP BY empresa ORDER BY empresa`).catch(() => ({ rows: [] })),
    ]);
    return res.json({ subempresas: subR.rows, bancos: bancosR.rows, canais: canaisR.rows, cp_empresa_valores: cpFornR.rows });
  }
  if (req.method === 'POST') {
    const { nome, cnpj, cor } = req.body;
    if (!nome) return res.status(400).json({ error: 'nome obrigatorio' });
    const r = await pool.query(
      `INSERT INTO subempresas (empresa, nome, cnpj, cor) VALUES ($1,$2,$3,$4)
       ON CONFLICT (empresa, nome) DO UPDATE SET cnpj=EXCLUDED.cnpj, cor=EXCLUDED.cor
       RETURNING id, nome, cnpj, cor`,
      [company, nome.substring(0, 100), cnpj || null, cor || '#007cdc']
    );
    return res.json({ ok: true, subempresa: r.rows[0] });
  }
  if (req.method === 'PATCH') {
    const { tipo, item_id, subempresa_id, grupo } = req.body;
    const sid = subempresa_id ? parseInt(subempresa_id) : null;
    if (tipo === 'banco') {
      if (!item_id) return res.status(400).json({ error: 'item_id obrigatorio' });
      await pool.query('UPDATE caixa_bancos SET subempresa_id=$1 WHERE id=$2 AND empresa=$3', [sid, parseInt(item_id), company]);
      return res.json({ ok: true });
    }
    if (tipo === 'canal') {
      const { canal } = req.body;
      if (!canal) return res.status(400).json({ error: 'canal obrigatorio' });
      await pool.query('UPDATE vendas_grupos_canais SET subempresa_id=$1 WHERE empresa=$2 AND canal=$3', [sid, company, canal]);
      return res.json({ ok: true });
    }
    if (tipo === 'contas_pagar_empresa') {
      const { old_valor, subempresa_nome, checked } = req.body;
      if (checked) {
        if (old_valor) {
          await pool.query('UPDATE contas_pagar SET empresa=$1 WHERE empresa=$2', [subempresa_nome, old_valor]);
        }
      } else {
        await pool.query("UPDATE contas_pagar SET empresa=NULL WHERE empresa=$1", [subempresa_nome]);
      }
      return res.json({ ok: true });
    }
    return res.status(400).json({ error: 'tipo deve ser banco, canal ou contas_pagar_empresa' });
  }
  if (req.method === 'DELETE') {
    const { id } = req.query;
    if (!id) return res.status(400).json({ error: 'id obrigatorio' });
    await pool.query('DELETE FROM subempresas WHERE id=$1 AND empresa=$2', [parseInt(id), company]);
    return res.json({ ok: true });
  }
  res.status(405).json({ error: 'Method not allowed' });
}

module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, DELETE, PATCH, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  if (req.method === 'OPTIONS') return res.status(200).end();

  const payload = verifyToken(req, res);
  if (!payload) return;
  const { company, pool } = getCompanyPool(payload);

  // Roteamento
  try {
    if (req.query.module === 'bancos') return await handleBancos(req, res, pool, company);
    if (req.query.module === 'subempresas') return await handleSubempresas(req, res, pool, company);
    if (req.query.module === 'pluggy' || req.query.module === 'belvo') return await handlePluggy(req, res, pool, company);
  } catch (e) {
    console.error('[CAIXA-EXTRATO module]', req.query.module, e.message);
    return res.status(500).json({ error: e.message });
  }

  try {
    if (req.method === 'GET') {
      const { ano, mes, banco_id } = req.query;
      if (ano && mes) {
        const bancoId = banco_id ? parseInt(banco_id) : null;
        const [r, dpR] = await Promise.all([
          pool.query(
            `SELECT e.id::text AS id, e.dia, e.descricao, e.razao_social, e.account_number,
                    e.counterparty_document, e.valor,
                    e.belvo_tx_id, e.banco_id, b.nome AS banco_nome,
                    e.atualizado_em
             FROM caixa_extrato e
             LEFT JOIN caixa_bancos b ON b.id = e.banco_id
             WHERE e.empresa=$1 AND e.ano=$2 AND e.mes=$3 AND e.belvo_tx_id IS NULL
               ${bancoId ? `AND e.banco_id = ${bancoId}` : ''}

             UNION ALL

             SELECT eof.id::text AS id, 
                    EXTRACT(DAY FROM eof.data_lancamento)::int AS dia, 
                    eof.descricao, 
                    eof.razao_social, 
                    eof.agencia_numero AS account_number,
                    eof.cnpj_cpf AS counterparty_document, 
                    ROUND(eof.valor * 100)::int AS valor,
                    eof.id::text AS belvo_tx_id, 
                    NULL::int AS banco_id, 
                    eof.banco AS banco_nome,
                    eof.data_lancamento AS atualizado_em
             FROM extrato_openfinance eof
             LEFT JOIN caixa_bancos b ON LOWER(b.nome) = LOWER(eof.banco) AND b.empresa = $1
             WHERE LOWER(eof.cliente)=LOWER($1) AND EXTRACT(YEAR FROM eof.data_lancamento)=$2 AND EXTRACT(MONTH FROM eof.data_lancamento)=$3
               ${bancoId ? `AND b.id = ${bancoId}` : ''}
             ORDER BY dia, id`,
            [company, parseInt(ano), parseInt(mes)]
          ),
          pool.query(
            "SELECT palavra_chave, razao_social, cnpj, categoria_nome FROM caixa_de_para WHERE empresa=$1 AND tipo='extrato' ORDER BY categoria_nome",
            [company]
          ),
        ]);
        const rules = dpR.rows;
        const rows = r.rows.map(row => {
          const effectiveRS = row.razao_social || (() => {
            const parts = (row.descricao || '').split('\u00b7');
            return parts.length > 1 ? parts.slice(1).join('\u00b7').trim() : '';
          })();
          const rsLower   = effectiveRS.toLowerCase();
          const descLower = (row.descricao || '').toLowerCase();
          const cnpjDoc   = (row.counterparty_document || '').replace(/\D/g, '');
          let categoria = null;
          for (const dp of rules) {
            const dpCNPJ = (dp.cnpj || '').replace(/\D/g, '');
            const dpRS   = (dp.razao_social || '').toLowerCase();
            const dpPK   = (dp.palavra_chave || '').toLowerCase();
            let matched = false;
            if (dpCNPJ) {
              matched = cnpjDoc && cnpjDoc === dpCNPJ;
            } else if (dpRS && dpPK) {
              matched = rsLower && rsLower.includes(dpRS) && descLower.includes(dpPK);
            } else if (dpRS) {
              matched = rsLower && rsLower.includes(dpRS);
            } else if (dpPK) {
              matched = descLower.includes(dpPK);
            }
            if (matched) { categoria = dp.categoria_nome; break; }
          }
          return { ...row, categoria };
        });
        return res.json({ rows });
      }
      // List months with data
      const r = await pool.query(
        `WITH meses_unificados AS (
           SELECT ano, mes, atualizado_em FROM caixa_extrato WHERE empresa=$1 AND belvo_tx_id IS NULL
           UNION ALL
           SELECT EXTRACT(YEAR FROM data_lancamento)::int AS ano, 
                  EXTRACT(MONTH FROM data_lancamento)::int AS mes,
                  data_lancamento AS atualizado_em
           FROM extrato_openfinance WHERE LOWER(cliente)=LOWER($1)
         )
         SELECT ano, mes, COUNT(*)::int as total_registros, MAX(atualizado_em) as ultima_atualizacao
         FROM meses_unificados GROUP BY ano, mes ORDER BY ano DESC, mes DESC`,
        [company]
      );
      return res.json({ meses: r.rows });
    }

    if (req.method === 'POST') {
      const { ano, mes, rows, banco_id, modo } = req.body;
      // modo: 'substituir' (default) or 'adicionar' (merge, skip exact duplicates)
      if (!ano || !mes || !Array.isArray(rows) || !rows.length)
        return res.status(400).json({ error: 'Informe ano, mes e rows' });

      const bancoId = banco_id ? parseInt(banco_id) : null;
      const client = await pool.connect();
      try {
        await client.query('BEGIN');
        let inserted = 0;

        if (modo === 'adicionar') {
          // Merge mode: skip rows where (banco_id, dia, descricao, valor) already exist
          const CHUNK = 50;
          for (let i = 0; i < rows.length; i += CHUNK) {
            const chunk = rows.slice(i, i + CHUNK);
            for (const r of chunk) {
              const exists = await client.query(
                `SELECT 1 FROM caixa_extrato
                  WHERE empresa=$1 AND ano=$2 AND mes=$3 AND banco_id IS NOT DISTINCT FROM $4
                    AND dia=$5 AND descricao=$6 AND valor=$7 LIMIT 1`,
                [company, parseInt(ano), parseInt(mes), bancoId,
                 parseInt(r.dia), String(r.descricao || '').substring(0, 500), parseInt(r.valor) || 0]
              );
              if (!exists.rows.length) {
                await client.query(
                  'INSERT INTO caixa_extrato (empresa, ano, mes, dia, descricao, razao_social, valor, banco_id) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)',
                  [company, parseInt(ano), parseInt(mes), parseInt(r.dia),
                   String(r.descricao || '').substring(0, 500),
                   r.razao_social ? String(r.razao_social).substring(0, 300) : null,
                   parseInt(r.valor) || 0, bancoId]
                );
                inserted++;
              }
            }
          }
        } else {
          // Substituir mode (default): replace all rows for this bank+month
          await client.query(
            'DELETE FROM caixa_extrato WHERE empresa=$1 AND ano=$2 AND mes=$3 AND banco_id IS NOT DISTINCT FROM $4',
            [company, parseInt(ano), parseInt(mes), bancoId]
          );
          const CHUNK = 200;
          for (let i = 0; i < rows.length; i += CHUNK) {
            const chunk = rows.slice(i, i + CHUNK);
            const vals = [], params = [];
            chunk.forEach((r, idx) => {
              const b = idx * 8;
              vals.push(`($${b+1},$${b+2},$${b+3},$${b+4},$${b+5},$${b+6},$${b+7},$${b+8})`);
              params.push(company, parseInt(ano), parseInt(mes), parseInt(r.dia),
                String(r.descricao || '').substring(0, 500),
                r.razao_social ? String(r.razao_social).substring(0, 300) : null,
                parseInt(r.valor) || 0, bancoId);
            });
            await client.query(
              `INSERT INTO caixa_extrato (empresa, ano, mes, dia, descricao, razao_social, valor, banco_id) VALUES ${vals.join(',')}`,
              params
            );
            inserted += chunk.length;
          }
        }

        await client.query('COMMIT');
        return res.json({ ok: true, count: inserted, modo: modo || 'substituir' });
      } catch (e) {
        await client.query('ROLLBACK'); throw e;
      } finally { client.release(); }
    }

    if (req.method === 'DELETE') {
      const { ano, mes } = req.query;
      if (!ano || !mes) return res.status(400).json({ error: 'Informe ano e mes' });
      const r = await pool.query(
        'DELETE FROM caixa_extrato WHERE empresa=$1 AND ano=$2 AND mes=$3',
        [company, parseInt(ano), parseInt(mes)]
      );
      return res.json({ ok: true, deleted: r.rowCount });
    }

    res.status(405).json({ error: 'Method not allowed' });
  } catch (e) {
    console.error('[CAIXA-EXTRATO]', e.message);
    res.status(500).json({ error: e.message });
  }
};
