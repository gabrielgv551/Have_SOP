const { getPool, getCompanyPool } = require('./db');
const { isTinyTable, TABELAS_PERMITIDAS, CANAL_GRUPO_SQL, getTableColumns, lerEstoqueFullMap } = require('./data-helpers');

module.exports = async function handleSopc(req, res, payload) {

  // Módulo S&OP Config
  if (req.query.module === 'sopc-config') {
    const { company, pool } = getCompanyPool(payload);
    try {
      if (req.method === 'GET') {
        const r = await pool.query(
          'SELECT modulo, chave, valor FROM sopc_config WHERE empresa=$1 ORDER BY modulo, chave',
          [company]
        );
        return res.json({ config: r.rows });
      }
      if (req.method === 'POST') {
        const { modulo, chave, valor } = req.body || {};
        if (!modulo || !chave || valor === undefined) {
          return res.status(400).json({ error: 'modulo, chave e valor são obrigatórios' });
        }
        await pool.query(`
          INSERT INTO sopc_config (empresa, modulo, chave, valor)
          VALUES ($1,$2,$3,$4)
          ON CONFLICT (empresa, modulo, chave) DO UPDATE SET valor = EXCLUDED.valor
        `, [company, modulo, chave, String(valor)]);
        return res.json({ ok: true });
      }
    } catch(e) {
      console.error('[SOPC-CONFIG]', e.message);
      return res.status(500).json({ error: e.message });
    }
  }

  // Módulo Fornecedores Config (lead time por Marca)
  if (req.query.module === 'fornecedores-config') {
    const { company, pool } = getCompanyPool(payload);
    try {
      if (req.method === 'GET') {
        const r = await pool.query(`
          SELECT m.marca,
                 f.lead_time_dias,
                 COALESCE(f.frequencia_tipo, 'mensal') AS frequencia_tipo,
                 COALESCE(f.dia_semana_preferido, 5) AS dia_semana_preferido,
                 COALESCE(f.intervalo_dias, 30) AS intervalo_dias
          FROM (
            SELECT DISTINCT "Marca" AS marca FROM cadastros_sku
            WHERE "Marca" IS NOT NULL AND TRIM("Marca") <> ''
          ) m
          LEFT JOIN fornecedores_config f ON f.marca = m.marca AND f.empresa = $1
          ORDER BY m.marca
        `, [company]);
        return res.json({ marcas: r.rows });
      }
      if (req.method === 'POST') {
        const { marca, lead_time_dias, frequencia_tipo, dia_semana_preferido, intervalo_dias } = req.body || {};
        if (!marca || lead_time_dias == null) {
          return res.status(400).json({ error: 'marca e lead_time_dias são obrigatórios' });
        }
        const dias = parseInt(lead_time_dias);
        if (isNaN(dias) || dias < 1) {
          return res.status(400).json({ error: 'lead_time_dias deve ser inteiro >= 1' });
        }
        const freqTipo = ['semanal','quinzenal','mensal','custom'].includes(frequencia_tipo) ? frequencia_tipo : 'mensal';
        const diaSem = Math.max(0, Math.min(6, parseInt(dia_semana_preferido) || 5));
        const intervalo = Math.max(1, parseInt(intervalo_dias) || 30);
        await pool.query(`
          INSERT INTO fornecedores_config (empresa, marca, lead_time_dias, frequencia_tipo, dia_semana_preferido, intervalo_dias)
          VALUES ($1, $2, $3, $4, $5, $6)
          ON CONFLICT (empresa, marca) DO UPDATE SET
            lead_time_dias = EXCLUDED.lead_time_dias,
            frequencia_tipo = EXCLUDED.frequencia_tipo,
            dia_semana_preferido = EXCLUDED.dia_semana_preferido,
            intervalo_dias = EXCLUDED.intervalo_dias
        `, [company, marca, dias, freqTipo, diaSem, intervalo]);
        return res.json({ ok: true });
      }
    } catch(e) {
      console.error('[FORNECEDORES-CONFIG]', e.message);
      return res.status(500).json({ error: e.message });
    }
  }

  // Módulo SKU Desativadas
  if (req.query.module === 'sku-desativadas') {
    const { company, pool } = getCompanyPool(payload);
    try {
      if (req.method === 'GET') {
        const r = await pool.query(
          'SELECT sku FROM sku_desativadas WHERE empresa=$1 ORDER BY sku',
          [company]
        );
        return res.json({ skus: r.rows.map(r => r.sku) });
      }
      if (req.method === 'POST') {
        const { action, sku, skus } = req.body || {};
        if (action === 'toggle') {
          if (!sku) return res.status(400).json({ error: 'sku obrigatorio' });
          const exists = await pool.query(
            'SELECT 1 FROM sku_desativadas WHERE empresa=$1 AND sku=$2', [company, sku]
          );
          if (exists.rowCount > 0) {
            await pool.query('DELETE FROM sku_desativadas WHERE empresa=$1 AND sku=$2', [company, sku]);
            return res.json({ ok: true, active: true });
          } else {
            await pool.query(
              'INSERT INTO sku_desativadas (empresa, sku) VALUES ($1,$2) ON CONFLICT DO NOTHING',
              [company, sku]
            );
            return res.json({ ok: true, active: false });
          }
        }
        if (action === 'deactivate_many') {
          if (!Array.isArray(skus) || !skus.length) return res.status(400).json({ error: 'skus obrigatorio' });
          for (const s of skus) {
            await pool.query(
              'INSERT INTO sku_desativadas (empresa, sku) VALUES ($1,$2) ON CONFLICT DO NOTHING',
              [company, String(s)]
            );
          }
          return res.json({ ok: true, count: skus.length });
        }
        if (action === 'activate_all') {
          await pool.query('DELETE FROM sku_desativadas WHERE empresa=$1', [company]);
          return res.json({ ok: true });
        }
        if (action === 'activate_many') {
          if (!Array.isArray(skus) || !skus.length) return res.status(400).json({ error: 'skus obrigatorio' });
          for (const s of skus) {
            await pool.query('DELETE FROM sku_desativadas WHERE empresa=$1 AND sku=$2', [company, String(s)]);
          }
          return res.json({ ok: true });
        }
        return res.status(400).json({ error: 'action invalida' });
      }
      return res.status(405).json({ error: 'Method not allowed' });
    } catch(e) {
      console.error('[SKU-DESATIVADAS]', e.message);
      return res.status(500).json({ error: e.message });
    }
  }

  // ── Generic table queries ──────────────────────────────────────────────
  const { tabela } = req.query;
  if (!tabela || (!TABELAS_PERMITIDAS.includes(tabela) && !isTinyTable(tabela)))
    return res.status(400).json({ error: `Tabela '${tabela}' não permitida.` });

  const pool = getPool(payload.company);

  try {
    let result;
    if (tabela === 'sopc') {
      const [ppRes, esRes, canalRes, full1Map, full2Map] = await Promise.all([
        pool.query(`SELECT sku, COALESCE(estoque_atual::numeric,0) AS estoque_atual, COALESCE(ponto_pedido::numeric,0) AS ponto_pedido, COALESCE(alerta,'SEM DADOS') AS alerta FROM ponto_pedido`),
        pool.query(`SELECT sku, REPLACE(media_mensal::text,',','.')::numeric AS media_mensal FROM estoque_seguranca`),
        pool.query(`SELECT "Sku" AS sku, TRIM("Canal de venda") AS canal, ROUND(SUM("Quantidade Vendida"::numeric)/3.0,1) AS media FROM bd_vendas WHERE "Status" !~* '(cancel|devol|n[aã]o.?pago)' AND "Data"::date >= (SELECT MAX("Data"::date) FROM bd_vendas) - INTERVAL '3 months' AND "Sku" IS NOT NULL AND TRIM("Canal de venda") IS NOT NULL AND TRIM("Canal de venda") != '' GROUP BY "Sku", TRIM("Canal de venda")`).catch(()=>({rows:[]})),
        lerEstoqueFullMap(pool,'full_1').catch(()=>({})),
        lerEstoqueFullMap(pool,'full_2').catch(()=>({})),
      ]);
      const mediaMap={};
      esRes.rows.forEach(r=>{mediaMap[String(r.sku||'').trim()]=parseFloat(r.media_mensal)||0;});
      const canalMap={};
      const canaisSet=new Set();
      canalRes.rows.forEach(r=>{
        const s=String(r.sku||'').trim(); const c=String(r.canal||'').trim();
        if(!s||!c) return;
        if(!canalMap[s]) canalMap[s]={};
        canalMap[s][c]=(canalMap[s][c]||0)+(parseFloat(r.media)||0);
        canaisSet.add(c);
      });
      const fullMap={};
      const allFull=new Set([...Object.keys(full1Map),...Object.keys(full2Map)]);
      allFull.forEach(s=>{fullMap[s]=(full1Map[s]||0)+(full2Map[s]||0);});

      const origenMap={};
      const origensSet=new Set();
      try {
        const ecCols = await getTableColumns(pool,'estoque_consolidado');
        const skuCol  = ecCols.find(c=>c==='SKU')||ecCols.find(c=>c.toLowerCase()==='sku')||'SKU';
        const oriCol  = ecCols.find(c=>c.toLowerCase()==='origem')||'Origem';
        const qtdCol  = ecCols.find(c=>c.toLowerCase().includes('estoque'))||'Estoque Base';
        const origenRes = await pool.query(
          `SELECT "${skuCol}" AS sku, TRIM("${oriCol}") AS origem, SUM("${qtdCol}"::numeric) AS qtd FROM estoque_consolidado WHERE "${skuCol}" IS NOT NULL AND TRIM("${skuCol}"::text)!='' AND "${oriCol}" IS NOT NULL AND TRIM("${oriCol}"::text)!='' GROUP BY "${skuCol}", TRIM("${oriCol}")`
        );
        origenRes.rows.forEach(r=>{
          const s=String(r.sku||'').trim(); const o=String(r.origem||'').trim();
          if(!s||!o) return;
          if(!origenMap[s]) origenMap[s]={};
          origenMap[s][o]=(origenMap[s][o]||0)+(parseFloat(r.qtd)||0);
          origensSet.add(o);
        });
      } catch(e){ console.error('[SOPC] origens:',e.message); }

      const rows=ppRes.rows.map(r=>{
        const sku=String(r.sku||'').trim();
        return {sku, alerta_pp:r.alerta, estoque_base:parseFloat(r.estoque_atual)||0, estoque_full:fullMap[sku]||0, media_mensal:mediaMap[sku]||0, ponto_pedido:parseFloat(r.ponto_pedido)||0, canais:canalMap[sku]||{}, origens:origenMap[sku]||{}};
      });
      return res.json({rows, canais_disponiveis:[...canaisSet].sort(), origens_disponiveis:[...origensSet].sort()});
    }
    if (tabela === 'sku_atividade') {
      result = await pool.query(`
        WITH max_d AS (SELECT MAX("Data"::date) AS d FROM bd_vendas)
        SELECT
          "Sku" AS sku,
          ROUND(SUM(CASE WHEN "Data"::date >= (SELECT d FROM max_d) - INTERVAL '1 month'  THEN "Quantidade Vendida"::numeric ELSE 0 END),0) AS qtd_1m,
          ROUND(SUM(CASE WHEN "Data"::date >= (SELECT d FROM max_d) - INTERVAL '3 months' THEN "Quantidade Vendida"::numeric ELSE 0 END),0) AS qtd_3m,
          ROUND(SUM(CASE WHEN "Data"::date >= (SELECT d FROM max_d) - INTERVAL '6 months' THEN "Quantidade Vendida"::numeric ELSE 0 END),0) AS qtd_6m,
          ROUND(SUM("Quantidade Vendida"::numeric),0) AS qtd_12m
        FROM bd_vendas
        WHERE "Status" !~* '(cancel|devol|n[aã]o.?pago)'
          AND "Sku" IS NOT NULL AND TRIM("Sku"::text) != ''
          AND "Data"::date >= (SELECT d FROM max_d) - INTERVAL '12 months'
        GROUP BY "Sku"
        ORDER BY qtd_12m ASC
      `);
      return res.json(result.rows);
    }
    if (tabela === 'sku_discontinued') {
      result = await pool.query(`
        SELECT DISTINCT "Sku" AS sku
        FROM cadastros_sku
        WHERE "Sku" NOT IN (
          SELECT DISTINCT "Sku"
          FROM bd_vendas
          WHERE "Data" >= CURRENT_DATE - INTERVAL '6 months'
        )
      `);
      return res.json(result.rows.map(r => r.sku));
    }
    if (tabela === 'dashboard_filters') {
      const [canalRes, marcaRes, mesesRes] = await Promise.all([
        pool.query(`
          SELECT DISTINCT (${CANAL_GRUPO_SQL}) AS canal
          FROM bd_vendas
          WHERE "Canal de venda" IS NOT NULL AND TRIM("Canal de venda") != ''
          ORDER BY 1
        `),
        pool.query(`
          SELECT DISTINCT TRIM("Marca") AS marca FROM cadastros_sku
          WHERE "Marca" IS NOT NULL AND TRIM("Marca") != ''
          ORDER BY 1
        `),
        pool.query(`
          SELECT DISTINCT "Ano" AS ano, "Mes" AS mes FROM bd_vendas
          WHERE "Ano" IS NOT NULL AND "Mes" IS NOT NULL
          ORDER BY "Ano" DESC, "Mes" DESC
        `)
      ]);
      return res.json({
        canais: canalRes.rows.map(r => r.canal).filter(Boolean),
        marcas: marcaRes.rows.map(r => r.marca).filter(Boolean),
        meses:  mesesRes.rows
      });
    }
    if (tabela === 'dashboard_kpis') {
      const { mes: mesFiltro, ano: anoFiltro, marca: marcaFiltro, canal: canalFiltro } = req.query;
      const params = [];
      // Usa "Data" válida quando disponível; senão, gera uma data a partir de Ano/Mes somente se ambos forem numéricos
      const safeAno = `CASE WHEN TRIM("Ano"::text) ~ '^\\d+$' THEN "Ano"::int ELSE NULL END`;
      const safeMes = `CASE WHEN TRIM("Mes"::text) ~ '^\\d+$' THEN "Mes"::int ELSE NULL END`;
      const dataExpr = `COALESCE(
          CASE WHEN TRIM("Data"::text) ~ '^\\d{4}-\\d{2}-\\d{2}$' THEN "Data"::date ELSE NULL END,
          CASE WHEN ${safeAno} IS NOT NULL AND ${safeMes} IS NOT NULL THEN MAKE_DATE(${safeAno}, ${safeMes}, 1) ELSE NULL END
        )`;
      const conditions = [`${dataExpr} IS NOT NULL`];

      if (canalFiltro) {
        params.push(canalFiltro);
        conditions.push(`(${CANAL_GRUPO_SQL}) = $${params.length}`);
      }
      if (marcaFiltro) {
        params.push(marcaFiltro);
        conditions.push(`"Sku" IN (SELECT "Sku" FROM cadastros_sku WHERE TRIM("Marca") = $${params.length})`);
      }

      let whereClause = `WHERE ${conditions.join(' AND ')}`;
      let targetAno = anoFiltro ? parseInt(anoFiltro, 10) : null;
      let targetMes = mesFiltro ? parseInt(mesFiltro, 10) : null;

      // Sem filtro de ano/mes: descobrir o período mais recente com MAX (muito mais rápido que GROUP BY em toda a tabela)
      if (!targetAno || !targetMes) {
        const periodRes = await pool.query(`
          SELECT
            EXTRACT(YEAR FROM MAX(${dataExpr}))::int AS ano,
            EXTRACT(MONTH FROM MAX(${dataExpr}))::int AS mes
          FROM bd_vendas
          ${whereClause}
        `, params);
        targetAno = periodRes.rows[0]?.ano || null;
        targetMes = periodRes.rows[0]?.mes || null;
      } else {
        params.push(targetAno);
        params.push(targetMes);
        whereClause += ` AND EXTRACT(YEAR FROM ${dataExpr})::int = $${params.length - 1} AND EXTRACT(MONTH FROM ${dataExpr})::int = $${params.length}`;
      }

      if (!targetAno || !targetMes) {
        return res.json({});
      }

      // Se descobrimos o período via MAX, precisamos adicionar os filtros de ano/mes aos params
      const kpiParams = [...params];
      if (!anoFiltro || !mesFiltro) {
        kpiParams.push(targetAno);
        kpiParams.push(targetMes);
      }

      const anoPlaceholder = `$${kpiParams.length - 1}`;
      const mesPlaceholder = `$${kpiParams.length}`;

      result = await pool.query(`
        SELECT
          ${targetAno} AS ano,
          ${targetMes} AS mes,
          SUM(COALESCE("Total Venda Pedido", "Total Venda")) AS receita_bruta,
          SUM(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN COALESCE("Total Venda", 0) ELSE 0 END) AS receita_liquida,
          SUM(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN COALESCE("Quantidade Vendida", 0) ELSE 0 END) AS qtd_liquida,
          SUM(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN COALESCE("Margem Produto", 0) ELSE 0 END) AS margem_bruta,
          SUM(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN COALESCE("Custo Total", 0) ELSE 0 END) AS custo_total
        FROM bd_vendas
        ${whereClause}
          AND EXTRACT(YEAR FROM ${dataExpr})::int = ${anoPlaceholder}
          AND EXTRACT(MONTH FROM ${dataExpr})::int = ${mesPlaceholder}
        GROUP BY EXTRACT(YEAR FROM ${dataExpr}), EXTRACT(MONTH FROM ${dataExpr})
      `, kpiParams);
      return res.json(result.rows[0] || {});
    }
    if (tabela === 'contas_pagar') {
      result = await pool.query(`
        SELECT id, situacao, token_origem, numero_doc, historico, fornecedor,
               valor, saldo, data_vencimento, data_emissao, atualizado_em, data_calculo,
               empresa
        FROM contas_pagar
        ORDER BY data_vencimento ASC NULLS LAST, id ASC
      `);
      return res.json(result.rows);
    }
    if (tabela === 'monthly_revenue') {
      const { marca: marcaFiltro, canal: canalFiltro } = req.query;
      const params = [];
      const filterClauses = [];
      if (canalFiltro) {
        params.push(canalFiltro);
        filterClauses.push(`(${CANAL_GRUPO_SQL}) = $${params.length}`);
      }
      if (marcaFiltro) {
        params.push(marcaFiltro);
        filterClauses.push(`"Sku" IN (SELECT "Sku" FROM cadastros_sku WHERE TRIM("Marca") = $${params.length})`);
      }
      const fWhere = filterClauses.length ? ' AND ' + filterClauses.join(' AND ') : '';
      result = await pool.query(`
        SELECT ano, mes,
               SUM(tvp)  AS receita,
               SUM(qtd)  AS qtd,
               CASE WHEN SUM(receita_liq) > 0
                    THEN ROUND((
                      SUM(margem_liq) / SUM(receita_liq) * 100
                    )::numeric, 2)
                    ELSE NULL END AS mc_pct
        FROM (
          SELECT "Ano" AS ano, "Mes" AS mes, "Order ID",
                 MAX("Total Venda Pedido") AS tvp,
                 SUM("Quantidade Vendida") AS qtd,
                 SUM(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN "Total Venda"                ELSE 0 END) AS receita_liq,
                 SUM(CASE WHEN "Status" !~* '(cancel|devol|n[aã]o.?pago)' THEN COALESCE("Margem Produto", 0) ELSE 0 END) AS margem_liq
          FROM bd_vendas
          WHERE "Ano" IS NOT NULL AND "Mes" IS NOT NULL${fWhere}
          GROUP BY "Ano", "Mes", "Order ID"
        ) t
        GROUP BY ano, mes
        ORDER BY ano ASC, mes ASC
      `, params);
      return res.json(result.rows);
    }
    if (tabela === 'pmv_months') {
      result = await pool.query(`
        SELECT DISTINCT "Ano" AS ano, "Mes" AS mes
        FROM bd_vendas
        WHERE "Ano" IS NOT NULL AND "Mes" IS NOT NULL
        ORDER BY "Ano" DESC, "Mes" DESC
      `);
      return res.json(result.rows);
    }
    if (tabela === 'pmv') {
      const { mes_prev, ano_prev, mes_curr, ano_curr, dia_ini_prev, dia_fim_prev, dia_ini_curr, dia_fim_curr, canal } = req.query;
      if (!mes_prev || !ano_prev || !mes_curr || !ano_curr)
        return res.status(400).json({ error: 'Parâmetros mes_prev, ano_prev, mes_curr, ano_curr são obrigatórios.' });
      const pad = n => String(n).padStart(2, '0');
      const lastDay = (y, m) => new Date(+y, +m, 0).getDate();
      const dIniPrev = dia_ini_prev ? +dia_ini_prev : 1;
      const dFimPrev = dia_fim_prev ? +dia_fim_prev : lastDay(ano_prev, mes_prev);
      const dIniCurr = dia_ini_curr ? +dia_ini_curr : 1;
      const dFimCurr = dia_fim_curr ? +dia_fim_curr : lastDay(ano_curr, mes_curr);
      const datePrevIni = `${ano_prev}-${pad(mes_prev)}-${pad(dIniPrev)}`;
      const datePrevFim = `${ano_prev}-${pad(mes_prev)}-${pad(dFimPrev)}`;
      const dateCurrIni = `${ano_curr}-${pad(mes_curr)}-${pad(dIniCurr)}`;
      const dateCurrFim = `${ano_curr}-${pad(mes_curr)}-${pad(dFimCurr)}`;
      const pmvParams = [datePrevIni, datePrevFim, dateCurrIni, dateCurrFim];
      let pmvCanalWhere = '';
      if (canal) {
        pmvParams.push(canal);
        pmvCanalWhere = ` AND (${CANAL_GRUPO_SQL}) = $${pmvParams.length}`;
      }
      result = await pool.query(`
        SELECT
          v."Sku" AS sku,
          MAX(v."Nome Produto") AS nome_produto,
          MAX(v."Categoria") AS categoria,
          COALESCE(NULLIF(TRIM(MAX(cs."Marca")), ''), '–') AS marca,
          COALESCE(MAX(pp.estoque_atual::numeric), 0) AS estoque_atual,
          SUM(CASE WHEN v."Data"::date BETWEEN $1::date AND $2::date THEN v."Quantidade Vendida" ELSE 0 END) AS qtd_prev,
          SUM(CASE WHEN v."Data"::date BETWEEN $1::date AND $2::date THEN v."Total Venda" ELSE 0 END) AS rev_prev,
          SUM(CASE WHEN v."Data"::date BETWEEN $1::date AND $2::date THEN COALESCE(v."Margem Produto",0) ELSE 0 END) AS mar_prev,
          SUM(CASE WHEN v."Data"::date BETWEEN $3::date AND $4::date THEN v."Quantidade Vendida" ELSE 0 END) AS qtd_curr,
          SUM(CASE WHEN v."Data"::date BETWEEN $3::date AND $4::date THEN v."Total Venda" ELSE 0 END) AS rev_curr,
          SUM(CASE WHEN v."Data"::date BETWEEN $3::date AND $4::date THEN COALESCE(v."Margem Produto",0) ELSE 0 END) AS mar_curr
        FROM bd_vendas v
        LEFT JOIN cadastros_sku cs ON TRIM(cs."Sku"::text) = TRIM(v."Sku"::text)
        LEFT JOIN ponto_pedido pp ON TRIM(pp.sku::text) = TRIM(v."Sku"::text)
        WHERE v."Status" !~* '(cancel|devol|n[aã]o.?pago)'
          AND (
            v."Data"::date BETWEEN $1::date AND $2::date OR
            v."Data"::date BETWEEN $3::date AND $4::date
          )${pmvCanalWhere}
        GROUP BY v."Sku"
        HAVING SUM(v."Quantidade Vendida") > 0
        ORDER BY SUM(CASE WHEN v."Data"::date BETWEEN $3::date AND $4::date THEN v."Total Venda" ELSE 0 END) DESC
      `, pmvParams);
      return res.json(result.rows);
    }
    if (tabela === 'pmv_canais') {
      const { mes_prev, ano_prev, mes_curr, ano_curr, dia_ini_prev, dia_fim_prev, dia_ini_curr, dia_fim_curr, sku, canal } = req.query;
      if (!mes_prev || !ano_prev || !mes_curr || !ano_curr || !sku)
        return res.status(400).json({ error: 'Parâmetros obrigatórios ausentes.' });
      const pad = n => String(n).padStart(2, '0');
      const lastDay = (y, m) => new Date(+y, +m, 0).getDate();
      const dIniPrev = dia_ini_prev ? +dia_ini_prev : 1;
      const dFimPrev = dia_fim_prev ? +dia_fim_prev : lastDay(ano_prev, mes_prev);
      const dIniCurr = dia_ini_curr ? +dia_ini_curr : 1;
      const dFimCurr = dia_fim_curr ? +dia_fim_curr : lastDay(ano_curr, mes_curr);
      const datePrevIni = `${ano_prev}-${pad(mes_prev)}-${pad(dIniPrev)}`;
      const datePrevFim = `${ano_prev}-${pad(mes_prev)}-${pad(dFimPrev)}`;
      const dateCurrIni = `${ano_curr}-${pad(mes_curr)}-${pad(dIniCurr)}`;
      const dateCurrFim = `${ano_curr}-${pad(mes_curr)}-${pad(dFimCurr)}`;
      const canaisParams = [datePrevIni, datePrevFim, dateCurrIni, dateCurrFim, sku];
      let canaisCanalWhere = '';
      if (canal) {
        canaisParams.push(canal);
        canaisCanalWhere = ` AND (${CANAL_GRUPO_SQL}) = $${canaisParams.length}`;
      }
      result = await pool.query(`
        SELECT
          COALESCE(NULLIF(TRIM("Canal Apelido"::text), ''), TRIM("Canal de venda"), 'Sem canal') AS canal,
          SUM(CASE WHEN "Data"::date BETWEEN $1::date AND $2::date THEN "Quantidade Vendida" ELSE 0 END) AS qtd_prev,
          SUM(CASE WHEN "Data"::date BETWEEN $1::date AND $2::date THEN "Total Venda" ELSE 0 END) AS rev_prev,
          SUM(CASE WHEN "Data"::date BETWEEN $1::date AND $2::date THEN COALESCE("Margem Produto",0) ELSE 0 END) AS mar_prev,
          SUM(CASE WHEN "Data"::date BETWEEN $3::date AND $4::date THEN "Quantidade Vendida" ELSE 0 END) AS qtd_curr,
          SUM(CASE WHEN "Data"::date BETWEEN $3::date AND $4::date THEN "Total Venda" ELSE 0 END) AS rev_curr,
          SUM(CASE WHEN "Data"::date BETWEEN $3::date AND $4::date THEN COALESCE("Margem Produto",0) ELSE 0 END) AS mar_curr
        FROM bd_vendas
        WHERE "Status" !~* '(cancel|devol|n[aã]o.?pago)'
          AND "Sku" = $5
          AND (
            "Data"::date BETWEEN $1::date AND $2::date OR
            "Data"::date BETWEEN $3::date AND $4::date
          )${canaisCanalWhere}
        GROUP BY COALESCE(NULLIF(TRIM("Canal Apelido"::text), ''), TRIM("Canal de venda"), 'Sem canal')
        HAVING SUM("Quantidade Vendida") > 0
        ORDER BY SUM(CASE WHEN "Data"::date BETWEEN $3::date AND $4::date THEN "Total Venda" ELSE 0 END) DESC
      `, canaisParams);
      return res.json(result.rows);
    }
    if (tabela === 'ponto_pedido') {
      const [ppRes, esRes, f1Map, f2Map, kitsRes] = await Promise.all([
        pool.query(`SELECT * FROM ponto_pedido LIMIT 5000`),
        pool.query(`SELECT sku, REPLACE(media_mensal::text,',','.')::numeric AS media_mensal FROM estoque_seguranca`).catch(() => ({ rows: [] })),
        lerEstoqueFullMap(pool, 'full_1').catch(() => ({})),
        lerEstoqueFullMap(pool, 'full_2').catch(() => ({})),
        pool.query(`SELECT UPPER(TRIM(sku_componente)) AS sku_componente, UPPER(TRIM(sku_kit)) AS sku_kit, quantidade::float FROM sku_kits WHERE ativo = true`).catch(() => ({ rows: [] })),
      ]);
      const mediaMap = {};
      esRes.rows.forEach(r => { mediaMap[String(r.sku || '').trim()] = r.media_mensal; });
      const fullMap = {};
      Object.keys(f1Map).forEach(s => { fullMap[s] = (fullMap[s] || 0) + f1Map[s]; });
      Object.keys(f2Map).forEach(s => { fullMap[s] = (fullMap[s] || 0) + f2Map[s]; });
      const kits_by_comp = {};
      kitsRes.rows.forEach(k => {
        const comp = String(k.sku_componente || '').trim();
        if (!comp) return;
        if (!kits_by_comp[comp]) kits_by_comp[comp] = [];
        kits_by_comp[comp].push({ sku_kit: String(k.sku_kit || '').trim(), quantidade: parseFloat(k.quantidade) || 1 });
      });
      const rows = ppRes.rows.map(r => {
        const sku = String(r.sku || '').trim();
        return {
          ...r,
          media_mensal: mediaMap[sku] ?? null,
          estoque_full: fullMap[sku] ?? (parseFloat(r.estoque_atual) || 0),
        };
      });
      return res.json({ rows, kits_by_comp });
    }
    if (tabela === 'curva_abc') {
      try {
        result = await pool.query(`
          SELECT * FROM ${tabela}
          WHERE ("Ano", "Mês") = (
            SELECT "Ano", "Mês" FROM ${tabela}
            ORDER BY "Ano" DESC, "Mês" DESC
            LIMIT 1
          )
          LIMIT 5000
        `);
        return res.json(result.rows);
      } catch(_) {
        // Tabelas sem coluna Ano/Mês – usa query genérica
      }
    }
    if (tabela === 'historico_vendas') {
      const hvRes = await pool.query(`
        SELECT
          TRIM("Sku") AS sku,
          EXTRACT(YEAR  FROM "Data"::date)::int AS ano,
          EXTRACT(MONTH FROM "Data"::date)::int AS mes,
          TO_CHAR("Data"::date, 'Mon/YY')        AS label,
          ROUND(SUM("Quantidade Vendida"::numeric), 0)::int AS qtd,
          ROUND(SUM("Total Venda"::numeric), 2)             AS receita
        FROM bd_vendas
        WHERE "Status" !~* '(cancel|devol|n[aã]o.?pago)'
          AND "Data"::date >= (SELECT MAX("Data"::date) FROM bd_vendas) - INTERVAL '12 months'
          AND "Sku" IS NOT NULL AND TRIM("Sku") != ''
        GROUP BY TRIM("Sku"), EXTRACT(YEAR FROM "Data"::date), EXTRACT(MONTH FROM "Data"::date), TO_CHAR("Data"::date, 'Mon/YY')
        ORDER BY TRIM("Sku"), ano, mes
        LIMIT 100000
      `);
      return res.json(hvRes.rows);
    }
    if (tabela === 'categoria_vendas') {
      const { ano, mes } = req.query;
      let whereClause = `WHERE "Categoria" IS NOT NULL AND TRIM("Categoria"::text) != '' AND "Status" !~* '(cancel|devol|n[aã]o.?pago)'`;
      const catParams = [];
      if (ano && mes) {
        whereClause += ` AND "Ano" = $1 AND "Mes" = $2`;
        catParams.push(parseInt(ano), parseInt(mes));
      } else {
        whereClause += ` AND ("Ano", "Mes") = (SELECT "Ano", "Mes" FROM bd_vendas WHERE "Ano" IS NOT NULL AND "Mes" IS NOT NULL ORDER BY "Ano" DESC, "Mes" DESC LIMIT 1)`;
      }
      result = await pool.query(`
        SELECT
          "Categoria"                                                              AS categoria,
          ROUND(SUM("Total Venda"), 2)                                             AS receita,
          ROUND(SUM(COALESCE("Margem Produto", 0)), 2)                             AS margem,
          ROUND(SUM(COALESCE("Custo Total", 0)), 2)                                AS custo,
          SUM("Quantidade Vendida")                                                AS qtd,
          COUNT(DISTINCT "Sku")                                                    AS skus
        FROM bd_vendas
        ${whereClause}
        GROUP BY "Categoria"
        ORDER BY receita DESC
      `, catParams);
      return res.json(result.rows);
    }
    if (tabela === 'ponto_pedido') {
      result = await pool.query(`
        SELECT pp.*,
               COALESCE(bv.nome_produto, '') AS nome_produto
        FROM ponto_pedido pp
        LEFT JOIN (
          SELECT TRIM("Sku"::text) AS sku,
                 MAX("Nome Produto") AS nome_produto
          FROM bd_vendas
          WHERE "Sku" IS NOT NULL
            AND TRIM("Sku"::text) != ''
            AND "Nome Produto" IS NOT NULL
            AND TRIM("Nome Produto"::text) != ''
          GROUP BY TRIM("Sku"::text)
        ) bv ON bv.sku = TRIM(pp.sku::text)
      `);
      return res.json(result.rows);
    }
    if (tabela === 'cadastros_sku') {
      try {
        const ecCols = await getTableColumns(pool, 'estoque_consolidado');
        const skuCol  = ecCols.find(c => c === 'SKU') || ecCols.find(c => c.toLowerCase() === 'sku') || null;
        const prodCol = ecCols.find(c => c === 'Produto') || ecCols.find(c => c.toLowerCase() === 'produto') || null;
        if (skuCol && prodCol) {
          result = await pool.query(`
            SELECT c.*,
                   ec_nome.produto_ec AS "Produto"
            FROM cadastros_sku c
            LEFT JOIN (
              SELECT TRIM("${skuCol}"::text) AS sku,
                     MAX("${prodCol}") AS produto_ec
              FROM estoque_consolidado
              WHERE "${skuCol}" IS NOT NULL
                AND TRIM("${skuCol}"::text) != ''
                AND "${prodCol}" IS NOT NULL
                AND TRIM("${prodCol}"::text) != ''
              GROUP BY TRIM("${skuCol}"::text)
            ) ec_nome ON ec_nome.sku = TRIM(c."Sku"::text)
            LIMIT 5000
          `);
        } else {
          result = await pool.query(`SELECT * FROM cadastros_sku LIMIT 5000`);
        }
      } catch {
        result = await pool.query(`SELECT * FROM cadastros_sku LIMIT 5000`);
      }
      return res.json(result.rows);
    }
    result = await pool.query(`SELECT * FROM ${tabela} LIMIT 5000`);
    res.json(result.rows);
  } catch (e) {
    console.error(`[ERRO] ${payload.company} / ${tabela}:`, e.message, e.stack);
    res.status(500).json({ error: e.message });
  }
};
