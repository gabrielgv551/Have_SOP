// ── Cenários What-If — Utility Functions ──────────────────────────────────────
// consolidarCenario, aplicarRegra, refreshSnapshot, undo, redo, sugestoes

const { consolidarMes, consolidarAnual, nextBizDay } = require('./consolidar-caixa');

// ── ENSURE TABLES EXIST ──────────────────────────────────────────────────────
// Idempotent — safe to call on every request in serverless
async function ensureTables(pool) {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS cenarios (
      id SERIAL PRIMARY KEY, empresa VARCHAR(50) NOT NULL, nome VARCHAR(200) NOT NULL,
      descricao TEXT, ano INTEGER NOT NULL, mes_inicio INTEGER NOT NULL, mes_fim INTEGER NOT NULL,
      cenario_pai_id INTEGER REFERENCES cenarios(id) ON DELETE SET NULL,
      criado_por VARCHAR(100), criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      atualizado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP, arquivado BOOLEAN DEFAULT FALSE
    );
    CREATE TABLE IF NOT EXISTS cenarios_snapshot_base (
      id SERIAL PRIMARY KEY, cenario_id INTEGER NOT NULL REFERENCES cenarios(id) ON DELETE CASCADE,
      mes INTEGER NOT NULL, categoria VARCHAR(100) NOT NULL, dia INTEGER NOT NULL,
      valor_centavos INTEGER NOT NULL DEFAULT 0,
      origem VARCHAR(20) NOT NULL DEFAULT 'realizado' CHECK (origem IN ('realizado','previsao')),
      capturado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS cenarios_regras (
      id SERIAL PRIMARY KEY, cenario_id INTEGER NOT NULL REFERENCES cenarios(id) ON DELETE CASCADE,
      nome VARCHAR(200) NOT NULL,
      tipo VARCHAR(30) NOT NULL CHECK (tipo IN ('percentual','valor_fixo','substituicao')),
      parametro NUMERIC NOT NULL, escopo_json JSONB NOT NULL DEFAULT '{}',
      criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS cenarios_ajustes (
      id SERIAL PRIMARY KEY, cenario_id INTEGER NOT NULL REFERENCES cenarios(id) ON DELETE CASCADE,
      tipo VARCHAR(30) NOT NULL DEFAULT 'override' CHECK (tipo IN ('override','lancamento_novo')),
      mes INTEGER NOT NULL, categoria VARCHAR(100) NOT NULL, dia INTEGER NOT NULL,
      valor_original_centavos INTEGER, valor_novo_centavos INTEGER NOT NULL,
      descricao TEXT, regra_id INTEGER REFERENCES cenarios_regras(id) ON DELETE CASCADE,
      criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS cenarios_historico (
      id SERIAL PRIMARY KEY, cenario_id INTEGER NOT NULL REFERENCES cenarios(id) ON DELETE CASCADE,
      operacao VARCHAR(50) NOT NULL, payload_json JSONB NOT NULL DEFAULT '{}',
      payload_reverso_json JSONB NOT NULL DEFAULT '{}',
      criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP, desfeito_em TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS cenarios_refresh_log (
      id SERIAL PRIMARY KEY, cenario_id INTEGER NOT NULL REFERENCES cenarios(id) ON DELETE CASCADE,
      refreshed_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      ajustes_preservados INTEGER NOT NULL DEFAULT 0,
      ajustes_orfaos_json JSONB NOT NULL DEFAULT '[]'
    );
    CREATE TABLE IF NOT EXISTS cenario_eventos (
      id SERIAL PRIMARY KEY,
      cenario_id INTEGER NOT NULL REFERENCES cenarios(id) ON DELETE CASCADE,
      tipo VARCHAR(50) NOT NULL,
      nome VARCHAR(200) NOT NULL,
      data_inicio DATE NOT NULL,
      data_fim DATE,
      parametros JSONB NOT NULL DEFAULT '{}',
      ativo BOOLEAN NOT NULL DEFAULT TRUE,
      ordem INTEGER NOT NULL DEFAULT 0,
      criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      atualizado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    ALTER TABLE cenarios ADD COLUMN IF NOT EXISTS horizonte_meses INTEGER NOT NULL DEFAULT 12;
    ALTER TABLE cenario_eventos DROP CONSTRAINT IF EXISTS cenario_eventos_tipo_check;
  `);
}

// ── CREATE SNAPSHOT ──────────────────────────────────────────────────────────
// Captures the current consolidated data for a range of months
async function createSnapshot(pool, client, company, cenarioId, ano, mesInicio, mesFim) {
  const todayStr = new Date().toISOString().slice(0, 10);
  for (let m = mesInicio; m <= mesFim; m++) {
    const data = await consolidarMes(pool, company, ano, m, { apenas_futuros: false });
    const rows = [];

    // Realizado — extrato bancário (sempre incluído, passado e presente)
    for (const [cat, dias] of Object.entries(data.valores || {})) {
      for (const [dia, val] of Object.entries(dias)) {
        rows.push([cenarioId, m, cat, parseInt(dia), parseInt(val), 'realizado']);
      }
    }
    // Previsão: combina valores_previsao + valores_previsao_pc (pedidos de compra), só datas futuras
    const previsaoMerged = {};
    for (const bucket of [data.valores_previsao || {}, data.valores_previsao_pc || {}]) {
      for (const [cat, dias] of Object.entries(bucket)) {
        for (const [dia, val] of Object.entries(dias)) {
          if (!val) continue;
          if (!previsaoMerged[cat]) previsaoMerged[cat] = {};
          previsaoMerged[cat][dia] = (previsaoMerged[cat][dia] || 0) + parseInt(val);
        }
      }
    }
    for (const [cat, dias] of Object.entries(previsaoMerged)) {
      for (const [dia, val] of Object.entries(dias)) {
        if (!val) continue;
        const dStr = `${ano}-${String(m).padStart(2,'0')}-${String(parseInt(dia)).padStart(2,'0')}`;
        if (dStr < todayStr) continue;
        rows.push([cenarioId, m, cat, parseInt(dia), val, 'previsao']);
      }
    }

    // Bulk insert in chunks
    const CHUNK = 200;
    for (let i = 0; i < rows.length; i += CHUNK) {
      const chunk = rows.slice(i, i + CHUNK);
      const vals = [], params = [];
      chunk.forEach((r, idx) => {
        const b = idx * 6;
        vals.push(`($${b+1},$${b+2},$${b+3},$${b+4},$${b+5},$${b+6})`);
        params.push(...r);
      });
      await client.query(
        `INSERT INTO cenarios_snapshot_base (cenario_id, mes, categoria, dia, valor_centavos, origem)
         VALUES ${vals.join(',')}
         ON CONFLICT DO NOTHING`,
        params
      );
    }
  }
}

// ── CONSOLIDAR CENÁRIO ───────────────────────────────────────────────────────
// Applies precedence: override > lancamento_novo (sum) > snapshot base
// Returns { categorias, months: [{ mes, valores, valores_previsao }], ajustes_aplicados }
async function consolidarCenario(pool, cenarioId) {
  const cenR = await pool.query('SELECT * FROM cenarios WHERE id=$1', [cenarioId]);
  if (!cenR.rows.length) throw new Error('Cenário não encontrado');
  const cenario = cenR.rows[0];

  // Get snapshot base
  const snapR = await pool.query(
    'SELECT mes, categoria, dia, valor_centavos, origem FROM cenarios_snapshot_base WHERE cenario_id=$1',
    [cenarioId]
  );

  // Get all ajustes (non-reverted)
  const ajR = await pool.query(
    'SELECT id, tipo, mes, categoria, dia, valor_original_centavos, valor_novo_centavos, descricao, regra_id FROM cenarios_ajustes WHERE cenario_id=$1 ORDER BY id',
    [cenarioId]
  );

  // Get categorias for this company
  const catsR = await pool.query(
    'SELECT id, nome, tipo, parent, ordem FROM caixa_categorias WHERE empresa=$1 ORDER BY ordem',
    [cenario.empresa]
  );

  // Build base data structure: months[m] = { valores: {cat: {dia: val}}, valores_previsao: {cat: {dia: val}} }
  const months = {};
  for (let m = cenario.mes_inicio; m <= cenario.mes_fim; m++) {
    months[m] = { mes: m, valores: {}, valores_previsao: {} };
  }

  // Fill from snapshot
  for (const row of snapR.rows) {
    const m = parseInt(row.mes);
    if (!months[m]) continue;
    const bucket = row.origem === 'realizado' ? 'valores' : 'valores_previsao';
    if (!months[m][bucket][row.categoria]) months[m][bucket][row.categoria] = {};
    months[m][bucket][row.categoria][row.dia] = parseInt(row.valor_centavos);
  }

  // Apply ajustes with precedence
  // Group ajustes by (mes, categoria, dia)
  const ajusteMap = {}; // key: "mes|cat|dia" → { overrides: [], novos: [] }
  const ajustes_aplicados = [];

  for (const aj of ajR.rows) {
    const key = `${aj.mes}|${aj.categoria}|${aj.dia}`;
    if (!ajusteMap[key]) ajusteMap[key] = { overrides: [], novos: [] };
    if (aj.tipo === 'override') ajusteMap[key].overrides.push(aj);
    else if (aj.tipo === 'lancamento_novo') ajusteMap[key].novos.push(aj);
  }

  for (const [key, group] of Object.entries(ajusteMap)) {
    const [mesStr, cat, diaStr] = key.split('|');
    const m = parseInt(mesStr), dia = parseInt(diaStr);
    if (!months[m]) continue;

    // Determine which bucket this category falls into
    // Try valores_previsao first (ajustes more commonly affect projections), fallback to valores
    let bucket = 'valores_previsao';
    if (months[m].valores[cat] && months[m].valores[cat][dia] !== undefined) {
      bucket = 'valores';
    }

    if (group.overrides.length > 0) {
      // Last override wins
      const last = group.overrides[group.overrides.length - 1];
      if (!months[m][bucket][cat]) months[m][bucket][cat] = {};
      months[m][bucket][cat][dia] = parseInt(last.valor_novo_centavos);
      ajustes_aplicados.push({
        id: last.id, tipo: 'override', mes: m, categoria: cat, dia,
        valor_original: last.valor_original_centavos, valor_novo: last.valor_novo_centavos,
        descricao: last.descricao, regra_id: last.regra_id,
      });
    } else if (group.novos.length > 0) {
      // Sum lancamentos novos on top of base
      const base = (months[m][bucket][cat] && months[m][bucket][cat][dia]) || 0;
      let soma = 0;
      for (const novo of group.novos) {
        soma += parseInt(novo.valor_novo_centavos);
        ajustes_aplicados.push({
          id: novo.id, tipo: 'lancamento_novo', mes: m, categoria: cat, dia,
          valor_novo: novo.valor_novo_centavos, descricao: novo.descricao,
        });
      }
      if (!months[m][bucket][cat]) months[m][bucket][cat] = {};
      months[m][bucket][cat][dia] = base + soma;
    }
  }

  // Build ordered array
  const monthsArr = [];
  for (let m = cenario.mes_inicio; m <= cenario.mes_fim; m++) {
    if (months[m]) monthsArr.push(months[m]);
  }

  return {
    cenario: {
      id: cenario.id, nome: cenario.nome, descricao: cenario.descricao,
      ano: cenario.ano, mes_inicio: cenario.mes_inicio, mes_fim: cenario.mes_fim,
      cenario_pai_id: cenario.cenario_pai_id, arquivado: cenario.arquivado,
      criado_por: cenario.criado_por, criado_em: cenario.criado_em,
    },
    categorias: catsR.rows,
    months: monthsArr,
    ajustes_aplicados,
  };
}

// ── APLICAR REGRA ────────────────────────────────────────────────────────────
// Expands a rule into individual ajustes. Returns { regra, ajustes_criados }
// escopo_json: { categorias: [...], meses: [...], dias: [...] }
// tipo: 'percentual' (param=10 → +10%), 'valor_fixo' (param=5000 → +R$50), 'substituicao' (param=10000 → set R$100)
async function aplicarRegra(pool, client, cenarioId, regra) {
  const { nome, tipo, parametro, escopo_json } = regra;
  const escopo = typeof escopo_json === 'string' ? JSON.parse(escopo_json) : escopo_json;

  // Insert the rule
  const rR = await client.query(
    `INSERT INTO cenarios_regras (cenario_id, nome, tipo, parametro, escopo_json)
     VALUES ($1,$2,$3,$4,$5) RETURNING id`,
    [cenarioId, nome, tipo, parametro, JSON.stringify(escopo)]
  );
  const regraId = rR.rows[0].id;

  // Get snapshot cells that match the escopo
  let snapQuery = 'SELECT mes, categoria, dia, valor_centavos, origem FROM cenarios_snapshot_base WHERE cenario_id=$1';
  const snapParams = [cenarioId];
  const snapR = await client.query(snapQuery, snapParams);

  const ajustesCriados = [];
  const ajusteIds = [];

  for (const cell of snapR.rows) {
    // Check if cell matches escopo
    if (escopo.categorias && escopo.categorias.length > 0) {
      if (!escopo.categorias.includes(cell.categoria)) continue;
    }
    if (escopo.meses && escopo.meses.length > 0) {
      if (!escopo.meses.includes(cell.mes)) continue;
    }
    if (escopo.dias && escopo.dias.length > 0) {
      if (!escopo.dias.includes(cell.dia)) continue;
    }

    const valorOriginal = parseInt(cell.valor_centavos);
    let valorNovo;

    if (tipo === 'percentual') {
      // parametro = percentage change, e.g. 10 = +10%, -5 = -5%
      valorNovo = Math.round(valorOriginal * (1 + parseFloat(parametro) / 100));
    } else if (tipo === 'valor_fixo') {
      // parametro = centavos to add
      valorNovo = valorOriginal + Math.round(parseFloat(parametro));
    } else if (tipo === 'substituicao') {
      // parametro = new value in centavos
      valorNovo = Math.round(parseFloat(parametro));
    } else {
      continue;
    }

    const ajR = await client.query(
      `INSERT INTO cenarios_ajustes (cenario_id, tipo, mes, categoria, dia, valor_original_centavos, valor_novo_centavos, regra_id)
       VALUES ($1,'override',$2,$3,$4,$5,$6,$7) RETURNING id`,
      [cenarioId, cell.mes, cell.categoria, cell.dia, valorOriginal, valorNovo, regraId]
    );
    ajusteIds.push(ajR.rows[0].id);
    ajustesCriados.push({
      id: ajR.rows[0].id, mes: cell.mes, categoria: cell.categoria, dia: cell.dia,
      valor_original: valorOriginal, valor_novo: valorNovo,
    });
  }

  // Record in history
  await registrarHistorico(client, cenarioId, 'aplicar_regra', {
    regra_id: regraId, nome, tipo, parametro, escopo_json: escopo,
    ajuste_ids: ajusteIds,
  }, {
    regra_id: regraId, ajuste_ids: ajusteIds,
  });

  return { regra: { id: regraId, nome, tipo, parametro, escopo_json: escopo }, ajustes_criados: ajustesCriados };
}

// ── REFRESH SNAPSHOT ─────────────────────────────────────────────────────────
// Recalculates snapshot from real data, preserves applicable ajustes, lists orphans
async function refreshSnapshot(pool, cenarioId) {
  const cenR = await pool.query('SELECT * FROM cenarios WHERE id=$1', [cenarioId]);
  if (!cenR.rows.length) throw new Error('Cenário não encontrado');
  const cenario = cenR.rows[0];

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    // Get existing ajustes before deleting snapshot
    const ajR = await client.query(
      'SELECT id, tipo, mes, categoria, dia, valor_original_centavos, valor_novo_centavos, descricao, regra_id FROM cenarios_ajustes WHERE cenario_id=$1',
      [cenarioId]
    );
    const existingAjustes = ajR.rows;

    // Delete old snapshot
    await client.query('DELETE FROM cenarios_snapshot_base WHERE cenario_id=$1', [cenarioId]);

    // Create fresh snapshot
    await createSnapshot(pool, client, cenario.empresa, cenarioId, cenario.ano, cenario.mes_inicio, cenario.mes_fim);

    // Get new snapshot keys
    const newSnapR = await client.query(
      'SELECT DISTINCT mes, categoria, dia FROM cenarios_snapshot_base WHERE cenario_id=$1',
      [cenarioId]
    );
    const newKeys = new Set(newSnapR.rows.map(r => `${r.mes}|${r.categoria}|${r.dia}`));

    // Classify ajustes: preserved vs orphan
    const preservados = [], orfaos = [];
    for (const aj of existingAjustes) {
      const key = `${aj.mes}|${aj.categoria}|${aj.dia}`;
      if (aj.tipo === 'lancamento_novo') {
        // Lancamentos novos are always preserved (they don't depend on snapshot)
        preservados.push(aj);
      } else if (newKeys.has(key)) {
        // Update valor_original to match new snapshot
        const newValR = await client.query(
          'SELECT valor_centavos FROM cenarios_snapshot_base WHERE cenario_id=$1 AND mes=$2 AND categoria=$3 AND dia=$4 LIMIT 1',
          [cenarioId, aj.mes, aj.categoria, aj.dia]
        );
        if (newValR.rows.length) {
          await client.query(
            'UPDATE cenarios_ajustes SET valor_original_centavos=$1 WHERE id=$2',
            [parseInt(newValR.rows[0].valor_centavos), aj.id]
          );
        }
        preservados.push(aj);
      } else {
        orfaos.push({
          id: aj.id, mes: aj.mes, categoria: aj.categoria, dia: aj.dia,
          tipo: aj.tipo, valor_novo: aj.valor_novo_centavos, descricao: aj.descricao,
        });
      }
    }

    // Log the refresh
    await client.query(
      `INSERT INTO cenarios_refresh_log (cenario_id, ajustes_preservados, ajustes_orfaos_json)
       VALUES ($1,$2,$3)`,
      [cenarioId, preservados.length, JSON.stringify(orfaos)]
    );

    await client.query(
      'UPDATE cenarios SET atualizado_em=CURRENT_TIMESTAMP WHERE id=$1',
      [cenarioId]
    );

    await client.query('COMMIT');

    return {
      ok: true,
      ajustes_preservados: preservados.length,
      ajustes_orfaos: orfaos,
    };
  } catch (e) {
    await client.query('ROLLBACK');
    throw e;
  } finally {
    client.release();
  }
}

// ── REGISTRAR HISTÓRICO ──────────────────────────────────────────────────────
async function registrarHistorico(client, cenarioId, operacao, payload, payloadReverso) {
  await client.query(
    `INSERT INTO cenarios_historico (cenario_id, operacao, payload_json, payload_reverso_json)
     VALUES ($1,$2,$3,$4)`,
    [cenarioId, operacao, JSON.stringify(payload), JSON.stringify(payloadReverso)]
  );

  // Trim history to 100 events, purge redo stack (events after undo)
  await client.query(`
    DELETE FROM cenarios_historico
    WHERE cenario_id=$1 AND id NOT IN (
      SELECT id FROM cenarios_historico WHERE cenario_id=$1 ORDER BY criado_em DESC LIMIT 100
    )
  `, [cenarioId]);
}

// ── UNDO ─────────────────────────────────────────────────────────────────────
async function undo(pool, cenarioId) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    // Find the last non-undone event
    const evR = await client.query(
      `SELECT id, operacao, payload_json, payload_reverso_json
       FROM cenarios_historico
       WHERE cenario_id=$1 AND desfeito_em IS NULL
       ORDER BY criado_em DESC LIMIT 1`,
      [cenarioId]
    );
    if (!evR.rows.length) {
      await client.query('ROLLBACK');
      return { ok: false, message: 'Nada para desfazer' };
    }

    const ev = evR.rows[0];
    const reverso = typeof ev.payload_reverso_json === 'string'
      ? JSON.parse(ev.payload_reverso_json)
      : ev.payload_reverso_json;

    // Apply reverse based on operation type
    if (ev.operacao === 'criar_ajuste') {
      // Reverse: delete the ajuste
      if (reverso.ajuste_id) {
        await client.query('DELETE FROM cenarios_ajustes WHERE id=$1 AND cenario_id=$2', [reverso.ajuste_id, cenarioId]);
      }
    } else if (ev.operacao === 'remover_ajuste') {
      // Reverse: re-insert the ajuste
      if (reverso.ajuste) {
        const a = reverso.ajuste;
        await client.query(
          `INSERT INTO cenarios_ajustes (cenario_id, tipo, mes, categoria, dia, valor_original_centavos, valor_novo_centavos, descricao, regra_id)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
          [cenarioId, a.tipo, a.mes, a.categoria, a.dia, a.valor_original_centavos, a.valor_novo_centavos, a.descricao || null, a.regra_id || null]
        );
      }
    } else if (ev.operacao === 'aplicar_regra') {
      // Reverse: delete all ajustes from this rule, and the rule itself
      if (reverso.regra_id) {
        await client.query('DELETE FROM cenarios_ajustes WHERE regra_id=$1 AND cenario_id=$2', [reverso.regra_id, cenarioId]);
        await client.query('DELETE FROM cenarios_regras WHERE id=$1 AND cenario_id=$2', [reverso.regra_id, cenarioId]);
      }
    } else if (ev.operacao === 'reverter_regra') {
      // Reverse: re-apply the rule — we stored the full rule + ajustes in payload_reverso
      if (reverso.regra && reverso.ajustes) {
        const r = reverso.regra;
        const rR = await client.query(
          `INSERT INTO cenarios_regras (cenario_id, nome, tipo, parametro, escopo_json)
           VALUES ($1,$2,$3,$4,$5) RETURNING id`,
          [cenarioId, r.nome, r.tipo, r.parametro, JSON.stringify(r.escopo_json)]
        );
        const newRegraId = rR.rows[0].id;
        for (const a of reverso.ajustes) {
          await client.query(
            `INSERT INTO cenarios_ajustes (cenario_id, tipo, mes, categoria, dia, valor_original_centavos, valor_novo_centavos, regra_id)
             VALUES ($1,'override',$2,$3,$4,$5,$6,$7)`,
            [cenarioId, a.mes, a.categoria, a.dia, a.valor_original, a.valor_novo, newRegraId]
          );
        }
      }
    }

    // Mark event as undone
    await client.query('UPDATE cenarios_historico SET desfeito_em=CURRENT_TIMESTAMP WHERE id=$1', [ev.id]);

    await client.query('COMMIT');
    return { ok: true, undone: ev.operacao, event_id: ev.id };
  } catch (e) {
    await client.query('ROLLBACK');
    throw e;
  } finally {
    client.release();
  }
}

// ── REDO ─────────────────────────────────────────────────────────────────────
async function redo(pool, cenarioId) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    // Find the most recent undone event
    const evR = await client.query(
      `SELECT id, operacao, payload_json, payload_reverso_json
       FROM cenarios_historico
       WHERE cenario_id=$1 AND desfeito_em IS NOT NULL
       ORDER BY desfeito_em DESC LIMIT 1`,
      [cenarioId]
    );
    if (!evR.rows.length) {
      await client.query('ROLLBACK');
      return { ok: false, message: 'Nada para refazer' };
    }

    const ev = evR.rows[0];
    const payload = typeof ev.payload_json === 'string'
      ? JSON.parse(ev.payload_json)
      : ev.payload_json;

    // Re-apply the original operation
    if (ev.operacao === 'criar_ajuste') {
      if (payload.ajuste) {
        const a = payload.ajuste;
        await client.query(
          `INSERT INTO cenarios_ajustes (cenario_id, tipo, mes, categoria, dia, valor_original_centavos, valor_novo_centavos, descricao, regra_id)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
          [cenarioId, a.tipo, a.mes, a.categoria, a.dia, a.valor_original_centavos, a.valor_novo_centavos, a.descricao || null, a.regra_id || null]
        );
      }
    } else if (ev.operacao === 'remover_ajuste') {
      if (payload.ajuste_id) {
        await client.query('DELETE FROM cenarios_ajustes WHERE id=$1 AND cenario_id=$2', [payload.ajuste_id, cenarioId]);
      }
    } else if (ev.operacao === 'aplicar_regra') {
      // Re-apply rule
      if (payload.regra_id) {
        // We need to rebuild — but the rule was deleted on undo. Re-create from payload.
        const rR = await client.query(
          `INSERT INTO cenarios_regras (cenario_id, nome, tipo, parametro, escopo_json)
           VALUES ($1,$2,$3,$4,$5) RETURNING id`,
          [cenarioId, payload.nome, payload.tipo, payload.parametro, JSON.stringify(payload.escopo_json)]
        );
        const newRegraId = rR.rows[0].id;

        // Re-expand the rule from snapshot
        const snapR = await client.query(
          'SELECT mes, categoria, dia, valor_centavos FROM cenarios_snapshot_base WHERE cenario_id=$1',
          [cenarioId]
        );
        const escopo = payload.escopo_json;
        for (const cell of snapR.rows) {
          if (escopo.categorias && escopo.categorias.length && !escopo.categorias.includes(cell.categoria)) continue;
          if (escopo.meses && escopo.meses.length && !escopo.meses.includes(cell.mes)) continue;
          if (escopo.dias && escopo.dias.length && !escopo.dias.includes(cell.dia)) continue;

          const valorOriginal = parseInt(cell.valor_centavos);
          let valorNovo;
          if (payload.tipo === 'percentual') valorNovo = Math.round(valorOriginal * (1 + parseFloat(payload.parametro) / 100));
          else if (payload.tipo === 'valor_fixo') valorNovo = valorOriginal + Math.round(parseFloat(payload.parametro));
          else if (payload.tipo === 'substituicao') valorNovo = Math.round(parseFloat(payload.parametro));
          else continue;

          await client.query(
            `INSERT INTO cenarios_ajustes (cenario_id, tipo, mes, categoria, dia, valor_original_centavos, valor_novo_centavos, regra_id)
             VALUES ($1,'override',$2,$3,$4,$5,$6,$7)`,
            [cenarioId, cell.mes, cell.categoria, cell.dia, valorOriginal, valorNovo, newRegraId]
          );
        }
      }
    } else if (ev.operacao === 'reverter_regra') {
      if (payload.regra_id) {
        await client.query('DELETE FROM cenarios_ajustes WHERE regra_id=$1 AND cenario_id=$2', [payload.regra_id, cenarioId]);
        await client.query('DELETE FROM cenarios_regras WHERE id=$1 AND cenario_id=$2', [payload.regra_id, cenarioId]);
      }
    }

    // Clear the undo marker
    await client.query('UPDATE cenarios_historico SET desfeito_em=NULL WHERE id=$1', [ev.id]);

    await client.query('COMMIT');
    return { ok: true, redone: ev.operacao, event_id: ev.id };
  } catch (e) {
    await client.query('ROLLBACK');
    throw e;
  } finally {
    client.release();
  }
}

// ── SUGESTÕES ────────────────────────────────────────────────────────────────
// Returns historical context for a (categoria, dia) cell
async function sugestoes(pool, company, categoria, mes, dia) {
  const today = new Date();
  const results = {};

  // Helper: query caixa_extrato for matched category amounts
  // We need to figure out which extrato descriptions map to this category
  const dpR = await pool.query(
    "SELECT palavra_chave FROM caixa_de_para WHERE empresa=$1 AND categoria_nome=$2 AND tipo='extrato'",
    [company, categoria]
  );
  const keywords = dpR.rows.map(r => r.palavra_chave.toLowerCase());

  if (keywords.length === 0) {
    return { categoria, mes, dia, media_3m: null, media_6m: null, media_12m: null, min: null, max: null, mesmo_dia_mes_anterior: null, message: 'Nenhum mapeamento de-para encontrado para esta categoria' };
  }

  // Build LIKE conditions for keywords
  const likeConds = keywords.map((_, i) => `LOWER(descricao) LIKE $${i + 2}`);
  const likeParams = keywords.map(k => `%${k}%`);

  // Get last 12 months of data for this category on the same day
  const mesesAtras = [3, 6, 12];
  for (const n of mesesAtras) {
    const r = await pool.query(
      `SELECT SUM(valor) AS total, COUNT(*) AS cnt
       FROM caixa_extrato
       WHERE empresa=$1 AND (${likeConds.join(' OR ')})
         AND dia=$${keywords.length + 2}
         AND (ano * 100 + mes) >= $${keywords.length + 3}`,
      [
        company,
        ...likeParams,
        parseInt(dia),
        (today.getFullYear() * 100 + (today.getMonth() + 1)) - n * 100 / 12,
      ]
    );
    const key = `media_${n}m`;
    if (r.rows[0].cnt > 0) {
      results[key] = Math.round(parseFloat(r.rows[0].total) / parseInt(r.rows[0].cnt));
    } else {
      results[key] = null;
    }
  }

  // Min/Max in the last 12 months for this category (any day)
  const mmR = await pool.query(
    `SELECT MIN(val) AS min_val, MAX(val) AS max_val FROM (
       SELECT SUM(valor) AS val
       FROM caixa_extrato
       WHERE empresa=$1 AND (${likeConds.join(' OR ')})
         AND (ano * 100 + mes) >= $${keywords.length + 2}
       GROUP BY ano, mes, dia
     ) t`,
    [company, ...likeParams, (today.getFullYear() - 1) * 100 + (today.getMonth() + 1)]
  );
  results.min = mmR.rows[0]?.min_val != null ? parseInt(mmR.rows[0].min_val) : null;
  results.max = mmR.rows[0]?.max_val != null ? parseInt(mmR.rows[0].max_val) : null;

  // Same day, previous month
  const prevMes = parseInt(mes) === 1 ? 12 : parseInt(mes) - 1;
  const prevAno = parseInt(mes) === 1 ? today.getFullYear() - 1 : today.getFullYear();
  const prevR = await pool.query(
    `SELECT SUM(valor) AS total
     FROM caixa_extrato
     WHERE empresa=$1 AND (${likeConds.join(' OR ')})
       AND ano=$${keywords.length + 2} AND mes=$${keywords.length + 3} AND dia=$${keywords.length + 4}`,
    [company, ...likeParams, prevAno, prevMes, parseInt(dia)]
  );
  results.mesmo_dia_mes_anterior = prevR.rows[0]?.total != null ? parseInt(prevR.rows[0].total) : null;

  return { categoria, mes: parseInt(mes), dia: parseInt(dia), ...results };
}

module.exports = {
  ensureTables,
  createSnapshot,
  consolidarCenario,
  aplicarRegra,
  refreshSnapshot,
  registrarHistorico,
  undo,
  redo,
  sugestoes,
};
