// ── Test: Cenários Playground ──────────────────────────────────────────────────
// Usage: node test/cenarios.test.js
// Requires: API_URL and TOKEN env vars
// Example: API_URL=http://localhost:3000 TOKEN=eyJ... node test/cenarios.test.js

const API_URL = process.env.API_URL || 'http://localhost:3000';
const TOKEN = process.env.TOKEN;

if (!TOKEN) {
  console.error('❌ Set TOKEN env var with a valid JWT. Example:');
  console.error('   TOKEN=eyJ... node test/cenarios.test.js');
  process.exit(1);
}

const headers = {
  'Content-Type': 'application/json',
  'Authorization': `Bearer ${TOKEN}`,
};

async function api(method, action, body = null, extra = '') {
  const url = `${API_URL}/api/cenarios?action=${action}${extra}`;
  const opts = { method, headers };
  if (body) opts.body = JSON.stringify(body);
  const r = await fetch(url, opts);
  const data = await r.json();
  if (!r.ok && r.status !== 400 && r.status !== 404) {
    console.error(`  ❌ ${method} ${action}${extra} → ${r.status}`, data);
  }
  return { status: r.status, data };
}

let cenarioId = null;
let branchId = null;
let ajusteId = null;
let regraId = null;

async function run() {
  console.log('═══════════════════════════════════════════════════════');
  console.log(' Cenários Playground — Integration Tests');
  console.log('═══════════════════════════════════════════════════════\n');

  // 1. Criar cenário
  console.log('1. Criar cenário...');
  {
    const { status, data } = await api('POST', 'criar', {
      nome: 'Teste Automatizado',
      descricao: 'Cenário criado pelo test runner',
      ano: 2026,
      mes_inicio: 4,
      mes_fim: 6,
    });
    console.assert(status === 200 && data.ok, '  Criar cenário falhou');
    cenarioId = data.cenario?.id;
    console.log(`  ✅ Cenário criado: id=${cenarioId}`);
  }

  // 2. Listar cenários
  console.log('2. Listar cenários...');
  {
    const { data } = await api('GET', 'listar', null, '&ano=2026');
    console.assert(Array.isArray(data.cenarios), '  Listar falhou');
    const found = data.cenarios.find(c => c.id === cenarioId);
    console.assert(found, '  Cenário recém-criado não encontrado na listagem');
    console.log(`  ✅ ${data.cenarios.length} cenários encontrados`);
  }

  // 3. Detalhe (consolidado)
  console.log('3. Detalhe consolidado...');
  {
    const { data } = await api('GET', 'detalhe', null, `&id=${cenarioId}`);
    console.assert(data.cenario && data.months, '  Detalhe falhou');
    console.log(`  ✅ ${data.months.length} meses, ${data.categorias?.length || 0} categorias, ${data.ajustes_aplicados?.length || 0} ajustes`);
  }

  // 4. Editar célula (override)
  console.log('4. Editar célula (override)...');
  {
    const { status, data } = await api('POST', 'ajuste', {
      mes: 4,
      categoria: 'FORNECEDORES',
      dia: 15,
      valor_novo_centavos: -5000000,
      descricao: 'Teste: fornecedor antecipado',
    }, `&id=${cenarioId}`);
    console.assert(status === 200 && data.ok, '  Criar ajuste falhou');
    ajusteId = data.ajuste?.id;
    console.log(`  ✅ Ajuste criado: id=${ajusteId}`);
  }

  // 5. Editar célula (lançamento novo)
  console.log('5. Lançamento novo (empréstimo)...');
  {
    const { status, data } = await api('POST', 'ajuste', {
      tipo: 'lancamento_novo',
      mes: 5,
      categoria: 'CAPTAÇÃO DE EMPRÉSTIMOS',
      dia: 10,
      valor_novo_centavos: 10000000,
      descricao: 'Simulação empréstimo R$100k',
    }, `&id=${cenarioId}`);
    console.assert(status === 200 && data.ok, '  Lançamento novo falhou');
    console.log(`  ✅ Lançamento novo criado: id=${data.ajuste?.id}`);
  }

  // 6. Verificar ajustes no detalhe
  console.log('6. Verificar ajustes aplicados...');
  {
    const { data } = await api('GET', 'detalhe', null, `&id=${cenarioId}`);
    console.assert(data.ajustes_aplicados.length >= 2, '  Deveria ter >= 2 ajustes');
    console.log(`  ✅ ${data.ajustes_aplicados.length} ajustes aplicados`);
  }

  // 7. Aplicar regra (+10% em todas entradas de abril)
  console.log('7. Aplicar regra em lote...');
  {
    const { status, data } = await api('POST', 'regra', {
      nome: 'Projeção otimista entradas',
      tipo: 'percentual',
      parametro: 10,
      escopo_json: { meses: [4] },
    }, `&id=${cenarioId}`);
    console.assert(status === 200 && data.ok, '  Aplicar regra falhou');
    regraId = data.regra?.id;
    console.log(`  ✅ Regra id=${regraId}, ${data.ajustes_criados?.length || 0} ajustes gerados`);
  }

  // 8. Undo (desfaz regra)
  console.log('8. Undo...');
  {
    const { data } = await api('POST', 'undo', null, `&id=${cenarioId}`);
    console.assert(data.ok, '  Undo falhou');
    console.log(`  ✅ Undo: ${data.undone}`);
  }

  // 9. Redo (reaplica regra)
  console.log('9. Redo...');
  {
    const { data } = await api('POST', 'redo', null, `&id=${cenarioId}`);
    console.assert(data.ok, '  Redo falhou');
    console.log(`  ✅ Redo: ${data.redone}`);
  }

  // 10. Branch (criar cenário derivado)
  console.log('10. Branch (cenário derivado)...');
  {
    const { status, data } = await api('POST', 'criar', {
      nome: 'Branch do Teste',
      descricao: 'Cenário filho derivado',
      ano: 2026,
      mes_inicio: 4,
      mes_fim: 6,
      cenario_pai_id: cenarioId,
    });
    console.assert(status === 200 && data.ok, '  Branch falhou');
    branchId = data.cenario?.id;
    console.log(`  ✅ Branch criado: id=${branchId}, pai_id=${cenarioId}`);
  }

  // 11. Compare
  console.log('11. Comparar cenários...');
  {
    const { data } = await api('GET', 'compare', null, `&ids=${cenarioId},${branchId}`);
    console.assert(data.cenarios && data.cenarios.length === 2, '  Compare falhou');
    console.log(`  ✅ ${data.divergencias?.length || 0} divergências (deve ser 0 pois branch é cópia)`);
  }

  // 12. Sugestões
  console.log('12. Sugestões históricas...');
  {
    const { data } = await api('GET', 'sugestoes', null, `&id=${cenarioId}&categoria=FORNECEDORES&mes=4&dia=15`);
    console.log(`  ✅ media_3m=${data.media_3m}, media_6m=${data.media_6m}, media_12m=${data.media_12m}`);
  }

  // 13. Histórico
  console.log('13. Histórico...');
  {
    const { data } = await api('GET', 'historico', null, `&id=${cenarioId}`);
    console.assert(Array.isArray(data.historico), '  Histórico falhou');
    console.log(`  ✅ ${data.historico.length} eventos no histórico`);
  }

  // 14. Refresh
  console.log('14. Refresh snapshot...');
  {
    const { data } = await api('POST', 'refresh', null, `&id=${cenarioId}`);
    console.assert(data.ok, '  Refresh falhou');
    console.log(`  ✅ Preservados: ${data.ajustes_preservados}, Órfãos: ${data.ajustes_orfaos?.length || 0}`);
  }

  // 15. Soft delete
  console.log('15. Arquivar cenário...');
  {
    const { data } = await api('DELETE', 'deletar', null, `&id=${branchId}`);
    console.assert(data.ok, '  Deletar falhou');
    console.log(`  ✅ Branch arquivado`);
  }
  {
    const { data } = await api('DELETE', 'deletar', null, `&id=${cenarioId}`);
    console.assert(data.ok, '  Deletar falhou');
    console.log(`  ✅ Cenário principal arquivado`);
  }

  console.log('\n═══════════════════════════════════════════════════════');
  console.log(' ✅ Todos os testes concluídos!');
  console.log('═══════════════════════════════════════════════════════');
}

run().catch(e => {
  console.error('❌ Erro fatal:', e.message);
  process.exit(1);
});
