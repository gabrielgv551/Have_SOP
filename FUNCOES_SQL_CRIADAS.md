# Funções SQL de Cálculo — Resumo

## 📋 Funções Criadas

### 1. `calcular_receita_liquida(empresa, ano, mes)`
Calcula a receita líquida excluindo cancelamentos, devoluções e não pagos.

**Uso:**
```sql
SELECT calcular_receita_liquida('lanzi', 2026, 5);
-- Resultado: 45230.50
```

---

### 2. `calcular_margem_bruta(empresa, ano, mes)`
Calcula a margem bruta total do período.

**Uso:**
```sql
SELECT calcular_margem_bruta('lanzi', 2026, 5);
-- Resultado: 12450.75
```

---

### 3. `calcular_margem_percentual(empresa, ano, mes)`
Calcula a margem como percentual da receita líquida.

**Uso:**
```sql
SELECT calcular_margem_percentual('lanzi', 2026, 5);
-- Resultado: 27.50 (27.50%)
```

---

### 4. `calcular_ticket_medio(empresa, ano, mes)`
Calcula o ticket médio (receita / quantidade).

**Uso:**
```sql
SELECT calcular_ticket_medio('lanzi', 2026, 5);
-- Resultado: 185.42
```

---

### 5. `calcular_receita_bruta(empresa, ano, mes)`
Calcula a receita bruta total sem filtros.

**Uso:**
```sql
SELECT calcular_receita_bruta('lanzi', 2026, 5);
-- Resultado: 48500.00
```

---

### 6. `calcular_custo_total(empresa, ano, mes)`
Calcula o custo total do período.

**Uso:**
```sql
SELECT calcular_custo_total('lanzi', 2026, 5);
-- Resultado: 32779.25
```

---

### 7. `get_dashboard_kpis(empresa, ano, mes)`
Retorna todos os KPIs do dashboard em uma única query.

**Uso:**
```sql
SELECT * FROM get_dashboard_kpis('lanzi', 2026, 5);

-- Resultado:
-- ano | mes | receita_bruta | receita_liquida | qtd_liquida | margem_bruta | margem_pct | ticket_medio | custo_total
-- 2026|  5  |   48500.00    |    45230.50     |    244      |  12450.75    |   27.50    |   185.42     |  32779.25
```

---

### 8. `get_monthly_kpis(empresa, ano_inicio, ano_fim)`
Retorna série temporal de KPIs mensais.

**Uso:**
```sql
SELECT * FROM get_monthly_kpis('lanzi', 2025, 2026);

-- Resultado: 24 linhas (12 meses x 2 anos)
-- ano | mes | receita_bruta | receita_liquida | qtd_liquida | margem_bruta | margem_pct
-- 2025|  1  |   42000.00    |    39500.00     |    210      |  10850.00    |   27.47
-- 2025|  2  |   45000.00    |    42300.00     |    225      |  11630.50    |   27.50
-- ...
-- 2026|  5  |   48500.00    |    45230.50     |    244      |  12450.75    |   27.50
```

---

## 🚀 Como Usar

### Na API (Node.js)

```javascript
// Obter KPIs do mês atual
const result = await pool.query(
  'SELECT * FROM get_dashboard_kpis($1)',
  ['lanzi']
);

// Obter KPIs de um mês específico
const result = await pool.query(
  'SELECT * FROM get_dashboard_kpis($1, $2, $3)',
  ['lanzi', 2026, 5]
);

// Obter série mensal
const result = await pool.query(
  'SELECT * FROM get_monthly_kpis($1, $2, $3)',
  ['lanzi', 2025, 2026]
);
```

### No Frontend

```javascript
// Chamar API que usa as funções
const response = await fetch(
  '/api/data?tabela=dashboard_kpis&ano=2026&mes=5',
  { headers: { 'Authorization': `Bearer ${token}` } }
);
const kpis = await response.json();
```

---

## 📦 Arquivo de Migração

**Localização:** `@/have-gestor-api/migrations/046_update_calculation_functions.sql`

Este arquivo contém todas as funções SQL e pode ser:
1. Executado via SSH no servidor de banco de dados
2. Incluído em um script de setup automático
3. Versionado no Git para controle de mudanças

---

## ✅ Verificação

Para verificar se as funções foram criadas corretamente:

```sql
-- Listar todas as funções
\df calcular_*
\df get_*

-- Testar cada função
SELECT calcular_receita_liquida('lanzi', 2026, 5);
SELECT calcular_margem_bruta('lanzi', 2026, 5);
SELECT * FROM get_dashboard_kpis('lanzi', 2026, 5);
SELECT * FROM get_monthly_kpis('lanzi', 2025, 2026) LIMIT 1;
```

---

## 🔄 Integração com Query Simplificada

A query `dashboard_kpis` simplificada na API agora pode usar essas funções:

```javascript
// Antes: Query complexa com 4 CTEs
// Depois: Usar função SQL
const result = await pool.query(
  'SELECT * FROM get_dashboard_kpis($1, $2, $3)',
  [company, ano, mes]
);
```

Isso torna o código mais limpo, reutilizável e fácil de manter.

---

## 📝 Próximos Passos

1. **Executar via SSH** — Usar instruções em `SSH_UPDATE_INSTRUCTIONS.md`
2. **Testar Funções** — Verificar se retornam dados corretos
3. **Atualizar API** — Usar as funções nas queries
4. **Deploy** — Fazer deploy da API atualizada (já feito via Vercel)
5. **Rodar Scripts S&OP** — Popular tabelas de curva_abc, ponto_pedido, etc
