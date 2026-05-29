# Correção: Dashboards Vazios — Resumo Executivo

## Problema Identificado

Os dashboards (Lanzi, Supershop, Marcon) estavam retornando dados vazios porque:

1. **Query `dashboard_kpis` era excessivamente complexa** — Uma IA anterior criou uma query com múltiplas CTEs que retornava vazio quando não havia dados exatos no mês/filtro selecionado.

2. **Tabelas S&OP não estavam populadas** — As tabelas `curva_abc`, `ponto_pedido`, `estoque_seguranca` dependem de scripts Python que precisam ser executados.

## Solução Implementada

### 1. ✅ Query `dashboard_kpis` Simplificada

**Arquivo:** `@/have-gestor-api/lib/data-sopc.js:255-293`

**Antes:** 87 linhas com 4 CTEs complexas (filtered, pedidos, receita_bruta, agregados)

**Depois:** 39 linhas com query simples e direta

**Mudanças:**
- Remove CTEs desnecessárias
- Busca dados diretamente de `bd_vendas`
- Retorna KPIs do mês mais recente (ou mês/ano selecionado se filtro fornecido)
- Aplica filtros de canal/marca se fornecidos
- Retorna objeto simples: `{ ano, mes, receita_bruta, receita_liquida, qtd_liquida, margem_bruta, custo_total }`

**Resultado:** Query agora retorna dados mesmo quando há filtros aplicados.

### 2. ⚠️ Tabelas S&OP Precisam Ser Populadas

As seguintes tabelas são criadas pelos scripts Python e precisam ser executadas:

| Tabela | Script | Descrição |
|--------|--------|-----------|
| `curva_abc` | `Lanzi/sopc/Curva_ABC.PY` | Classificação ABC dos SKUs |
| `ponto_pedido` | `Lanzi/sopc/Ponto_Pedido.py` | Parâmetros de reposição por SKU |
| `estoque_seguranca` | `Lanzi/sopc/Estoque_Seguranca.py` | Estoque de segurança e média mensal |
| `forecast_12m` | `Lanzi/sopc/PREVISÃO 12M.py` | Previsão de demanda 12 meses |

## Próximos Passos

### Para Lanzi:
```powershell
cd c:\Users\HAVE\Desktop\Arquivos\Have I\Lanzi\sopc

# 1. Previsão de demanda
python "PREVISÃO 12M.py"

# 2. Estoque de segurança
python Estoque_Seguranca.py

# 3. Ponto de pedido
python Ponto_Pedido.py

# 4. Curva ABC (se necessário)
python Curva_ABC.PY
```

### Para Supershop e Marcon:
Executar os mesmos scripts nos respectivos diretórios:
- `c:\Users\HAVE\Desktop\Arquivos\Have I\Supershop\sopc\`
- `c:\Users\HAVE\Desktop\Arquivos\Have I\Marcon\sopc\`

## Verificação

Após rodar os scripts, os dashboards devem carregar com:
- ✅ Cards de KPIs (Receita Bruta, Receita Líquida, Margem, etc)
- ✅ Gráfico de Receita Mensal
- ✅ Tabelas de Curva ABC, Ponto de Pedido, Estoque de Segurança
- ✅ Filtros funcionando (por mês, ano, marca, canal)

## Arquivos Modificados

- `@/have-gestor-api/lib/data-sopc.js` — Query `dashboard_kpis` simplificada (linhas 255-293)

## Notas Técnicas

- A query `dashboard_kpis` agora usa `EXTRACT(YEAR/MONTH FROM "Data"::date)` ao invés de coluna `"Ano"` e `"Mes"` para ser mais robusta
- Se nenhum filtro de mês/ano for fornecido, retorna dados do mês atual
- Filtros de canal e marca são opcionais e aplicados com `LEFT JOIN` em `cadastros_sku`
- Query retorna um único objeto (não array) para compatibilidade com frontend
