# Instruções: Atualizar Funções SQL via SSH

## Servidor de Banco de Dados
- **Host:** 37.60.236.200
- **Porta:** 5432
- **Banco:** Lanzi (e outros)

## Passo 1: Conectar via SSH

```bash
ssh postgres@37.60.236.200
# Ou com chave SSH:
ssh -i /caminho/para/chave.pem postgres@37.60.236.200
```

## Passo 2: Executar o Script SQL

### Opção A: Executar arquivo SQL diretamente

```bash
# Copiar arquivo para servidor (do seu PC)
scp c:\Users\HAVE\Desktop\Arquivos\Have\ I\have-gestor-api\migrations\046_update_calculation_functions.sql postgres@37.60.236.200:/tmp/

# Conectar e executar
ssh postgres@37.60.236.200
psql -U postgres -d Lanzi -f /tmp/046_update_calculation_functions.sql

# Repetir para outros bancos se necessário
psql -U postgres -d Supershop -f /tmp/046_update_calculation_functions.sql
psql -U postgres -d Marcon -f /tmp/046_update_calculation_functions.sql
```

### Opção B: Executar via psql interativo

```bash
ssh postgres@37.60.236.200

# Conectar ao banco Lanzi
psql -U postgres -d Lanzi

# Copiar e colar o conteúdo do arquivo SQL
-- (Cole todo o conteúdo de 046_update_calculation_functions.sql)

# Verificar se as funções foram criadas
\df calcular_*
\df get_*
```

## Passo 3: Verificar Funções Criadas

```sql
-- Listar todas as funções de cálculo
SELECT routine_name, routine_type 
FROM information_schema.routines 
WHERE routine_schema = 'public' 
  AND routine_name LIKE 'calcular_%' 
  OR routine_name LIKE 'get_%'
ORDER BY routine_name;

-- Testar função de KPIs
SELECT * FROM get_dashboard_kpis('lanzi', 2026, 5);

-- Testar série mensal
SELECT * FROM get_monthly_kpis('lanzi', 2025, 2026) LIMIT 12;
```

## Passo 4: Confirmar Sucesso

Se ver a mensagem:
```
Calculation functions updated successfully
```

Então as funções foram criadas com sucesso!

## Funções Criadas

| Função | Descrição |
|--------|-----------|
| `calcular_receita_liquida()` | Receita excluindo cancelamentos |
| `calcular_margem_bruta()` | Margem total do período |
| `calcular_margem_percentual()` | Margem como % da receita |
| `calcular_ticket_medio()` | Receita / Quantidade |
| `calcular_receita_bruta()` | Receita total sem filtros |
| `calcular_custo_total()` | Custo total do período |
| `get_dashboard_kpis()` | Retorna todos os KPIs de uma vez |
| `get_monthly_kpis()` | Retorna série temporal de KPIs |

## Uso nas Queries

Agora você pode usar essas funções nas queries:

```sql
-- Exemplo 1: Obter KPIs do mês atual
SELECT * FROM get_dashboard_kpis('lanzi');

-- Exemplo 2: Obter KPIs de um mês específico
SELECT * FROM get_dashboard_kpis('lanzi', 2026, 5);

-- Exemplo 3: Obter série mensal
SELECT * FROM get_monthly_kpis('lanzi', 2025, 2026);

-- Exemplo 4: Usar funções individuais
SELECT 
  calcular_receita_bruta('lanzi', 2026, 5) AS receita_bruta,
  calcular_receita_liquida('lanzi', 2026, 5) AS receita_liquida,
  calcular_margem_percentual('lanzi', 2026, 5) AS margem_pct;
```

## Troubleshooting

### Erro: "function does not exist"
- Verificar se o script foi executado no banco correto
- Executar `\df` para listar funções disponíveis

### Erro: "permission denied"
- Verificar se o usuário postgres tem permissões
- Executar como superuser: `psql -U postgres -d Lanzi`

### Erro: "syntax error"
- Verificar se o arquivo SQL foi copiado corretamente
- Tentar executar linha por linha no psql interativo

## Próximos Passos

Após atualizar as funções:

1. **Atualizar API** — Usar as novas funções nas queries
2. **Testar Dashboard** — Verificar se KPIs carregam corretamente
3. **Rodar Scripts S&OP** — Popular tabelas de curva_abc, ponto_pedido, etc
