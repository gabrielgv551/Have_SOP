# Instruções de Deploy — Correção Dashboard

## Status da Correção

✅ **Query `dashboard_kpis` foi simplificada e corrigida**

Arquivo modificado: `@/have-gestor-api/lib/data-sopc.js:255-293`

## Como Fazer Deploy

### Opção 1: Vercel CLI (Recomendado)

```powershell
cd "c:\Users\HAVE\Desktop\Arquivos\Have I\have-gestor-api"

# Instalar Vercel CLI (se não tiver)
npm install -g vercel

# Fazer deploy
vercel --prod
```

### Opção 2: Git Push (Se configurado com CI/CD)

```powershell
cd "c:\Users\HAVE\Desktop\Arquivos\Have I\have-gestor-api"
git add -A
git commit -m "fix: simplify dashboard_kpis query to return data from bd_vendas"
git push origin main
```

### Opção 3: Netlify CLI

```powershell
cd "c:\Users\HAVE\Desktop\Arquivos\Have I\have-gestor-api"

# Instalar Netlify CLI (se não tiver)
npm install -g netlify-cli

# Fazer deploy
netlify deploy --prod
```

## Após o Deploy

1. **Aguardar build completar** (~2-5 minutos)

2. **Testar a API:**
   ```
   GET https://have-gestor-api.vercel.app/api/data?tabela=dashboard_kpis
   ```
   (Com header `Authorization: Bearer <seu_token_jwt>`)

3. **Recarregar o dashboard** em `https://have-gestor-frontend.vercel.app`

## Próximo Passo: Popular Tabelas S&OP

Após o deploy estar online, execute os scripts Python para popular as tabelas S&OP:

### Para Lanzi:
```powershell
cd "c:\Users\HAVE\Desktop\Arquivos\Have I\Lanzi\sopc"

# 1. Previsão de demanda (12 meses)
python "PREVISÃO 12M.py"

# 2. Estoque de segurança
python Estoque_Seguranca.py

# 3. Ponto de pedido
python Ponto_Pedido.py

# 4. Curva ABC (opcional)
python Curva_ABC.PY
```

### Para Supershop:
```powershell
cd "c:\Users\HAVE\Desktop\Arquivos\Have I\Supershop\sopc"

# Executar os mesmos scripts
python "PREVISÃO 12M.py"
python Estoque_Seguranca.py
python Ponto_Pedido.py
```

### Para Marcon:
```powershell
cd "c:\Users\HAVE\Desktop\Arquivos\Have I\Marcon\sopc"

# Executar os mesmos scripts
python "PREVISÃO 12M.py"
python Estoque_Seguranca.py
python Ponto_Pedido.py
```

## Verificação Final

Após rodar os scripts Python, os dashboards devem mostrar:

- ✅ Cards de KPIs (Receita Bruta, Receita Líquida, Margem, Ticket Médio)
- ✅ Gráfico de Receita Mensal com linha de Margem %
- ✅ Tabela de Curva ABC com distribuição de SKUs
- ✅ Tabela de Ponto de Pedido com alertas
- ✅ Filtros funcionando (Mês, Ano, Marca, Canal)

## Troubleshooting

### Dashboard ainda vazio após deploy?

1. **Verificar se `bd_vendas` tem dados:**
   ```sql
   SELECT COUNT(*) FROM bd_vendas;
   ```

2. **Verificar se scripts S&OP foram executados:**
   ```sql
   SELECT COUNT(*) FROM curva_abc;
   SELECT COUNT(*) FROM ponto_pedido;
   SELECT COUNT(*) FROM estoque_seguranca;
   ```

3. **Verificar logs da API:**
   - Vercel: https://vercel.com/dashboard
   - Netlify: https://app.netlify.com

### Query retorna erro 500?

Verificar se coluna `"Data"` existe em `bd_vendas`:
```sql
SELECT column_name FROM information_schema.columns 
WHERE table_name='bd_vendas' AND column_name='Data';
```

Se não existir, usar coluna correta (pode ser `"data"`, `"Data Pedido"`, etc).

## Arquivos Modificados

- `@/have-gestor-api/lib/data-sopc.js` — Query `dashboard_kpis` simplificada
- `@/have-gestor-api/netlify.toml` — Arquivo de configuração criado

## Documentação Completa

Ver: `@/DASHBOARD_FIX_SUMMARY.md`
