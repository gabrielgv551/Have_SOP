# Schema Completo do Banco de Dados — Have Gestor

## Contexto do Projeto

Sistema de gestão inteligente para e-commerce B2C. Stack: Node.js (Vercel Serverless) + PostgreSQL.
Multi-tenant: toda tabela tem coluna `empresa` (ex: `'lanzi'`) para isolamento de dados.
Valores monetários em `dfs_balanco` são NUMERIC(18,2). Valores em `caixa_extrato` e `dfs_fluxo_caixa_diario` são INTEGER em centavos (dividir por 100 para reais).

---

## TABELAS DE GESTÃO DO SISTEMA

### `usuarios`
Gerenciamento de usuários com autenticação JWT + bcrypt.

| Coluna | Tipo | Descrição |
|---|---|---|
| `id` | SERIAL PK | Identificador único |
| `empresa` | VARCHAR(50) | Tenant (ex: `'lanzi'`) |
| `nome` | VARCHAR(255) | Nome completo do usuário |
| `usuario` | VARCHAR(100) | Login do usuário |
| `senha_hash` | VARCHAR(255) | Hash bcrypt da senha |
| `perfil` | VARCHAR(50) | Nível de acesso: `'admin'`, `'gestor'`, `'have'` |
| `ativo` | BOOLEAN | Se o usuário está ativo |
| `criado_em` | TIMESTAMP | Data de criação |
| `atualizado_em` | TIMESTAMP | Última atualização (auto-trigger) |

**Constraint:** `UNIQUE(empresa, usuario)`

---

## TABELAS FINANCEIRAS (DFS — Demonstrativos Financeiros)

### `dfs_balanco`
Razão contábil mensal — base para DRE e Balanço Patrimonial.
Cada linha é uma conta contábil em um mês específico.

| Coluna | Tipo | Descrição |
|---|---|---|
| `id` | SERIAL PK | Identificador único |
| `empresa` | VARCHAR(50) | Tenant |
| `ano` | INTEGER | Ano de referência (ex: 2025) |
| `mes` | INTEGER | Mês de referência (1–12) |
| `conta` | VARCHAR(100) | Código da conta contábil (ex: `'1.1.01'`) |
| `nome` | VARCHAR(255) | Nome da conta (ex: `'Caixa e Equivalentes'`) |
| `saldo_anterior` | NUMERIC(18,2) | Saldo no início do período |
| `debito` | NUMERIC(18,2) | Total de débitos no período |
| `credito` | NUMERIC(18,2) | Total de créditos no período |
| `saldo_atual` | NUMERIC(18,2) | Saldo ao final do período |
| `criado_em` | TIMESTAMP | Data de inserção |
| `atualizado_em` | TIMESTAMP | Última atualização |

**Constraint:** `UNIQUE(empresa, ano, mes, conta)`
**Uso:** Agentes DRE, Balanço Patrimonial, KPIs

---

### `dfs_estrutura`
Configuração da estrutura e mapeamento das demonstrações financeiras.
Armazena em JSONB como montar DRE e Balanço a partir do `dfs_balanco`.

| Coluna | Tipo | Descrição |
|---|---|---|
| `id` | SERIAL PK | Identificador único |
| `empresa` | VARCHAR(50) | Tenant |
| `tipo` | VARCHAR(20) | `'structure'` (hierarquia do DRE/BP) ou `'mappings'` (conta → linha) |
| `dados` | JSONB | JSON com a estrutura ou mapeamentos das contas |
| `atualizado_em` | TIMESTAMP | Última atualização |

**Constraint:** `UNIQUE(empresa, tipo)`
**Uso:** Renderização do DRE e Balanço Patrimonial no frontend

---

### `dfs_fluxo_caixa_diario`
Fluxo de caixa projetado/realizado por dia. Valores em **centavos** (INTEGER).

| Coluna | Tipo | Descrição |
|---|---|---|
| `id` | SERIAL PK | Identificador único |
| `empresa` | VARCHAR(50) | Tenant |
| `ano` | INTEGER | Ano de referência |
| `mes` | INTEGER | Mês de referência (1–12) |
| `dia` | INTEGER | Dia do mês (1–31) |
| `tipo` | VARCHAR(50) | Categoria/tipo do lançamento (ex: `'Recebimento Clientes'`) |
| `valor` | INTEGER | Valor em centavos (positivo = entrada, negativo = saída) |
| `atualizado_em` | TIMESTAMP | Última atualização |

**Constraint:** `UNIQUE(empresa, ano, mes, dia, tipo)`
**Uso:** Agente Fluxo de Caixa

---

## TABELAS DE CAIXA (Open Finance / Extrato Bancário)

### `caixa_extrato`
Transações bancárias brutas importadas via Belvo (Open Finance) ou lançamento manual.
Valores em **centavos** (INTEGER).

| Coluna | Tipo | Descrição |
|---|---|---|
| `id` | SERIAL PK | Identificador único |
| `empresa` | VARCHAR(50) | Tenant |
| `ano` | INTEGER | Ano da transação |
| `mes` | INTEGER | Mês da transação (1–12) |
| `dia` | INTEGER | Dia da transação (1–31) |
| `descricao` | TEXT | Descrição original do lançamento bancário |
| `valor` | INTEGER | Valor em centavos (positivo = crédito, negativo = débito) |
| `belvo_tx_id` | VARCHAR(100) | ID único da transação no Belvo (NULL para lançamentos manuais) |
| `atualizado_em` | TIMESTAMP | Última atualização |

**Índice único:** `(empresa, belvo_tx_id)` — evita duplicatas no sync Open Finance
**Uso:** Agente Caixa/Fluxo de Caixa

---

### `caixa_categorias`
Modelo de fluxo de caixa gerencial — categorias configuráveis pelo usuário.
Define a hierarquia de linhas do demonstrativo de caixa (DFC).

| Coluna | Tipo | Descrição |
|---|---|---|
| `id` | SERIAL PK | Identificador único |
| `empresa` | VARCHAR(50) | Tenant |
| `nome` | VARCHAR(100) | Nome da categoria (ex: `'Receitas Operacionais'`) |
| `tipo` | VARCHAR(20) | `'item'` (linha de detalhe) ou `'grupo'` (agrupador/subtotal) |
| `parent` | VARCHAR(100) | Nome da categoria pai (hierarquia) |
| `ordem` | INTEGER | Posição de exibição no modelo |

**Constraint:** `UNIQUE(empresa, nome)`

---

### `caixa_de_para`
Dicionário de mapeamento: palavra-chave da descrição bancária → categoria do DFC.
Usado para categorizar automaticamente os lançamentos do `caixa_extrato`.

| Coluna | Tipo | Descrição |
|---|---|---|
| `id` | SERIAL PK | Identificador único |
| `empresa` | VARCHAR(50) | Tenant |
| `palavra_chave` | TEXT | Texto que aparece na descrição bancária (ex: `'MERCADO LIVRE'`) |
| `categoria_nome` | VARCHAR(100) | Nome da categoria destino (FK lógica para `caixa_categorias.nome`) |

**Constraint:** `UNIQUE(empresa, palavra_chave)`

---

### `belvo_links`
Conexões bancárias ativas via Belvo (Open Finance).
Cada registro é uma conta bancária conectada.

| Coluna | Tipo | Descrição |
|---|---|---|
| `id` | SERIAL PK | Identificador único |
| `empresa` | VARCHAR(50) | Tenant |
| `link_id` | VARCHAR(100) | ID da conexão no Belvo |
| `institution` | VARCHAR(100) | Nome do banco/instituição (ex: `'Banco do Brasil'`) |
| `account_type` | VARCHAR(50) | Tipo de conta (ex: `'CHECKING'`, `'SAVINGS'`) |
| `ultimo_sync` | TIMESTAMP | Data/hora do último sincronismo bem-sucedido |
| `ativo` | BOOLEAN | Se a conexão está ativa |
| `criado_em` | TIMESTAMP | Data de criação da conexão |

**Constraint:** `UNIQUE(empresa, link_id)`

---

## TABELAS S&OP (Importadas do ERP / Planilhas)

> **ATENÇÃO:** Estas tabelas são importadas via upload de planilha (CSV/Excel).
> Não possuem migration SQL — são criadas dinamicamente com os nomes/colunas do arquivo importado.
> Os nomes de coluna variam e o sistema usa detecção automática de colunas.

---

### `bd_vendas`
**Tabela principal de vendas.** Gerada pelo ETL Gefinance. Cada linha é um item de pedido vendido.
Colunas com aspas duplas (case-sensitive no PostgreSQL).

> **⚠️ Schema atualizado (Gefinance ETL):** `"Mes"` sem acento · `"Total Venda"` · `"Margem Produto"`

| Coluna | Tipo | Descrição |
|---|---|---|
| `"Order ID"` | BIGINT | ID do pedido (PK composta com `"Produto ID"`) |
| `"Produto ID"` | BIGINT | ID do produto |
| `"Sku"` | TEXT | Código do produto |
| `"Nome Produto"` | TEXT | Nome descritivo do produto |
| `"Categoria"` | TEXT | Categoria do produto |
| `"Data"` | DATE | Data da venda |
| `"Ano"` | INTEGER | Ano da venda |
| `"Mes"` | INTEGER | Mês da venda (1–12) — **sem acento** |
| `"Quantidade Vendida"` | NUMERIC | Quantidade de unidades vendidas |
| `"Total Venda"` | NUMERIC | Receita bruta do item nível produto (R$) |
| `"Margem Produto"` | NUMERIC | Margem de contribuição nível produto (R$) |
| `"Custo Total"` | NUMERIC | Custo total do item (R$) |
| `"Status"` | TEXT | Status do pedido; cancelados têm `'cancel'` no texto |
| `"Canal de venda"` | TEXT | Canal de venda original (ex: `'Shopee'`, `'Mercado Livre'`) |
| `"Canal Apelido"` | TEXT | Nome amigável do canal (usado em lugar de `"Canal de venda"` quando disponível) |
| `"Total Venda Pedido"` | NUMERIC | Receita total do pedido (nível pedido) |
| `"Margem Contribuicao"` | NUMERIC | Margem de contribuição nível pedido (sem acento) |

**Uso:** Todos os agentes. Base de toda análise de vendas, KPIs, S&OP, Estoque.
**Filtro padrão:** `WHERE "Status" !~* '(cancel|devol|n[aã]o.?pago)'` para excluir cancelados, devolvidos e não pagos.
**Atenção:** `curva_abc` ainda usa `"Mês"` com acento (upload CSV) — não confundir.

---

### `ponto_pedido`
Parâmetros de reposição por SKU — base do painel S&OP.

| Coluna | Tipo esperado | Descrição |
|---|---|---|
| `sku` | TEXT | Código do produto |
| `estoque_atual` | NUMERIC | Estoque disponível atual |
| `ponto_pedido` | NUMERIC | Quantidade mínima antes de gerar pedido de compra |
| `alerta` | TEXT | Status calculado: `'OK'`, `'ATENÇÃO'`, `'CRÍTICO'`, `'SEM DADOS'` |

---

### `estoque_seguranca`
Estoque de segurança e média de consumo mensal por SKU.

| Coluna | Tipo esperado | Descrição |
|---|---|---|
| `sku` | TEXT | Código do produto |
| `media_mensal` | NUMERIC | Média de vendas mensais (usado para calcular dias de cobertura) |

---

### `curva_abc`
Classificação ABC dos SKUs por receita/volume.

| Coluna | Tipo esperado | Descrição |
|---|---|---|
| `sku` (ou `"Sku"`) | TEXT | Código do produto |
| `curva` (ou `"Curva"`) | TEXT | Classificação: `'A'`, `'B'`, `'C'` |
| `"Ano"` | INTEGER | Ano da classificação |
| `"Mês"` | INTEGER | Mês da classificação |

---

### `full_1` e `full_2`
Tabelas de estoque físico — representam dois depósitos/locais de armazenagem.
Colunas detectadas automaticamente (sistema busca coluna com `'sku'` e coluna com `'estoque'`).

| Coluna detectada | Tipo | Descrição |
|---|---|---|
| `SKU` ou `sku` | TEXT | Código do produto |
| `Estoque Base` ou similar | NUMERIC | Quantidade em estoque no depósito |

---

### `estoque_consolidado`
Visão consolidada do estoque por SKU e origem (depósito/localização).
Colunas detectadas automaticamente.

| Coluna detectada | Tipo | Descrição |
|---|---|---|
| `SKU` | TEXT | Código do produto |
| `Origem` | TEXT | Nome do depósito/origem (ex: `'Depósito SP'`, `'CD RJ'`) |
| `Estoque Base` | NUMERIC | Quantidade disponível nessa origem |

---

### `ppr_sku`
Plano de Produção/Reposição por SKU. Contém quantidades planejadas de compra/produção.

---

### `forecast_12m`
Previsão de demanda para os próximos 12 meses por SKU.

---

### `semana_pedidos`
Pedidos de compra em aberto por semana — para acompanhamento de lead time.

---

### `cadastros_sku`
Cadastro mestre de SKUs ativos. Usado para identificar produtos descontinuados
(SKUs presentes aqui mas sem venda nos últimos 6 meses).

---

## TABELAS AUXILIARES

### `sku_desativadas`
SKUs marcados manualmente como inativos — excluídos das análises S&OP.

| Coluna | Tipo | Descrição |
|---|---|---|
| `id` | SERIAL PK | Identificador único |
| `empresa` | VARCHAR(50) | Tenant |
| `sku` | TEXT | Código do produto desativado |
| `criado_em` | TIMESTAMP | Data de desativação |

**Constraint:** `UNIQUE(empresa, sku)`

---

## VIEWS / QUERIES CALCULADAS (não são tabelas físicas)

### `sopc` (view virtual, montada em runtime no `api/data.js`)
Consolidação S&OP — resultado de 5 queries paralelas cruzadas:
- `ponto_pedido` → estoque atual + ponto de pedido + alerta
- `estoque_seguranca` → média mensal por SKU
- `bd_vendas` (últimos 3m) → média de vendas por canal
- `full_1` + `full_2` → estoque físico total
- `estoque_consolidado` → breakdown por origem/depósito

**Resultado por SKU:**
```json
{
  "sku": "SKU-001",
  "alerta_pp": "CRÍTICO",
  "estoque_base": 150,
  "estoque_full": 220,
  "media_mensal": 80.5,
  "ponto_pedido": 100,
  "canais": { "Shopee": 45.2, "Mercado Livre": 35.3 },
  "origens": { "Depósito SP": 150, "CD RJ": 70 }
}
```

### `sku_atividade` (query calculada)
Vendas por SKU em janelas de 1m, 3m, 6m e 12m — para identificar SKUs mortos.

### `dashboard_kpis` (query calculada)
KPIs do mês mais recente da `bd_vendas`:
- `receita_bruta`, `receita_liquida`, `qtd_liquida`, `margem_bruta`, `custo_total`

### `monthly_revenue` (query calculada)
Receita e quantidade mensal histórica — série temporal completa.

---

## CAMPOS PRÉ-CALCULADOS pelos Agentes IA (em `api/agents.js`)

Os agentes calculam campos enriquecidos antes de enviar ao modelo:

| Campo | Fórmula | Usado por |
|---|---|---|
| `dias_cobertura` | `estoque / (media_mensal / 30)` | S&OP, Estoque |
| `status` | RUPTURA / RUPTURA_IMINENTE / ABAIXO_META_30D / RISCO_ENCALHE / OK | S&OP |
| `tendencia_pct` | `((media_3m - media_6m) / media_6m) * 100` | S&OP, Vendas |
| `variacao_mom` | `((receita_atual - receita_anterior) / receita_anterior) * 100` | Vendas |
| `margem_pct` | `(margem / receita) * 100` | Vendas, Financeiro |
| `ticket_medio` | `receita / quantidade` | Vendas |
| `burn_rate_medio` | Média de saídas dos últimos 3 meses | Caixa |
| `dias_caixa_estimados` | `saldo_atual / burn_rate_diario` | Caixa |
| `liquidez_corrente` | `ativo_circulante / passivo_circulante` | Financeiro |

---

## ARQUITETURA DE AGENTES ATUAL (api/agents.js)

5 agentes implementados, cada um com:
- `systemPrompt`: prompt de sistema com regras do negócio
- `autoPrompt`: instrução CoT (chain-of-thought) para o modelo
- `fetchData(pool, empresa)`: busca e pré-calcula dados do PostgreSQL

| Agente | Domínio | Tabelas consultadas |
|---|---|---|
| `sopc` | S&OP | `ponto_pedido`, `estoque_seguranca`, `curva_abc`, `bd_vendas`, `full_1`, `full_2` |
| `estoque` | Inventário | `ponto_pedido`, `estoque_seguranca`, `curva_abc`, `bd_vendas` |
| `financeiro` | DRE/BP | `dfs_balanco` |
| `vendas` | Vendas | `bd_vendas`, `curva_abc`, `estoque_seguranca` |
| `caixa` | Fluxo de Caixa | `caixa_extrato`, `caixa_categorias`, `caixa_de_para` |

---

## ARQUITETURA ALVO (multi-agente com orquestrador)

### Agentes Temáticos (dados brutos)
| Agente | Tabelas principais |
|---|---|
| DRE | `dfs_balanco`, `dfs_estrutura` |
| Balanço Patrimonial | `dfs_balanco`, `dfs_estrutura` |
| S&OP | `ponto_pedido`, `estoque_seguranca`, `curva_abc`, `bd_vendas`, `full_1`, `full_2` |
| Estoque | `ponto_pedido`, `estoque_seguranca`, `curva_abc`, `full_1`, `full_2`, `estoque_consolidado` |

### Agentes Analíticos (síntese)
| Agente | Tabelas principais |
|---|---|
| Compras | `semana_pedidos`, `ppr_sku`, `ponto_pedido` |
| Fluxo de Caixa | `caixa_extrato`, `dfs_fluxo_caixa_diario`, `caixa_categorias`, `caixa_de_para` |
| KPIs | `bd_vendas`, `dfs_balanco`, `caixa_extrato` |
| Alertas | Todas as tabelas — roda em background |

### Orquestrador
- Recebe pergunta em linguagem natural
- Decide quais agentes acionar (1 a N em paralelo)
- Consolida as respostas em uma análise unificada

---

## VARIÁVEIS DE AMBIENTE

```
JWT_SECRET              — chave para assinar/verificar tokens JWT
LANZI_HOST              — host do PostgreSQL (empresa Lanzi)
LANZI_PORT              — porta (default 5432)
LANZI_DB                — nome do banco
LANZI_USER              — usuário PostgreSQL
LANZI_PASSWORD          — senha PostgreSQL
OPENROUTER_API_KEY      — chave da API OpenRouter (modelos LLM)
PLUGGY_CLIENT_ID        — Open Finance (Pluggy)
PLUGGY_CLIENT_SECRET    — Open Finance (Pluggy)
```
