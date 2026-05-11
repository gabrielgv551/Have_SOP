# Have Gestor — Sistema de Gestão Empresarial Integrada

> Plataforma web de gestão inteligente para e-commerce B2C, com módulos financeiros, operacionais e análise por IA.

---

## 🏢 Visão Geral do Projeto

O **Have Gestor** é uma plataforma SaaS multi-tenant que centraliza em um único sistema todas as informações críticas de um negócio de e-commerce: fluxo de caixa, demonstrativos financeiros, gestão de estoque, S&OP, análise de margens e inteligência artificial integrada.

**Stack tecnológica:**
- **Frontend:** SPA em HTML/CSS/JS (hospedado na Netlify)
- **Backend:** Node.js serverless na Vercel
- **Banco de dados:** PostgreSQL (isolamento por empresa)
- **Autenticação:** JWT + bcrypt
- **IA:** Google Gemini 2.0 Flash + Gemini 2.5 Pro via OpenRouter
- **Open Finance:** Belvo / Pluggy

**Modelo de acesso:** multi-empresa, com 3 perfis de usuário — `admin`, `gestor` e `have` — e controle granular de quais módulos cada usuário pode visualizar.

---

## 📦 Módulos do Sistema

---

### 1. 💰 Módulo Caixa

Gestão completa do fluxo de caixa e movimentações bancárias da empresa.

#### Funcionalidades

| Painel | Descrição |
|---|---|
| **Base de Dados** | Importação e visualização dos extratos bancários brutos (lançamentos manuais ou via Open Finance) |
| **Fluxo Diário** | Demonstrativo de caixa projetado/realizado dia a dia, com navegação por mês, cabeçalhos fixos e scrollbar horizontal persistente |
| **De-Para** | Dicionário de mapeamento automático: palavra-chave da descrição bancária → categoria gerencial do DFC |
| **Open Finance** | Conexão e sincronização automática de contas bancárias via Belvo/Pluggy |
| **Contas a Pagar** | Controle de obrigações financeiras e vencimentos |
| **Pedidos de Compra** | Gestão de ordens de compra com datas de emissão, lead time e frequência de reposição por fornecedor |
| **Configurações** | Modelo gerencial de caixa — criação e hierarquia de categorias customizáveis |
| **Vendas** | Análise de receitas por canal de venda |
| **Cenários** | Simulações e projeções de fluxo de caixa |

#### Principais indicadores calculados
- Saldo atual por conta
- Burn rate médio (últimos 3 meses)
- Dias estimados de caixa (`saldo / burn_rate_diário`)
- Entradas vs. saídas por categoria

---

### 2. 📊 Módulo DFS — Demonstrativos Financeiros

Gestão contábil e geração automática de demonstrativos financeiros a partir do razão contábil mensal.

#### Funcionalidades

| Painel | Descrição |
|---|---|
| **Base de dados** | Upload e gestão do razão contábil mensal (`dfs_balanco`) — saldo anterior, débitos, créditos, saldo atual por conta |
| **Balanço Patrimonial** | Geração automática do BP a partir da estrutura de contas configurada |
| **DRE** | Demonstração de Resultado do Exercício com hierarquia de contas customizável |
| **Fluxo de Caixa Indireto** | Demonstrativo de fluxo de caixa pelo método indireto |
| **KPIs** | Indicadores financeiros calculados: liquidez corrente, margem bruta, EBITDA, etc. |
| **Plano de Contas** | Configuração da estrutura e mapeamento das contas contábeis para os demonstrativos |

#### Principais indicadores calculados
- Liquidez corrente (`ativo_circulante / passivo_circulante`)
- Margem bruta e EBITDA
- Variação mês a mês de receita e custos
- Posição patrimonial (Ativo × Passivo × PL)

---

### 3. 📦 Módulo S&OP — Sales & Operations Planning

Gestão integrada de estoque, reposição, forecast e performance operacional.

#### Funcionalidades

| Painel | Descrição |
|---|---|
| **Ponto de Pedido** | Tabela S&OP com status por SKU (OK / ATENÇÃO / CRÍTICO), estoque atual, média mensal, dias de cobertura, quantidade sugerida de compra e custo estimado |
| **PPR · Performance** | Plano de Produção/Reposição — acompanhamento de performance de compras e reposição por SKU |
| **Forecast 12M** | Previsão de demanda para os próximos 12 meses por produto |
| **PMV · Price Volume Mix** | Análise de variação de receita por preço, volume e mix de produtos |
| **Controle de Estoques** | Visão consolidada por depósito/origem — `full_1`, `full_2` e estoque consolidado |
| **SKUs Desativadas** | Gestão de produtos descontinuados — removidos das análises S&OP |

#### Principais indicadores calculados
- Dias de estoque: `(estoque_atual × 30) / média_mensal`
- Status de ruptura: RUPTURA / RUPTURA_IMINENTE / ABAIXO_META_30D / RISCO_ENCALHE / OK
- Tendência de vendas: variação média 3M vs. 6M (`%`)
- Curva ABC (classificação A/B/C por volume/receita)
- Quantidade sugerida de compra com custo estimado

---

### 4. 📈 Módulo Margens

Análise de rentabilidade gerencial da operação.

#### Funcionalidades

| Painel | Descrição |
|---|---|
| **DRE Gerencial** | Demonstração de resultado gerencial com visão de margens por produto, canal e período |

#### Principais indicadores
- Margem de contribuição por SKU e canal
- Receita bruta vs. líquida
- Custo total e variação mês a mês
- Ticket médio por canal de venda

---

### 5. 🤖 Módulo IA — Análise Integrada por Inteligência Artificial

Sistema de IA com **5 agentes especializados** e um **orquestrador** que responde perguntas em linguagem natural cruzando todos os módulos do sistema.

#### Agentes especializados

| Agente | Domínio | Tabelas consultadas |
|---|---|---|
| **S&OP** | Estoque e reposição | `ponto_pedido`, `estoque_seguranca`, `curva_abc`, `bd_vendas`, `full_1/2` |
| **Estoque** | Inventário e cobertura | `ponto_pedido`, `estoque_seguranca`, `curva_abc`, `bd_vendas` |
| **Financeiro** | DRE e Balanço | `dfs_balanco` |
| **Vendas** | Performance comercial | `bd_vendas`, `curva_abc`, `estoque_seguranca` |
| **Caixa** | Fluxo de caixa | `caixa_extrato`, `caixa_categorias`, `caixa_de_para` |

#### Orquestrador (Análise IA Integrada)

Fluxo de execução em 3 etapas:

```
1. Pergunta do usuário
       ↓
2. Gemini Flash (roteamento)
   → identifica quais agentes acionar
       ↓
3. Agentes selecionados buscam dados em paralelo
       ↓
4. Gemini 2.5 Pro (síntese)
   → análise cross-módulo como CFO + S&OP Director + Controller
```

#### Exemplos de perguntas suportadas
- *"Tenho dinheiro para repor o estoque crítico?"* → aciona S&OP + Financeiro + Caixa
- *"Os SKUs em ruptura são os mais rentáveis?"* → aciona S&OP + Vendas + Estoque
- *"O burn rate está sendo alimentado por encalhe?"* → aciona Estoque + Caixa + Financeiro
- *"Saúde geral do negócio"* → aciona todos os 5 agentes

---

### 6. ⚙️ Módulo Admin

Gerenciamento de usuários e permissões de acesso ao sistema.

#### Funcionalidades

| Função | Descrição |
|---|---|
| **Criar usuário** | Cadastro com nome, login, senha (bcrypt), perfil e permissões de módulos |
| **Editar usuário** | Atualização de dados, perfil, status ativo/inativo |
| **Resetar senha** | Reset de senha por administrador |
| **Desativar usuário** | Soft delete — mantém histórico |
| **Permissões de sidebar** | Controle de quais seções do menu cada usuário pode visualizar (Caixa, DFS, S&OP, Margens, IA) |

#### Perfis de acesso
- **Admin** — acesso total, gerencia usuários, vê todos os módulos
- **Gestor** — acesso operacional conforme permissões configuradas
- **Have** — perfil interno da empresa Have

---

## 🗄️ Base de Dados — Tabelas Principais

| Tabela | Módulo | Descrição |
|---|---|---|
| `usuarios` | Admin | Autenticação e perfis de acesso |
| `caixa_extrato` | Caixa | Transações bancárias (Open Finance + manual) |
| `caixa_categorias` | Caixa | Hierarquia de categorias do DFC |
| `caixa_de_para` | Caixa | Mapeamento descrição → categoria |
| `belvo_links` | Caixa | Conexões bancárias ativas |
| `dfs_balanco` | DFS | Razão contábil mensal |
| `dfs_estrutura` | DFS | Estrutura e mapeamento dos demonstrativos |
| `dfs_fluxo_caixa_diario` | DFS | Fluxo de caixa diário projetado |
| `bd_vendas` | S&OP / Margens | Base histórica de vendas por item de pedido |
| `ponto_pedido` | S&OP | Parâmetros de reposição por SKU |
| `estoque_seguranca` | S&OP | Média mensal de consumo por SKU |
| `curva_abc` | S&OP | Classificação A/B/C dos SKUs |
| `ppr_sku` | S&OP | Plano de produção/reposição |
| `forecast_12m` | S&OP | Previsão de demanda 12 meses |
| `sku_desativadas` | S&OP | SKUs descontinuados |

---

## 🔐 Segurança e Autenticação

- Autenticação por **JWT** com expiração de 8 horas
- Senhas com hash **bcrypt** (salt 10)
- Isolamento de dados por empresa (`empresa` em todas as tabelas)
- Permissões de módulos armazenadas no token JWT (`nav_permissoes`)
- Perfil `admin` tem acesso irrestrito; demais perfis seguem permissões configuradas

---

## 🚀 Infraestrutura e Deploy

| Componente | Tecnologia | Plataforma |
|---|---|---|
| Frontend | HTML / CSS / JavaScript | Netlify |
| Backend API | Node.js Serverless | Vercel |
| Banco de Dados | PostgreSQL | Servidor dedicado |
| IA | Gemini 2.0 Flash + 2.5 Pro | OpenRouter API |
| Open Finance | Belvo / Pluggy | API externa |

---

## 📌 Diferenciais do Projeto

1. **Visão 360°** — único sistema que integra caixa, estoque, financeiro e vendas
2. **IA Orquestrada** — análise cruzada de módulos em linguagem natural com modelos Gemini
3. **Open Finance nativo** — sincronização automática de extratos bancários
4. **Multi-tenant** — arquitetura preparada para múltiplas empresas
5. **Permissões granulares** — controle de acesso por módulo e por usuário
6. **Sem dependência de ERP** — dados importados via planilha ou API, sem integração obrigatória

---

*Have Consultoria — Sistema desenvolvido e mantido pela equipe Have*
