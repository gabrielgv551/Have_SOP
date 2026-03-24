# Deployment Guide: Have Gestor Inteligente

## 🏗️ Arquitetura

```
┌──────────────────┐
│  Frontend        │  (Gestor Have/index.html)
│  (Vercel ou GitHub Pages)
└────────┬─────────┘
         │ JWT Token + API Calls
┌────────▼──────────────────────┐
│  Backend API (Vercel)         │  (have-gestor-api/)
│  • /api/login                 │
│  • /api/data?tabela=...       │
└────────┬──────────────────────┘
         │ SQL Queries (secured)
┌────────▼──────────────────────┐
│  PostgreSQL Database          │
│  (37.60.236.200 ou outra)     │
└───────────────────────────────┘
```

---

## 📋 Checklist de Deployment

### **1. Deploy da API no Vercel**

#### 1.1 Preparar o repositório
```bash
cd have-gestor-api
npm install  # Instala jsonwebtoken e pg
```

#### 1.2 Conectar ao Vercel
```bash
vercel login
vercel link  # Link ao projeto Vercel existente
```

#### 1.3 Adicionar Environment Variables no Painel Vercel

**Settings → Environment Variables:**

```
# JWT
JWT_SECRET=sua_string_aleatoria_muito_segura_aqui

# Lanzi Database
LANZI_HOST=37.60.236.200
LANZI_PORT=5432
LANZI_DB=Lanzi
LANZI_USER=postgres
LANZI_PASSWORD=131105Gv
LANZI_PASS_ADMIN=lanzi2024
LANZI_PASS_GESTOR=have2024
LANZI_PASS_HAVE=lanzi@2024

# Exemplo: Para adicionar Empresa 2
# EMP2_HOST=xxx.xxx.xxx.xxx
# EMP2_PORT=5432
# EMP2_DB=empresa2_db
# EMP2_USER=user
# EMP2_PASSWORD=pass
# EMP2_PASS_ADMIN=admin123
```

#### 1.4 Deploy
```bash
vercel deploy --prod
```

Resultado: `https://have-gestor-api.vercel.app` estará online.

---

### **2. Deploy do Frontend**

#### 2.1 Opção A: Vercel (Recomendado)
```bash
cd "Gestor Have"
vercel deploy --prod
```

Resultado: Frontend em `https://have-gestor.vercel.app`

#### 2.2 Opção B: GitHub Pages
1. Push `Gestor Have/index.html` para um repositório
2. Enable GitHub Pages nas settings
3. Resultado: `https://username.github.io/repo`

#### 2.3 Opção C: Servidor Estático Qualquer
- Copie `Gestor Have/index.html` para um servidor web (Nginx, Apache, etc)
- O arquivo se conecta automaticamente a `https://have-gestor-api.vercel.app`

---

### **3. Fluxo de Login e Autenticação**

#### Frontend (index.html):
```javascript
// 1. Usuário digita credenciais
// 2. POST para /api/login
const res = await fetch('https://have-gestor-api.vercel.app/api/login', {
  method: 'POST',
  body: JSON.stringify({
    company: 'lanzi',      // ou 'empresa2', 'empresa3', etc
    user: 'admin',
    password: 'lanzi2024'
  })
});

// 3. API retorna JWT token de 8h
const { token, companyName } = await res.json();
localStorage.setItem('have_token', token);

// 4. Cada query usa o token
fetch('https://have-gestor-api.vercel.app/api/data?tabela=curva_abc', {
  headers: { 'Authorization': `Bearer ${token}` }
})
```

#### Backend (api/login.js):
```javascript
// 1. Recebe company, user, password
// 2. Valida contra companies.js e env vars
// 3. Se OK, gera JWT com process.env.JWT_SECRET
// 4. Retorna token
```

#### Backend (api/data.js):
```javascript
// 1. Recebe tabela + token
// 2. Valida token
// 3. Conecta ao banco da empresa correta (via env vars)
// 4. Query SELECT * FROM tabela
// 5. Retorna JSON
```

---

## 🔐 Segurança

### ✅ O que está protegido:
- **Senhas do banco** nunca são enviadas ao frontend (apenas env vars no Vercel)
- **Senhas de usuários** são validadas no servidor
- **Tokens JWT** expiram em 8 horas
- **SQL Injection** bloqueado com whitelist de tabelas
- **CORS** aberto para demo (ajuste em produção)

### ⚠️ Para Produção:
1. Use `JWT_SECRET` forte (32+ caracteres aleatórios)
2. Ative SSL no PostgreSQL (`ssl: true`)
3. Restrinja CORS origin em `api/login.js` e `api/data.js`
4. Implemente refresh token para sessões longas

---

## 🆕 Adicionar Nova Empresa

### 1. Preparar credenciais do banco
- IP/Host
- Porta (padrão 5432)
- Database name
- User
- Password

### 2. No Vercel → Environment Variables:
```
EMP2_HOST=xxx.xxx.xxx.xxx
EMP2_PORT=5432
EMP2_DB=empresa2
EMP2_USER=postgres
EMP2_PASSWORD=senhadodb
EMP2_PASS_ADMIN=admin123
EMP2_PASS_GESTOR=gestor123
```

### 3. Em `lib/companies.js`:
```javascript
module.exports = {
  lanzi: { ... },
  empresa2: {
    name: "Empresa 2",
    dbEnvKey: "EMP2",
    users: {
      admin: process.env.EMP2_PASS_ADMIN,
      gestor: process.env.EMP2_PASS_GESTOR,
    }
  }
};
```

### 4. Em `api/data.js` (se tabelas diferentes):
```javascript
const TABELAS_PERMITIDAS = [
  'curva_abc',
  'ponto_pedido',
  // ... adicione tabelas de EMP2 se diferentes
];
```

### 5. Deploy:
```bash
git push  # Vercel redeploy automático
```

---

## 🐛 Troubleshooting

### "401 Token inválido"
→ Token expirou (8h). Faça login novamente.

### "401 Usuário ou senha incorretos"
→ Verifique:
  - `company` existe em `lib/companies.js`
  - `user` existe em `companies[company].users`
  - `COMPANY_PASS_USER` está correto nas env vars do Vercel

### "Tabela 'X' não permitida"
→ A tabela não está em `TABELAS_PERMITIDAS` em `api/data.js`

### "Error connecting to database"
→ Verifique:
  - `COMPANY_HOST` é acessível do Vercel
  - `COMPANY_USER` e `COMPANY_PASSWORD` estão corretos
  - PostgreSQL não tem firewall bloqueando

### "CORS error"
→ Frontend e API estão em origens diferentes (normal). Verifique se `api/data.js` tem:
```javascript
res.setHeader('Access-Control-Allow-Origin', '*');
```

---

## 📚 Referência Rápida

| O que | Onde |
|-------|------|
| Frontend | Vercel ou GitHub Pages |
| API Backend | Vercel Serverless (`have-gestor-api`) |
| Database | PostgreSQL (37.60.236.200:5432 ou outra) |
| Credenciais | Env vars do Vercel (NUNCA no código) |
| Login | POST `/api/login` → JWT token |
| Queries | GET `/api/data?tabela=X` + Bearer token |
| Usuários | `lib/companies.js` + env vars |
| Tabelas | Whitelist em `api/data.js` |

---

## 🚀 Após Deployment

1. **Teste login** em `https://seu-frontend.vercel.app`
2. **Teste query** com curl:
   ```bash
   # 1. Login
   TOKEN=$(curl -s -X POST https://have-gestor-api.vercel.app/api/login \
     -H "Content-Type: application/json" \
     -d '{"company":"lanzi","user":"admin","password":"lanzi2024"}' \
     | jq -r '.token')

   # 2. Query
   curl https://have-gestor-api.vercel.app/api/data?tabela=curva_abc \
     -H "Authorization: Bearer $TOKEN"
   ```

3. **Monitore logs** no Vercel dashboard

---

**Última atualização:** 2026-03-24
