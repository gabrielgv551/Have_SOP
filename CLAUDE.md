# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Architecture Overview

**Have Gestor Inteligente** is a three-tier intelligent inventory/supply chain management platform:

```
┌─────────────┐
│   Browser   │ (Frontend: index.html)
│  (Gestor    │
│   Have/)    │
└──────┬──────┘
       │ POST /api/login (JWT token)
       │ GET /api/data?tabela=X (with Bearer token)
       │
┌──────▼──────────────────┐
│  API Backend (Vercel)   │ (have-gestor-api/)
│  • api/login.js         │ ← JWT generation
│  • api/data.js          │ ← Query dispatch with whitelist
│  • lib/companies.js     │ ← Multi-tenant config
└──────┬──────────────────┘
       │ Credentials from env vars
       │
┌──────▼──────────────────────────────────────┐
│  PostgreSQL (Multiple Databases)            │
│  • lanzi.example.com (Lanzi company)        │
│  • (extensible to many companies)           │
│  Tables: curva_abc, ponto_pedido, etc.      │
└─────────────────────────────────────────────┘
```

**Three directory groups:**

| Directory | Purpose | Tech |
|-----------|---------|------|
| `Gestor Have/` | Frontend + local Flask server | HTML/JS/CSS + Flask (app.py, server.py) |
| `have-gestor-api/` | Cloud backend (Vercel serverless) | Node.js (Express-style via Vercel) |
| `Lanzi/` | ETL pipeline & analytics | Python (psycopg2, pandas) |

## Key Security & Design Patterns

### 1. **Multi-Tenant with Environment Variables**
- Each company gets a `DB_*` env var prefix (e.g., `LANZI_HOST`, `LANZI_USER`, `LANZI_PASSWORD`)
- User credentials for each company are also env vars (e.g., `LANZI_PASS_ADMIN`, `LANZI_PASS_GESTOR`)
- `lib/companies.js` maps company slugs (e.g., "lanzi") to their config—**never hardcode secrets**

### 2. **JWT Token Flow**
- **Login endpoint** (`api/login.js`): validates company + user + password, returns 8h JWT token
- **Data endpoint** (`api/data.js`): validates Bearer token, grants access to whitelisted tables only
- Token payload includes `{ company, user, companyName }`

### 3. **Whitelist Security**
- `api/data.js` has a hardcoded `TABELAS_PERMITIDAS` array (SQL injection prevention)
- Only SELECT queries on whitelisted tables are allowed
- Limit set to 5000 rows per request

### 4. **Connection Pooling**
- `api/data.js` caches `pg.Pool` instances per company (avoids overhead on each request)
- Max 5 connections per pool

## Common Development Tasks

### **Running the Frontend Locally**
```bash
# Option 1: Flask server (app.py)
cd "Gestor Have"
pip install flask psycopg2-binary
python app.py
# Runs on http://localhost:8080, serves index.html + /query endpoint

# Option 2: HTTP server (server.py)
python server.py
# Runs on http://localhost:8787, connects to 37.60.236.200:5432
```

### **Running the Vercel API Locally**
```bash
cd have-gestor-api
npm install
# For local testing, set env vars:
export JWT_SECRET="test_secret"
export LANZI_HOST="37.60.236.200"
export LANZI_PORT="5432"
export LANZI_DB="Lanzi"
export LANZI_USER="postgres"
export LANZI_PASSWORD="<see production>"
export LANZI_PASS_ADMIN="<see production>"
export LANZI_PASS_GESTOR="<see production>"
export LANZI_PASS_HAVE="<see production>"

# Use Vercel CLI to test endpoints
vercel dev  # Local emulation of Vercel functions
```

### **Testing API Endpoints**
```bash
# Login
curl -X POST http://localhost:3000/api/login \
  -H "Content-Type: application/json" \
  -d '{"company":"lanzi","user":"admin","password":"lanzi2024"}'
# Returns: { token: "...", companyName: "Lanzi", user: "admin" }

# Query data (replace TOKEN with real token)
curl http://localhost:3000/api/data?tabela=curva_abc \
  -H "Authorization: Bearer TOKEN"
```

### **Adding a New Company**
1. **In `lib/companies.js`:** uncomment the template, fill in company slug, name, and dbEnvKey
2. **In Vercel Settings → Environment Variables:** add `COMPANY_HOST`, `COMPANY_PORT`, `COMPANY_DB`, `COMPANY_USER`, `COMPANY_PASSWORD`, and user password vars
3. **In `api/data.js`:** if new tables needed, add them to `TABELAS_PERMITIDAS`

### **Running the ETL Pipeline (Lanzi)**
```bash
cd Lanzi
pip install psycopg2 pandas openpyxl

# Run all scripts in order
python Rodar_Todos.py
# This orchestrates: UPLOAD_ETL → PREVISÃO 12M → Curva_ABC → Estoque_Seguranca → Ponto_Pedido → PPR_SKU
```

Individual scripts:
- `UPLOAD_ETL.py` — loads Excel data into PostgreSQL
- `PREVISÃO 12M.py` — generates 12-month demand forecast
- `Curva_ABC.PY` — calculates ABC inventory curve
- `Estoque_Seguranca.py` — computes safety stock per SKU
- `Ponto_Pedido.py` — calculates reorder points + weekly order list
- `PPR_SKU.py` — sales performance by temporal window

## Database Connection Details

**Lanzi Database (Production)**
- Host: `37.60.236.200` (port 5432)
- Database: `Lanzi`
- User: `postgres`
- Tables exposed via API: `curva_abc`, `ponto_pedido`, `estoque_seguranca`, `ppr_sku`, `forecast_12m`, `semana_pedidos`

## Important Patterns & Gotchas

### **Flask vs HTTP Server**
- `app.py` (Flask) is cleaner, supports standard REST patterns
- `server.py` (HTTPServer) is lower-level; prefer Flask for new features
- Both serve `index.html` and relay `/query` POST requests to PostgreSQL

### **JWT Expiry & Token Refresh**
- Tokens expire in 8 hours; frontend must handle 401 and re-prompt for login
- No refresh token mechanism yet—consider adding if long sessions are needed

### **CORS Headers**
- Both backends set `Access-Control-Allow-Origin: *` and allow POST/GET/OPTIONS
- Safe for public demo; tighten origin in production

### **Environment Variable Naming**
- Pattern: `{COMPANY_CODE}_{ATTRIBUTE}` (e.g., `LANZI_HOST`, `EMP2_DB`)
- User passwords: `{COMPANY_CODE}_PASS_{ROLE}` (e.g., `LANZI_PASS_ADMIN`)
- Global: `JWT_SECRET` (required for all Vercel deployments)

### **Frontend Token Management**
- Token stored in `localStorage` under key `have_token`
- Cleared on logout; re-prompt on 401 response
- No built-in refresh—sessions must re-authenticate after 8h

## Deployment

### **Vercel Deployment**
1. Connect `have-gestor-api` repo to Vercel
2. Set all environment variables in Vercel dashboard (Settings → Environment Variables)
3. Deploy: `git push` or use Vercel CLI (`vercel deploy`)
4. API available at `https://have-gestor-api.vercel.app`

### **Frontend Hosting**
- `Gestor Have/index.html` + static assets → static host (GitHub Pages, Vercel, S3, etc.)
- Ensure CORS headers allow requests to the API origin

## File Roles & Responsibilities

| File | Role |
|------|------|
| `index.html` | Dashboard UI, login form, data tables, charts |
| `app.py` | Flask server for local dev; /query → PostgreSQL proxy |
| `server.py` | Raw HTTPServer alternative (legacy) |
| `have-gestor-api/api/login.js` | JWT generation & user validation |
| `have-gestor-api/api/data.js` | Query dispatcher with table whitelist & token verification |
| `have-gestor-api/lib/companies.js` | Multi-tenant config (no secrets, env-driven) |
| `Lanzi/*.py` | ETL scripts for inventory analytics |

---

**Last Updated:** 2026-03-24
For questions on architecture decisions, check the git history or review the inline comments in each file.
