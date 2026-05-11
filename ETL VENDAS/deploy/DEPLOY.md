# Deploy do ETL Worker no Servidor

## Pré-requisitos
- Servidor `37.60.236.200` com Python 3.11+
- PostgreSQL rodando
- Metabase rodando (JAR)

## 1. Copiar arquivos para o servidor

```bash
# No seu PC local (PowerShell), troque USER pelo seu usuário SSH:
scp -r "C:/Users/HAVE/Desktop/Arquivos/Have I/ETL VENDAS" USER@37.60.236.200:/opt/have-etl
```

## 2. Configurar no servidor

```bash
ssh user@37.60.236.200

# Criar virtualenv
cd /opt/have-etl
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Copiar e editar .env
cp .env.example .env
nano .env   # preencher senhas e METABASE_TEMPLATE_ML
```

## 3. Instalar serviço systemd

```bash
sudo cp deploy/have-etl-worker.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable have-etl-worker
sudo systemctl start have-etl-worker

# Verificar status
sudo systemctl status have-etl-worker
sudo journalctl -u have-etl-worker -f   # logs em tempo real
```

## 4. Configurar Vercel (env vars)

No Vercel Dashboard → Settings → Environment Variables, adicionar:

| Variável | Valor |
|----------|-------|
| `ETL_WORKER_URL` | `http://37.60.236.200:5050` |
| `ETL_SECRET` | mesma chave do .env no servidor |

## 5. Criar Template no Metabase

1. Acesse o Metabase (`http://37.60.236.200:3000`)
2. Crie um dashboard "Template - Mercado Livre" com queries na tabela `bd_vendas_ml`:
   - **Vendas por mês** — `SELECT ano, mes, SUM(total_venda_pedido) FROM bd_vendas_ml GROUP BY ano, mes ORDER BY ano, mes`
   - **Top 10 SKUs** — `SELECT sku, nome_produto, SUM(quantidade) as qtd, SUM(total_item) as total FROM bd_vendas_ml GROUP BY sku, nome_produto ORDER BY total DESC LIMIT 10`
   - **Status dos pedidos** — `SELECT status, COUNT(*) FROM bd_vendas_ml GROUP BY status`
   - **Receita vs Comissão** — `SELECT data, SUM(total_item) as receita, SUM(comissao_item) as comissao FROM bd_vendas_ml GROUP BY data ORDER BY data`
   - **Vendas por estado** — `SELECT receiver_state, SUM(total_item) FROM bd_vendas_ml WHERE receiver_state IS NOT NULL GROUP BY receiver_state ORDER BY 2 DESC`
   - **Ticket médio** — `SELECT DATE_TRUNC('month', data) as mes, AVG(total_venda_pedido) FROM bd_vendas_ml GROUP BY 1 ORDER BY 1`
3. Anote o **ID do dashboard** (aparece na URL: `/dashboard/42`)
4. Coloque no `.env` do servidor: `METABASE_TEMPLATE_ML=42`

## 6. Testar

```bash
# Health check
curl http://37.60.236.200:5050/etl/status

# Trigger manual (substitua os valores)
curl -X POST http://37.60.236.200:5050/etl/trigger \
  -H "Content-Type: application/json" \
  -d '{"company":"lanzi","account_id":"ml_conta1","secret":"SUA_CHAVE"}'

# Ver resultado do job
curl http://37.60.236.200:5050/etl/jobs
```

## Fluxo Automático

Após o deploy, o fluxo funciona assim:
1. Usuário conecta conta ML no Have Gestor (OAuth)
2. `data.js` salva tokens e dispara webhook para o Worker
3. Worker lê tokens do banco, roda ETL, provisiona Metabase
4. Dashboard pronto em ~2-5 minutos

## Firewall

Certifique-se que a porta `5050` está acessível da Vercel:
```bash
sudo ufw allow 5050/tcp
```

Ou use Nginx como reverse proxy se preferir.
