# Deploy do tiny_estoque_v2.py para o servidor SSH
$server = "37.60.236.200"
$user = "root"
$scriptLocal = "c:\Users\HAVE\Desktop\Arquivos\Have I\Marcon\etl\tiny_estoque_v2.py"
$remoteDir = "/opt/tiny_estoque"

Write-Host "====================================================="
Write-Host "Deploy do tiny_estoque_v2.py para o servidor" -ForegroundColor Cyan
Write-Host "Servidor: $server"
Write-Host "====================================================="
Write-Host ""

# 1. Criar diretório remoto e instalar dependências
Write-Host "[1/3] Criando diretório remoto..." -ForegroundColor Yellow
ssh "${user}@${server}" "mkdir -p ${remoteDir} && pip3 install requests sqlalchemy psycopg2-binary -q 2>/dev/null || pip install requests sqlalchemy psycopg2-binary -q"

# 2. Copiar script para o servidor
Write-Host "[2/3] Copiando script..." -ForegroundColor Yellow
scp "${scriptLocal}" "${user}@${server}:${remoteDir}/tiny_estoque_v2.py"

# 3. Verificar se copiou corretamente
Write-Host "[3/3] Verificando..." -ForegroundColor Yellow
ssh "${user}@${server}" "ls -la ${remoteDir}/tiny_estoque_v2.py"

Write-Host ""
Write-Host "====================================================="
Write-Host "Deploy concluído!" -ForegroundColor Green
Write-Host ""
Write-Host "Para rodar no servidor:"
Write-Host "ssh ${user}@${server} \"cd ${remoteDir} && python3 tiny_estoque_v2.py --token 'SEU_TOKEN_V2'\"" -ForegroundColor Cyan
Write-Host ""
Write-Host "Para usar no n8n, crie um nó 'Execute Command':"
Write-Host "Command: ssh" -ForegroundColor Gray
Write-Host "Args: ${user}@${server} cd ${remoteDir} && python3 tiny_estoque_v2.py --token 'SEU_TOKEN_V2'" -ForegroundColor Gray
Write-Host "====================================================="
