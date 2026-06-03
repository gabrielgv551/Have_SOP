# Script de deploy para servidor SSH
$server = "37.60.236.200"
$user = "root"
$localScript = "c:\Users\HAVE\Desktop\Arquivos\Have I\Marcon\etl\tiny_estoque_v2.py"
$remoteDir = "/opt/tiny_estoque"

Write-Host "Deploy do tiny_estoque_v2.py para $server..."

# Criar diretório remoto
ssh ${user}@${server} "mkdir -p $remoteDir && pip3 install requests sqlalchemy psycopg2-binary -q"

# Copiar script
scp "$localScript" ${user}@${server}:${remoteDir}/

Write-Host "Script copiado para ${server}:${remoteDir}/tiny_estoque_v2.py"
Write-Host ""
Write-Host "Para rodar no servidor:"
Write-Host "ssh ${user}@${server} \"cd ${remoteDir} && python3 tiny_estoque_v2.py --token 'SEU_TOKEN'\""
