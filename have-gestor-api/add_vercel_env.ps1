# Script PowerShell para adicionar variáveis de ambiente da Autoequip no Vercel
$env:VERCEL_TELEMETRY_DISABLED = "1"

$vars = @{
    "AUTOEQUIP_HOST"         = "37.60.236.200"
    "AUTOEQUIP_PORT"         = "5432"
    "AUTOEQUIP_DB"           = "Autoequip"
    "AUTOEQUIP_USER"         = "postgres"
    "AUTOEQUIP_PASSWORD"     = "131105Gv"
    "AUTOEQUIP_PASS_ADMIN"   = "autoequip2024"
    "AUTOEQUIP_PASS_GESTOR"  = "gestor2024"
    "AUTOEQUIP_PASS_HAVE"    = "have2024"
}

foreach ($key in $vars.Keys) {
    $value = $vars[$key]
    Write-Host "Adicionando $key..."
    $value | vercel env add $key production --force 2>&1
    Write-Host "  -> OK"
}

Write-Host ""
Write-Host "Todas as variaveis adicionadas! Fazendo redeploy..."
vercel --prod --yes
Write-Host "Deploy finalizado!"
