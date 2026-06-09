# Testa extrair_contas_a_pagar_v2.py (hibrido Playwright + requests)
$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

$py = $null
foreach ($c in @("python", "python3", "py")) {
    $cmd = Get-Command $c -ErrorAction SilentlyContinue
    if ($cmd) { $py = $cmd.Source; break }
}
if (-not $py) {
    foreach ($p in @(
        "$env:LOCALAPPDATA\Programs\Python\Python312\python.exe",
        "$env:LOCALAPPDATA\Programs\Python\Python311\python.exe",
        "C:\Python312\python.exe"
    )) {
        if (Test-Path $p) { $py = $p; break }
    }
}
if (-not $py) {
    Write-Host "Python nao encontrado. Instale Python 3.11+ e rode:"
    Write-Host "  pip install playwright requests urllib3"
    Write-Host "  playwright install chromium"
    exit 1
}

& $py -m pip install -q playwright requests urllib3
& $py -m playwright install chromium

Write-Host "Executando extrator (navegador visivel)..."
& $py extrair_contas_a_pagar_v2.py
exit $LASTEXITCODE
