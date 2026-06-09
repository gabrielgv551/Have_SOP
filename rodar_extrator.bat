@echo off
cd /d "%~dp0"
set PY=%LOCALAPPDATA%\Programs\Python\Python312\python.exe
if not exist "%PY%" (
    echo Python nao encontrado. Rode primeiro: instalar_python_e_rodar.bat
    pause
    exit /b 1
)

set CHROME=%LOCALAPPDATA%\ms-playwright\chromium-1223\chrome-win64\chrome.exe
if not exist "%CHROME%" (
    echo Instalando Chromium do Playwright ^(primeira vez, ~180 MB^)...
    "%PY%" -m playwright install chromium
    if not exist "%CHROME%" (
        echo ERRO: Chromium nao instalado. Rode manualmente:
        echo   "%PY%" -m playwright install chromium
        pause
        exit /b 1
    )
)

"%PY%" extrair_contas_a_pagar_v2.py
echo.
echo Codigo de saida: %ERRORLEVEL%
pause
exit /b %ERRORLEVEL%
