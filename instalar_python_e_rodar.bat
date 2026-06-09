@echo off
chcp 65001 >nul
cd /d "%~dp0"
title Have - Instalar Python e rodar extrator

echo.
echo === Have SOP - Extrator GCOM ===
echo Pasta: %CD%
echo.

where python >nul 2>&1
if %ERRORLEVEL%==0 goto :deps

where py >nul 2>&1
if %ERRORLEVEL%==0 (
    set PY=py -3
    goto :deps
)

echo [1/3] Python NAO encontrado. Baixando instalador oficial 3.12...
set INSTALLER=%TEMP%\python-3.12.10-amd64.exe
curl.exe -L -o "%INSTALLER%" "https://www.python.org/ftp/python/3.12.10/python-3.12.10-amd64.exe"
if not exist "%INSTALLER%" (
    echo ERRO: download falhou. Instale manualmente:
    echo   https://www.python.org/downloads/
    echo Marque: "Add python.exe to PATH"
    pause
    exit /b 1
)

echo [2/3] Instalando Python para seu usuario + PATH...
echo       Se aparecer janela de permissao, clique Sim.
"%INSTALLER%" /passive InstallAllUsers=0 PrependPath=1 Include_test=0 Include_launcher=1
if %ERRORLEVEL% neq 0 (
    echo Instalacao retornou codigo %ERRORLEVEL%. Tente instalar manualmente pelo site.
    pause
    exit /b 1
)

set "PY=%LOCALAPPDATA%\Programs\Python\Python312\python.exe"
if not exist "%PY%" (
    where python >nul 2>&1
    if %ERRORLEVEL%==0 (set PY=python) else (
        echo Python instalado mas nao encontrado em Python312.
        echo Feche o CMD, abra um NOVO CMD e rode este .bat de novo.
        pause
        exit /b 1
    )
)
goto :deps

:deps
if not defined PY set PY=python
echo.
echo Usando: %PY%
%PY% --version
if %ERRORLEVEL% neq 0 (
    echo ERRO: Python nao responde.
    pause
    exit /b 1
)

echo [3/3] Instalando bibliotecas e Chromium...
%PY% -m pip install --upgrade pip
%PY% -m pip install playwright requests urllib3
%PY% -m playwright install chromium

echo.
echo Rodando extrair_contas_a_pagar_v2.py ...
echo Uma janela do Chrome pode abrir para login no GCOM.
echo.
%PY% extrair_contas_a_pagar_v2.py
set RC=%ERRORLEVEL%
echo.
echo Codigo de saida: %RC%
pause
exit /b %RC%
