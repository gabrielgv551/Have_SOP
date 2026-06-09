@echo off
echo ========================================================
echo   Refazendo a base do Preco Certo (Maio e Junho)
echo ========================================================
cd /d "%~dp0"

set PYTHONIOENCODING=utf-8
set FULL_RELOAD=1
set PRECOCERTO_FIRST_DATE=2026-05-01
set FORCE_DOWNLOAD=1

echo Iniciando extração e sincronizacao no banco de dados...
python PRECOCERTO_ETL.py

echo.
echo ========================================================
echo   Processo finalizado.
echo ========================================================
pause
