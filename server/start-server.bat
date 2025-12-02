@echo off
REM Script para iniciar o servidor Torre de Controle
REM Este script pode ser executado manualmente ou configurado para iniciar automaticamente

REM Obter o diretório do script
cd /d "%~dp0"

REM Verificar se Python está instalado
python --version >nul 2>&1
if errorlevel 1 (
    echo [ERRO] Python nao encontrado! Instale Python primeiro.
    pause
    exit /b 1
)

REM Verificar se o ambiente virtual existe (opcional)
if exist "venv\Scripts\activate.bat" (
    call venv\Scripts\activate.bat
)

REM Iniciar o servidor
echo [INFO] Iniciando servidor Torre de Controle...
echo [INFO] Diretorio: %CD%
echo [INFO] Servidor iniciara em: http://localhost:8001
echo [INFO] Pressione Ctrl+C para parar o servidor
echo.

python -m app.main

REM Se o servidor fechar, manter a janela aberta para ver erros
if errorlevel 1 (
    echo.
    echo [ERRO] O servidor foi encerrado com erro!
    pause
)


