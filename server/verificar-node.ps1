# Script para verificar se Node.js está instalado

Write-Host "Verificando Node.js..." -ForegroundColor Cyan

# Verificar node
$nodeVersion = node --version 2>$null
if ($nodeVersion) {
    Write-Host "[OK] Node.js encontrado: $nodeVersion" -ForegroundColor Green
} else {
    Write-Host "[ERRO] Node.js nao encontrado no PATH" -ForegroundColor Red
    Write-Host "Tente reiniciar o terminal ou verificar a instalacao do Node.js" -ForegroundColor Yellow
}

# Verificar npm
$npmVersion = npm --version 2>$null
if ($npmVersion) {
    Write-Host "[OK] npm encontrado: $npmVersion" -ForegroundColor Green
} else {
    Write-Host "[ERRO] npm nao encontrado no PATH" -ForegroundColor Red
    Write-Host "Tente reiniciar o terminal" -ForegroundColor Yellow
}

# Verificar onde está instalado
$nodePath = Get-Command node -ErrorAction SilentlyContinue
if ($nodePath) {
    Write-Host "[INFO] Node.js localizado em: $($nodePath.Source)" -ForegroundColor Cyan
}

$npmPath = Get-Command npm -ErrorAction SilentlyContinue
if ($npmPath) {
    Write-Host "[INFO] npm localizado em: $($npmPath.Source)" -ForegroundColor Cyan
}

Write-Host ""
Write-Host "Pressione qualquer tecla para continuar..."
$null = $Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")


