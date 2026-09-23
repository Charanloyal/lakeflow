# ==============================================================================
# Data Platform Lab - Setup & Startup Script for Windows (PowerShell)
# ==============================================================================

Write-Host "========================================================================" -ForegroundColor Cyan
Write-Host " Starting Data Platform Lab Infrastructure Setup" -ForegroundColor Cyan
Write-Host "========================================================================" -ForegroundColor Cyan

# 1. Environment file check
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = Split-Path -Parent $ScriptDir
Set-Location $ProjectRoot

if (-not (Test-Path "$ProjectRoot\.env")) {
    Write-Host "[INFO] .env not found. Generating from .env.example..." -ForegroundColor Yellow
    Copy-Item "$ProjectRoot\.env.example" "$ProjectRoot\.env"
    Write-Host "[OK] .env generated successfully." -ForegroundColor Green
} else {
    Write-Host "[OK] .env configuration detected." -ForegroundColor Green
}

# 2. Check Docker CLI
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    Write-Host "[ERROR] Docker CLI not found. Please install Docker Desktop for Windows." -ForegroundColor Red
    exit 1
}

# 3. Check Docker Daemon
Write-Host "[INFO] Checking Docker engine connectivity..." -ForegroundColor Yellow
docker info 2>&1 | Out-Null
if ($LASTEXITCODE -ne 0) {
    Write-Host "[WARNING] Docker Desktop daemon is not currently running." -ForegroundColor Yellow
    Write-Host "[INFO] Attempting to launch Docker Desktop..." -ForegroundColor Cyan
    Start-Process "C:\Program Files\Docker\Docker\Docker Desktop.exe" -ErrorAction SilentlyContinue

    Write-Host "Waiting for Docker daemon to become responsive..." -ForegroundColor Yellow
    $retries = 30
    $ready = $false
    while ($retries -gt 0 -and -not $ready) {
        Start-Sleep -Seconds 3
        docker info 2>&1 | Out-Null
        if ($LASTEXITCODE -eq 0) {
            $ready = $true
        }
        $retries--
    }

    if (-not $ready) {
        Write-Host "[ERROR] Docker engine could not be contacted. Please start Docker Desktop manually and re-run." -ForegroundColor Red
        exit 1
    }
}
Write-Host "[OK] Docker daemon is running and responsive." -ForegroundColor Green

# 4. Validate Docker Compose config
Write-Host "[INFO] Validating docker compose configuration..." -ForegroundColor Yellow
docker compose config --quiet
if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR] docker compose config failed validation." -ForegroundColor Red
    exit 1
}
Write-Host "[OK] Docker Compose configuration valid." -ForegroundColor Green

# 5. Start stack
Write-Host "[INFO] Starting platform containers..." -ForegroundColor Cyan
docker compose up -d

Write-Host "[INFO] Waiting 20 seconds for initialization..." -ForegroundColor Yellow
Start-Sleep -Seconds 20

# 6. Show status
Write-Host "========================================================================" -ForegroundColor Cyan
Write-Host " Container Status" -ForegroundColor Cyan
Write-Host "========================================================================" -ForegroundColor Cyan
docker compose ps

Write-Host ""
Write-Host "[SUCCESS] Setup complete! Run 'python scripts/verify_health.py' to test all endpoints." -ForegroundColor Green
