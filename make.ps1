# PowerShell Makefile wrapper for Windows
param (
    [Parameter(Position=0)]
    [string]$Target = "help"
)

switch ($Target.ToLower()) {
    "config"    { docker compose config }
    "up"        { docker compose up -d }
    "down"      { docker compose down }
    "restart"   { docker compose down; docker compose up -d }
    "status"    { docker compose ps }
    "logs"      { docker compose logs -f }
    "test"      { python -m unittest discover -s tests -p "test_*.py" -v }
    "demo"      { python scripts/demo.py }
    "benchmark" { python apps/benchmarks/benchmark_engine.py }
    "verify"    { python scripts/verify_health.py }
    "clean"     { docker compose down -v --remove-orphans }
    Default {
        Write-Host "========================================================================" -ForegroundColor Cyan
        Write-Host " Data Platform Lab - PowerShell Operational Commands" -ForegroundColor Cyan
        Write-Host "========================================================================" -ForegroundColor Cyan
        Write-Host " .\make.ps1 test        - Run all automated test suites"
        Write-Host " .\make.ps1 demo        - Run end-to-end 5-step mutation lifecycle demo"
        Write-Host " .\make.ps1 benchmark   - Run 2,000,000 event benchmark & generate report"
        Write-Host " .\make.ps1 config      - Validate docker-compose configuration"
        Write-Host " .\make.ps1 up          - Start all containers"
        Write-Host " .\make.ps1 down        - Stop all containers"
        Write-Host " .\make.ps1 status      - View container status"
        Write-Host " .\make.ps1 verify      - Probe service endpoints"
        Write-Host "========================================================================" -ForegroundColor Cyan
    }
}
