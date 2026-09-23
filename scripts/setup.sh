#!/usr/bin/env bash
# ==============================================================================
# Data Platform Lab - Setup & Startup Script for Linux / macOS / WSL
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_ROOT"

echo "========================================================================"
echo " Starting Data Platform Lab Infrastructure Setup"
echo "========================================================================"

# 1. Environment file check
if [ ! -f "$PROJECT_ROOT/.env" ]; then
    echo "[INFO] .env not found. Copying from .env.example..."
    cp "$PROJECT_ROOT/.env.example" "$PROJECT_ROOT/.env"
    echo "[OK] Created .env."
else
    echo "[OK] .env exists."
fi

# 2. Check Docker
if ! command -v docker &> /dev/null; then
    echo "[ERROR] Docker is not installed or not in PATH."
    exit 1
fi

if ! docker info &> /dev/null; then
    echo "[ERROR] Docker daemon is not running. Please start Docker."
    exit 1
fi
echo "[OK] Docker daemon is running."

# 3. Validate Docker Compose config
echo "[INFO] Validating docker compose configuration..."
docker compose config --quiet
echo "[OK] Compose configuration is valid."

# 4. Start services
echo "[INFO] Starting platform containers..."
docker compose up -d

echo "[INFO] Waiting 20 seconds for services to initialize..."
sleep 20

# 5. Display status
echo "========================================================================"
echo " Container Status"
echo "========================================================================"
docker compose ps

echo ""
echo "[SUCCESS] Stack launched! Run 'python3 scripts/verify_health.py' to verify health."
