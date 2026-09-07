#!/bin/bash
# =============================================================================
# One-Click Setup Script: Bootstrap Entire E-Commerce Real-time Data Pipeline
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="${SCRIPT_DIR}/.."
cd "${ROOT_DIR}"

echo "================================================================="
echo "   E-Commerce Real-Time Data Pipeline — Environment Bootstrap"
echo "================================================================="

# 1. Check prerequisites
command -v docker >/dev/null 2>&1 || { echo "Error: docker is not installed. Aborting."; exit 1; }
docker compose version >/dev/null 2>&1 || {
    docker-compose version >/dev/null 2>&1 || { echo "Error: docker compose is not installed. Aborting."; exit 1; }
}

DOCKER_COMPOSE_CMD="docker compose"
if ! docker compose version >/dev/null 2>&1; then
    DOCKER_COMPOSE_CMD="docker-compose"
fi

# 2. Ensure .env exists
if [ ! -f .env ]; then
    echo "[1/4] Creating .env from .env.example..."
    cp .env.example .env
    echo "  ✓ .env created."
else
    echo "[1/4] .env already exists."
fi

# 3. Start containers
echo "[2/4] Starting Docker services in background..."
${DOCKER_COMPOSE_CMD} up -d

# 4. Wait for core healthchecks
echo "[3/4] Waiting for services to become healthy..."
echo "  - Checking PostgreSQL (Main)..."
until docker exec postgres-main pg_isready -U admin -d ecommerce_db >/dev/null 2>&1; do
    sleep 2
done
echo "    ✓ PostgreSQL (Main) is healthy."

echo "  - Checking ClickHouse..."
until curl -s "http://localhost:8123/ping" | grep -q "Ok"; do
    sleep 2
done
echo "    ✓ ClickHouse is healthy."

echo "  - Checking Debezium..."
bash "${SCRIPT_DIR}/register_connector.sh"

echo "[4/4] Pipeline environment is fully initialized!"
echo "================================================================="
echo "                      Service Endpoints                          "
echo "================================================================="
echo "  • Airflow Web UI:      http://localhost:8080 (admin / admin)"
echo "  • Kafka UI:            http://localhost:8085"
echo "  • Debezium UI:         http://localhost:8084"
echo "  • Metabase BI:         http://localhost:3000"
echo "  • ClickHouse HTTP:     http://localhost:8123"
echo "  • PostgreSQL OLTP:     localhost:5432 (ecommerce_db)"
echo "================================================================="
echo "  Next steps:"
echo "    - Trigger order simulation in Airflow UI or run: bash scripts/verify_pipeline.sh"
echo "================================================================="
