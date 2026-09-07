#!/bin/bash
# =============================================================================
# Clean & Teardown Script
# Usage:
#   bash scripts/clean.sh            # Stop containers
#   bash scripts/clean.sh --volumes  # Stop containers and wipe volumes
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="${SCRIPT_DIR}/.."
cd "${ROOT_DIR}"

DOCKER_COMPOSE_CMD="docker compose"
if ! docker compose version >/dev/null 2>&1; then
    DOCKER_COMPOSE_CMD="docker-compose"
fi

if [ "$1" = "--volumes" ] || [ "$1" = "-v" ]; then
    echo "================================================================="
    echo "  Tearing down containers and deleting all persistent volumes..."
    echo "================================================================="
    ${DOCKER_COMPOSE_CMD} down -v --remove-orphans
    echo "  ✓ All containers and data volumes have been wiped clean."
else
    echo "================================================================="
    echo "  Stopping all containers (preserving persistent volume data)..."
    echo "================================================================="
    ${DOCKER_COMPOSE_CMD} down --remove-orphans
    echo "  ✓ Containers stopped. Use 'bash scripts/clean.sh --volumes' to wipe data."
fi
