#!/bin/bash
# =============================================================================
# Register Debezium Connector
# Run this script AFTER all services are up and healthy:
#   docker-compose up -d
#   ./debezium/register-connector.sh
# =============================================================================

set -e

DEBEZIUM_HOST="${DEBEZIUM_HOST:-localhost}"
DEBEZIUM_PORT="${DEBEZIUM_PORT:-8083}"
CONNECTOR_FILE="$(dirname "$0")/connectors/ecommerce-connector.json"

echo "============================================="
echo " Debezium Connector Registration"
echo "============================================="

# 1. Wait for Debezium to be ready
echo "[1/3] Waiting for Debezium Connect to be ready..."
until curl -s "http://${DEBEZIUM_HOST}:${DEBEZIUM_PORT}/" > /dev/null 2>&1; do
    echo "  Debezium not ready yet, retrying in 5s..."
    sleep 5
done
echo "  ✓ Debezium Connect is ready!"

# 2. Check if connector already exists
echo "[2/3] Checking existing connectors..."
EXISTING=$(curl -s "http://${DEBEZIUM_HOST}:${DEBEZIUM_PORT}/connectors")
echo "  Current connectors: ${EXISTING}"

if echo "$EXISTING" | grep -q "ecommerce-connector"; then
    echo "  ⚠ Connector 'ecommerce-connector' already exists. Updating..."
    curl -s -X PUT \
        -H "Content-Type: application/json" \
        -d @"${CONNECTOR_FILE}" \
        "http://${DEBEZIUM_HOST}:${DEBEZIUM_PORT}/connectors/ecommerce-connector/config" | python3 -m json.tool
else
    echo "  Registering new connector..."
    curl -s -X POST \
        -H "Content-Type: application/json" \
        -d @"${CONNECTOR_FILE}" \
        "http://${DEBEZIUM_HOST}:${DEBEZIUM_PORT}/connectors" | python3 -m json.tool
fi

# 3. Verify
echo ""
echo "[3/3] Verifying connector status..."
sleep 3
curl -s "http://${DEBEZIUM_HOST}:${DEBEZIUM_PORT}/connectors/ecommerce-connector/status" | python3 -m json.tool

echo ""
echo "============================================="
echo " ✓ Done! Check Debezium UI: http://localhost:8084"
echo " ✓ Check Kafka UI:    http://localhost:8085"
echo "============================================="
