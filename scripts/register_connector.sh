#!/bin/bash
# =============================================================================
# Register or Update Debezium CDC Connector for PostgreSQL (22 tables)
# =============================================================================

set -e

DEBEZIUM_HOST="${DEBEZIUM_HOST:-localhost}"
DEBEZIUM_PORT="${DEBEZIUM_PORT:-8083}"
CONNECTOR_NAME="ecommerce-connector"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONNECTOR_CONFIG="${SCRIPT_DIR}/../debezium/connectors/ecommerce-connector.json"

echo "=================================================="
echo "  Debezium CDC Connector Registration"
echo "=================================================="

# 1. Wait for Debezium REST API to be ready
echo "[1/3] Checking Debezium Connect service..."
MAX_RETRIES=30
RETRY_COUNT=0

until curl -s "http://${DEBEZIUM_HOST}:${DEBEZIUM_PORT}/" > /dev/null 2>&1; do
    RETRY_COUNT=$((RETRY_COUNT + 1))
    if [ $RETRY_COUNT -ge $MAX_RETRIES ]; then
        echo "Error: Debezium Connect failed to become ready after ${MAX_RETRIES} attempts."
        exit 1
    fi
    echo "  Waiting for Debezium at ${DEBEZIUM_HOST}:${DEBEZIUM_PORT}... (${RETRY_COUNT}/${MAX_RETRIES})"
    sleep 3
done
echo "  ✓ Debezium Connect is up and healthy!"

# 2. Check existing connectors
echo "[2/3] Registering connector configuration..."
EXISTING=$(curl -s "http://${DEBEZIUM_HOST}:${DEBEZIUM_PORT}/connectors")

if echo "$EXISTING" | grep -q "\"${CONNECTOR_NAME}\""; then
    echo "  Connector '${CONNECTOR_NAME}' exists. Updating configuration..."
    curl -s -X PUT \
        -H "Content-Type: application/json" \
        -d @"${CONNECTOR_CONFIG}" \
        "http://${DEBEZIUM_HOST}:${DEBEZIUM_PORT}/connectors/${CONNECTOR_NAME}/config" > /dev/null
    echo "  ✓ Configuration updated."
else
    echo "  Creating new connector '${CONNECTOR_NAME}'..."
    curl -s -X POST \
        -H "Content-Type: application/json" \
        -d @"${CONNECTOR_CONFIG}" \
        "http://${DEBEZIUM_HOST}:${DEBEZIUM_PORT}/connectors" > /dev/null
    echo "  ✓ Connector created."
fi

# 3. Verify status
echo "[3/3] Verifying connector status..."
sleep 2

STATUS_JSON=$(curl -s "http://${DEBEZIUM_HOST}:${DEBEZIUM_PORT}/connectors/${CONNECTOR_NAME}/status")
CONNECTOR_STATE=$(echo "$STATUS_JSON" | grep -o '"state":"[^"]*"' | head -n 1 | cut -d':' -f2 | tr -d '"')

if [ "$CONNECTOR_STATE" = "RUNNING" ]; then
    echo "  ✓ Connector state: RUNNING"
else
    echo "  Status response: ${STATUS_JSON}"
fi

echo "=================================================="
echo "  ✓ Debezium CDC Connector is active!"
echo "  Kafka UI:    http://localhost:8085"
echo "  Debezium UI: http://localhost:8084"
echo "=================================================="
