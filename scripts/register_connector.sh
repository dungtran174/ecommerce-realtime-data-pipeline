#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
CONFIG_FILE="${ROOT_DIR}/debezium/connectors/ecommerce-connector.json"
CONNECTOR_NAME="ecommerce-connector"

env_value() {
    [ -f "${ROOT_DIR}/.env" ] || return 0
    sed -n "s/^${1}=//p" "${ROOT_DIR}/.env" | head -n 1
}

DEBEZIUM_HOST="${DEBEZIUM_HOST:-localhost}"
DEBEZIUM_PORT="${DEBEZIUM_PORT:-$(env_value DEBEZIUM_PORT)}"
DEBEZIUM_PORT="${DEBEZIUM_PORT:-8083}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-$(env_value POSTGRES_PASSWORD)}"
if [ -z "${POSTGRES_PASSWORD}" ]; then
    echo "POSTGRES_PASSWORD is required in the environment or .env" >&2
    exit 1
fi
command -v jq >/dev/null || { echo "jq is required" >&2; exit 1; }

API="http://${DEBEZIUM_HOST}:${DEBEZIUM_PORT}"
for attempt in $(seq 1 30); do
    if curl -fsS --max-time 3 "${API}/" >/dev/null 2>&1; then
        break
    fi
    if [ "$attempt" -eq 30 ]; then
        echo "Debezium API did not become ready at ${API}" >&2
        exit 1
    fi
    sleep 2
done

# Kafka Connect POST accepts {name,config}; PUT /config accepts config only.
# Inject the password at request time so it is absent from the committed JSON.
request_file="$(mktemp)"
chmod 600 "$request_file"
trap 'rm -f "$request_file"' EXIT
if curl -fsS --max-time 10 "${API}/connectors/${CONNECTOR_NAME}" >/dev/null 2>&1; then
    jq --arg password "$POSTGRES_PASSWORD" '.config + {"database.password": $password}' \
        "$CONFIG_FILE" > "$request_file"
    curl -fsS --max-time 30 -X PUT -H 'Content-Type: application/json' \
        --data-binary @"$request_file" "${API}/connectors/${CONNECTOR_NAME}/config" >/dev/null
else
    jq --arg password "$POSTGRES_PASSWORD" '.config["database.password"] = $password' \
        "$CONFIG_FILE" > "$request_file"
    curl -fsS --max-time 30 -X POST -H 'Content-Type: application/json' \
        --data-binary @"$request_file" "${API}/connectors" >/dev/null
fi

for attempt in $(seq 1 30); do
    # The worker may return 404 briefly while it propagates a new connector.
    status="$(curl -fsS --max-time 10 "${API}/connectors/${CONNECTOR_NAME}/status" 2>/dev/null || true)"
    if [ -z "$status" ]; then
        sleep 2
        continue
    fi
    if jq -e '.connector.state == "RUNNING" and (.tasks | length > 0) and all(.tasks[]; .state == "RUNNING")' \
        >/dev/null <<< "$status"; then
        echo "Connector and all tasks are RUNNING"
        exit 0
    fi
    if jq -e '.connector.state == "FAILED" or any(.tasks[]; .state == "FAILED")' \
        >/dev/null <<< "$status"; then
        echo "$status" | jq '{connector, tasks}' >&2
        exit 1
    fi
    sleep 2
done
echo "Connector or task did not become RUNNING" >&2
echo "$status" | jq '{connector, tasks}' >&2
exit 1
