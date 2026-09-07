#!/bin/bash
# =============================================================================
# Automated Pipeline Verification & End-to-End Latency Measurement
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="${SCRIPT_DIR}/.."
cd "${ROOT_DIR}"

CLICKHOUSE_URL="http://localhost:8123"

echo "================================================================="
echo "   Pipeline Health & Data Verification"
echo "================================================================="

# 1. Check PostgreSQL Record Count
echo "[1/4] Checking PostgreSQL OLTP records..."
PG_ORDERS_COUNT=$(docker exec postgres-main psql -U admin -d ecommerce_db -t -c "SELECT COUNT(*) FROM orders;" 2>/dev/null | tr -d ' ' || echo "0")
echo "  • Total orders in PostgreSQL: ${PG_ORDERS_COUNT}"

# 2. Check ClickHouse Bronze Layer
echo "[2/4] Checking ClickHouse Bronze Layer..."
CH_BRONZE_ORDERS=$(curl -s "${CLICKHOUSE_URL}/?query=SELECT+count()+FROM+bronze.orders" || echo "0")
echo "  • Total orders in ClickHouse Bronze: ${CH_BRONZE_ORDERS}"

# 3. Check ClickHouse Silver Layer & End-to-End Latency
echo "[3/4] Measuring Processing Latency in ClickHouse Silver Layer..."
LATENCY_RESULT=$(curl -s "${CLICKHOUSE_URL}/" -d "
SELECT
    order_id,
    created_at AS source_time,
    updated_at AS warehouse_time,
    dateDiff('second', created_at, updated_at) AS processing_latency_sec
FROM silver.orders
ORDER BY order_id DESC
LIMIT 1
FORMAT PrettyCompactMonoBlock
" 2>/dev/null || true)

if [ -n "$LATENCY_RESULT" ]; then
    echo "${LATENCY_RESULT}"
else
    echo "  No orders recorded in silver.orders yet. Trigger the Airflow DAG first."
fi

# 4. Check Gold Layer Fact & Dimension Tables
echo "[4/4] Checking ClickHouse Gold Analytical Tables..."
curl -s "${CLICKHOUSE_URL}/" -d "
SELECT
    table,
    total_rows
FROM system.tables
WHERE database = 'gold' AND total_rows > 0
ORDER BY table
FORMAT PrettyCompactMonoBlock
" 2>/dev/null || true

echo "================================================================="
echo "  ✓ Verification completed."
echo "================================================================="
