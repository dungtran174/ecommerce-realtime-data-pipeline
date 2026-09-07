# Troubleshooting & Debugging Guide

This guide covers resolution steps for common issues encountered during setup or operation of the data pipeline.

---

## 1. Kafka KRaft Startup Failure

### Symptoms
- Container `kafka` exits with code 1.
- Log error: `Cluster ID doesn't match stored ID`.

### Cause
Kafka's persistent storage contains metadata initialized under a different cluster ID than the current environment configuration.

### Fix
```bash
# Wipe only the Kafka volume
docker compose stop kafka
docker volume rm ecommerce-realtime-data-pipeline_kafka-data
docker compose up -d kafka
```

---

## 2. Debezium Replication Slot Conflict

### Symptoms
- Debezium log: `replication slot "debezium_ecommerce" is already active`.
- Connector state in Debezium UI remains `FAILED`.

### Cause
A previous container shutdown did not release the PostgreSQL logical replication slot cleanly.

### Fix
Execute from host terminal to reset the replication slot:
```bash
docker exec -it postgres-main psql -U admin -d ecommerce_db -c "
SELECT pg_drop_replication_slot('debezium_ecommerce') 
WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'debezium_ecommerce');"

# Restart connector
bash scripts/register_connector.sh
```

---

## 3. ClickHouse Type Casting & Decimal Mismatches

### Symptoms
- ClickHouse Materialized View fails to insert into Bronze or Silver tables.
- Log shows `Cannot parse string as Decimal64`.

### Cause
Empty strings or non-numeric representations emitted by upstream CDC events.

### Fix
Use `toDecimal64OrDefault(column, 2, 0.0)` or `toDecimal64OrNull(column, 2)` instead of direct `toDecimal64()` casts. This pattern has been standardized across `clickhouse/init/01_bronze_kafka_tables.sql`.

---

## 4. Timezone Offset Discrepancy (+7h vs UTC)

### Symptoms
Order timestamps in ClickHouse appear 7 hours behind the local system time.

### Cause
PostgreSQL stores local timestamps without timezone offsets (`TIMESTAMP WITHOUT TIME ZONE`), which ClickHouse parses as UTC by default.

### Fix
The Silver layer Materialized Views automatically correct this by applying:
```sql
(created_at - INTERVAL 7 HOUR) AS created_at
```
Ensure all custom analytical queries reference `silver.*`, `gold.*`, or `report.*` layers rather than querying `bronze.*` raw tables directly.
