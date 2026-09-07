# Change Data Capture (CDC) with Debezium

Debezium is used as a distributed Change Data Capture platform that streams low-latency row-level changes directly from PostgreSQL into Apache Kafka.

---

## 1. Why CDC over Traditional Polling ETL?

1. **Zero Impact on Production Queries**: Rather than running recurring `SELECT * WHERE updated_at > ...` scans that lock tables and degrade API response times, Debezium reads the PostgreSQL **Write-Ahead Log (WAL)** directly.
2. **Captures Deletes & State Transitions**: Hard deletes and intermediate status changes (e.g., Pending -> Processing -> Shipped) are immediately captured as discrete events.
3. **Sub-second Latency**: Events are emitted immediately upon transaction commit.

---

## 2. PostgreSQL Configuration

PostgreSQL is configured with logical replication in `docker-compose.yml`:
```yaml
command: [
  "postgres",
  "-c", "wal_level=logical",
  "-c", "max_wal_senders=10",
  "-c", "max_replication_slots=10"
]
```

- `wal_level=logical`: Instructs Postgres to write detailed row changes to the WAL.
- `plugin.name=pgoutput`: Uses PostgreSQL's built-in logical decoding output plugin.

---

## 3. Connector Configuration

The connector configuration is maintained in `debezium/connectors/ecommerce-connector.json`:

```json
{
  "name": "ecommerce-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "postgres-main",
    "database.port": "5432",
    "database.user": "admin",
    "database.password": "secret",
    "database.dbname": "ecommerce_db",
    "topic.prefix": "ecommerce_cdc",
    "schema.include.list": "public",
    "table.include.list": "public.users,public.roles,public.role_user,public.regions,public.provinces,public.addresses,public.categories,public.brands,public.tags,public.products,public.product_tag,public.order_status,public.payment_status,public.shipping_status,public.payment_methods,public.shipping_methods,public.ads_campaigns,public.discounts,public.orders,public.orderdetails,public.order_status_history,public.transactions",
    "plugin.name": "pgoutput",
    "slot.name": "debezium_ecommerce",
    "publication.name": "dbz_publication",
    "snapshot.mode": "initial",
    "decimal.handling.mode": "string",
    "transforms": "unwrap",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.unwrap.drop.tombstones": "true",
    "transforms.unwrap.delete.handling.mode": "rewrite",
    "transforms.unwrap.add.fields": "op,table,source.ts_ms"
  }
}
```

### Key Parameters Explained:
- `snapshot.mode = initial`: On first run, captures an initial consistent snapshot of all existing data before streaming incremental WAL events.
- `ExtractNewRecordState (SMT)`: Unwraps Debezium's complex schema payload into a flattened JSON object, while appending metadata (`__op`, `__table`, `__source_ts_ms`) for downstream ClickHouse deduplication.
- `decimal.handling.mode = string`: Emits high-precision currency values as strings to prevent floating-point inaccuracies.

---

## 4. Operational Commands & API Inspection

```bash
# Register or update connector
bash scripts/register_connector.sh

# List registered connectors
curl -s http://localhost:8083/connectors | jq .

# Check connector & task execution status
curl -s http://localhost:8083/connectors/ecommerce-connector/status | jq .

# Restart connector if failed
curl -s -X POST http://localhost:8083/connectors/ecommerce-connector/restart
```
