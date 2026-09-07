# Pipeline Verification & Quality Benchmarks

To validate that the pipeline processes data accurately, reliably, and with low latency, we define 4 rigorous test scenarios.

---

## Scenario 1: End-to-End Latency Measurement

### Objective
Measure total elapsed time from when an order is created in PostgreSQL (`T_source`) to when it is fully cleansed, enriched, and queryable in ClickHouse (`T_warehouse`).

### Execution Procedure
1. Trigger `ecommerce_test_single_order_manual` DAG from Airflow (`http://localhost:8080`).
2. The DAG generates exactly 1 order with its line items and payment transaction.
3. Debezium reads the WAL event and produces a message to Kafka topic `ecommerce_cdc.public.orders`.
4. ClickHouse Kafka Engine pulls the message into `bronze.orders` and pushes it through the Materialized View into `silver.orders`.
5. Run the latency calculation query:

```sql
SELECT
    order_id,
    created_at AS source_time,
    updated_at AS warehouse_time,
    dateDiff('second', created_at, updated_at) AS processing_latency_sec
FROM silver.orders
ORDER BY order_id DESC
LIMIT 1;
```

### Benchmark Result
- **Source Timestamp**: `12:32:11`
- **Warehouse Timestamp**: `12:32:15`
- **End-to-End Latency**: **~4 seconds** (well within the near real-time target of < 10 seconds).

---

## Scenario 2: Entity Integrity Validation

### Objective
Confirm zero data loss across the streaming boundary by verifying that all entity keys created at the source exist in the destination.

### Verification Queries
Compare record counts between PostgreSQL and ClickHouse for dimension tables:

```sql
-- PostgreSQL:
SELECT count(*) FROM public.provinces;   -- e.g. 63

-- ClickHouse:
SELECT count(*) FROM gold.dim_locations; -- 63 + 1 ('Unknown') = 64
```

```sql
-- PostgreSQL:
SELECT count(*) FROM public.adscampaigns; -- e.g. 25

-- ClickHouse:
SELECT count(*) FROM gold.dim_campaigns; -- 25 + 1 ('No_Campaign') = 26
```

---

## Scenario 3: Final State Accuracy Validation (SCD Type 1)

### Objective
Validate that subsequent record updates at the source correctly overwrite previous states in ClickHouse via `ReplacingMergeTree`.

### Procedure
1. Update an entity name in PostgreSQL:
   ```sql
   UPDATE public.products SET product_name = 'Smart Watch Pro Max' WHERE id = 1;
   ```
2. Query ClickHouse with the `FINAL` modifier:
   ```sql
   SELECT product_key, product_name, updated_at
   FROM gold.dim_products FINAL
   WHERE product_key = 1;
   ```
3. **Expected Result**: ClickHouse returns the latest updated name `'Smart Watch Pro Max'` without duplicate rows.

---

## Scenario 4: Fact Aggregation & Additivity Check

### Objective
Ensure that pre-aggregated metric columns in `FACT_SALES_PRODUCT` and `FACT_ORDER_OVERVIEW` match the source numbers exactly.

### Verification Query
Compare gross revenue computed on PostgreSQL against ClickHouse:

```sql
-- PostgreSQL:
SELECT d.product_id, SUM(d.quantity * d.product_price) AS total_gmv_source
FROM orderdetails d
JOIN orders o ON d.order_id = o.id
WHERE o.order_status_id = 4
GROUP BY d.product_id
ORDER BY d.product_id;

-- ClickHouse:
SELECT product_id, sum(gmv) AS total_gmv_dest
FROM gold.FACT_SALES_PRODUCT
GROUP BY product_id
ORDER BY product_id;
```

**Result**: Values between PostgreSQL source calculation and ClickHouse pre-aggregated Fact tables reconcile with 100% precision.
