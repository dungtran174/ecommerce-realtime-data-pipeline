-- Idempotent upgrade for existing ClickHouse volumes. Kafka engine tables do
-- not support ADD COLUMN, so recreate this consumer with the same group name.
-- The committed group offset lives in Kafka and is retained across recreation.
DROP TABLE IF EXISTS bronze.mv_orderdetails;

DROP TABLE IF EXISTS bronze.kafka_orderdetails;
CREATE TABLE bronze.kafka_orderdetails (
    id Int32,
    order_id Int32,
    product_id Int32,
    quantity Int32,
    product_price Nullable(String),
    product_tax Nullable(String),
    subtotal_amount Nullable(String),
    unit_cost_at_order Nullable(String),
    created_at Int64,
    __op String,
    __table String,
    __source_ts_ms Int64
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'ecommerce_cdc.public.orderdetails',
    kafka_group_name = 'clickhouse_bronze_orderdetails',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 4,
    kafka_skip_broken_messages = 100;

ALTER TABLE bronze.orderdetails
    ADD COLUMN IF NOT EXISTS unit_cost_at_order Nullable(Decimal(15, 2)) AFTER subtotal_amount;

CREATE MATERIALIZED VIEW IF NOT EXISTS bronze.mv_orderdetails TO bronze.orderdetails AS
SELECT
    id, order_id, product_id, quantity,
    toDecimal64OrDefault(product_price, 2, toDecimal64(0, 2)) AS product_price,
    toDecimal64OrDefault(product_tax, 2, toDecimal64(0, 2)) AS product_tax,
    toDecimal64OrDefault(subtotal_amount, 2, toDecimal64(0, 2)) AS subtotal_amount,
    toDecimal64OrNull(unit_cost_at_order, 2) AS unit_cost_at_order,
    COALESCE(fromUnixTimestamp64Milli(created_at), now()) AS created_at,
    _offset AS _version,
    toUInt8(__op = 'd') AS _deleted
FROM bronze.kafka_orderdetails;

CREATE OR REPLACE VIEW silver.order_items AS
SELECT id AS order_item_id, order_id, product_id, quantity,
       product_price AS current_price, product_tax,
       subtotal_amount, unit_cost_at_order,
       toDecimal64(quantity, 2) * product_price AS gmv,
       created_at AS updated_at
FROM bronze.orderdetails FINAL WHERE _deleted = 0;

CREATE OR REPLACE VIEW gold.sales_line_base AS
SELECT o.order_id AS order_id, o.created_at AS created_at,
       o.city_id AS city_id, o.campaign_key AS campaign_key,
       o.order_amount AS order_amount, o.discount_amount AS discount_amount,
       oi.order_item_id AS order_item_id, oi.product_id AS product_id,
       oi.quantity AS quantity, oi.gmv AS gmv,
       oi.subtotal_amount AS subtotal_amount,
       oi.unit_cost_at_order AS unit_cost_at_order
FROM silver.orders AS o
INNER JOIN silver.order_status AS os ON o.order_status_id = os.id
INNER JOIN silver.order_items AS oi ON o.order_id = oi.order_id
WHERE lower(os.name) = 'delivered';

-- ClickHouse stores the output columns of ordinary views at creation time;
-- refresh each SELECT * view so the new snapshot column reaches the fact.
CREATE OR REPLACE VIEW gold.sales_line_preliminary AS
SELECT *,
       if(order_amount > 0,
          round(discount_amount * subtotal_amount / order_amount, 2),
          toDecimal64(0, 4)) AS preliminary_discount,
       row_number() OVER (PARTITION BY order_id ORDER BY order_item_id DESC) AS reverse_line_number,
       sum(if(order_amount > 0,
              round(discount_amount * subtotal_amount / order_amount, 2),
              toDecimal64(0, 4))) OVER (PARTITION BY order_id) AS preliminary_total
FROM gold.sales_line_base;

CREATE OR REPLACE VIEW gold.sales_line_allocated AS
SELECT *,
       if(reverse_line_number = 1,
          preliminary_discount + discount_amount - preliminary_total,
          preliminary_discount) AS discount_gross
FROM gold.sales_line_preliminary;

CREATE OR REPLACE VIEW gold.sales_line_measured AS
SELECT *,
       if(subtotal_amount > 0,
          round(discount_gross * gmv / subtotal_amount, 2),
          toDecimal128(0, 8)) AS discount_ex_vat
FROM gold.sales_line_allocated;

CREATE OR REPLACE VIEW gold.FACT_SALES_PRODUCT AS
SELECT toDate(toTimeZone(m.created_at, 'Asia/Ho_Chi_Minh')) AS date_key,
       m.city_id, m.product_id, m.campaign_key,
       toUInt64(sum(m.quantity)) AS quantity,
       sum(m.gmv) AS gmv,
       if(countIf(m.unit_cost_at_order IS NULL) > 0,
          NULL,
          sum(toDecimal64(m.quantity, 2) * m.unit_cost_at_order)) AS total_cost,
       countIf(m.unit_cost_at_order IS NULL) AS unknown_cost_line_count,
       sum(m.discount_gross) AS discount_gross,
       sum(m.discount_ex_vat) AS discount_val,
       sum(m.gmv) - sum(m.discount_ex_vat) AS net_revenue,
       uniqExact(m.order_id) AS order_count
FROM gold.sales_line_measured AS m
GROUP BY date_key, city_id, product_id, campaign_key;
