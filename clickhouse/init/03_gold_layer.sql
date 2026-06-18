-- =============================================================================
-- ClickHouse Gold Layer
-- Dimension tables (ReplacingMergeTree) + Fact tables (SummingMergeTree)
-- Business-ready analytics layer
-- =============================================================================

CREATE DATABASE IF NOT EXISTS gold;

-- #############################################################################
-- DIMENSION TABLES
-- #############################################################################

-- =============================================
-- 1. DIM_DATE — Pre-generated calendar (2020–2029)
-- =============================================

CREATE TABLE IF NOT EXISTS gold.dim_date (
    date_key    Date,
    day         UInt8,
    month       UInt8,
    quarter     UInt8,
    year        UInt16,
    day_of_week UInt8,
    is_weekend  UInt8
) ENGINE = MergeTree()
ORDER BY date_key;

-- Populate 10 years of dates
INSERT INTO gold.dim_date
SELECT
    date                                                    AS date_key,
    toDayOfMonth(date)                                      AS day,
    toMonth(date)                                           AS month,
    toQuarter(date)                                         AS quarter,
    toYear(date)                                            AS year,
    toDayOfWeek(date)                                       AS day_of_week,
    if(toDayOfWeek(date) IN (6, 7), 1, 0)                   AS is_weekend
FROM (
    SELECT toDate('2020-01-01') + number AS date
    FROM numbers(3653)  -- ~10 years
);

-- =============================================
-- 2. DIM_PRODUCTS — from silver.products
-- =============================================

CREATE TABLE IF NOT EXISTS gold.dim_products (
    product_id       UInt32,
    product_name     String,
    brand_name       String,
    category_name    String,
    subcategory_name String,
    unit_cost        Decimal(18, 2),
    current_price    Decimal(18, 2),
    updated_at       DateTime
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY product_id;

-- Seed 'Unknown' row for orphan dimension lookups
INSERT INTO gold.dim_products VALUES (0, 'Unknown', 'Unknown', 'Unknown', 'Unknown', 0, 0, now());

CREATE MATERIALIZED VIEW IF NOT EXISTS gold.mv_dim_products TO gold.dim_products AS
SELECT
    product_id,
    product_name,
    brand_name,
    category_name,
    subcategory_name,
    unit_cost,
    current_price,
    updated_at
FROM silver.products;

-- =============================================
-- 3. DIM_LOCATIONS — from silver.locations
-- =============================================

CREATE TABLE IF NOT EXISTS gold.dim_locations (
    province_id   UInt32,
    province_name String,
    region_id     UInt32,
    region_name   String,
    updated_at    DateTime
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY province_id;

-- Seed 'Unknown' row
INSERT INTO gold.dim_locations VALUES (0, 'Unknown', 0, 'Unknown', now());

CREATE MATERIALIZED VIEW IF NOT EXISTS gold.mv_dim_locations TO gold.dim_locations AS
SELECT
    province_id,
    province_name,
    region_id,
    region_name,
    updated_at
FROM silver.locations;

-- =============================================
-- 4. DIM_CAMPAIGNS — from silver.campaigns
-- =============================================

CREATE TABLE IF NOT EXISTS gold.dim_campaigns (
    campaign_id    UInt32,
    campaign_title String,
    updated_at     DateTime
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY campaign_id;

-- Seed 'No Campaign' row for orders without discounts
INSERT INTO gold.dim_campaigns VALUES (0, 'No_Campaign', now());

CREATE MATERIALIZED VIEW IF NOT EXISTS gold.mv_dim_campaigns TO gold.dim_campaigns AS
SELECT
    campaign_id,
    campaign_title,
    updated_at
FROM silver.campaigns;

-- =============================================
-- 5. DIM_ORDER_STATUS — from silver.order_status
-- =============================================

CREATE TABLE IF NOT EXISTS gold.dim_order_status (
    id         UInt32,
    name       String,
    updated_at DateTime
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY id;

CREATE MATERIALIZED VIEW IF NOT EXISTS gold.mv_dim_order_status TO gold.dim_order_status AS
SELECT
    id,
    name,
    updated_at
FROM silver.order_status;


-- #############################################################################
-- FACT TABLES
-- #############################################################################

-- =============================================
-- 6. FACT_USER_REGISTRATION — MV from silver.users
--    Grain: 1 row = total new users per day
--    Engine: SummingMergeTree (auto-aggregates on merge)
-- =============================================

CREATE TABLE IF NOT EXISTS gold.FACT_USER_REGISTRATION (
    registration_date Date,
    user_amount       UInt64
) ENGINE = SummingMergeTree(user_amount)
ORDER BY registration_date;

CREATE MATERIALIZED VIEW IF NOT EXISTS gold.mv_fact_user_registration
TO gold.FACT_USER_REGISTRATION AS
SELECT
    registration_date,
    count() AS user_amount
FROM silver.users
GROUP BY registration_date;

-- =============================================
-- 7. FACT_ORDER_OVERVIEW — MV from silver.orders
--    Grain: 1 order per time/location/status/method
--    Engine: SummingMergeTree
-- =============================================

CREATE TABLE IF NOT EXISTS gold.FACT_ORDER_OVERVIEW (
    date_key          Date,
    city_id           UInt32,
    order_status_id   UInt32,
    payment_method    LowCardinality(String),
    shipping_method   LowCardinality(String),
    order_count       UInt64,
    total_gmv         Decimal(38, 2)
) ENGINE = SummingMergeTree((order_count, total_gmv))
ORDER BY (date_key, city_id, order_status_id, payment_method, shipping_method);

CREATE MATERIALIZED VIEW IF NOT EXISTS gold.mv_fact_order_overview
TO gold.FACT_ORDER_OVERVIEW AS
SELECT
    toDate(created_at)  AS date_key,
    city_id,
    order_status_id,
    payment_method,
    shipping_method,
    count()             AS order_count,
    sum(total_amount)   AS total_gmv
FROM silver.orders
GROUP BY
    date_key, city_id, order_status_id,
    payment_method, shipping_method;

-- =============================================
-- 8. FACT_SALES_PRODUCT — ETL every 5 minutes (NOT a MV)
--    Grain: 1 product per time/location/campaign
--    Reason: Requires multi-table JOIN (orders + order_items + products)
--            Data may arrive at different times, 5min buffer ensures integrity
--    Engine: SummingMergeTree
-- =============================================

CREATE TABLE IF NOT EXISTS gold.FACT_SALES_PRODUCT (
    date_key       Date,
    city_id        UInt32,
    product_id     UInt32,
    campaign_key   UInt32,
    quantity       UInt64,
    gmv            Decimal(38, 2),
    total_cost     Decimal(38, 2),
    discount_val   Decimal(38, 2),
    net_revenue    Decimal(38, 2),
    order_count    UInt64
) ENGINE = SummingMergeTree((quantity, gmv, total_cost, discount_val, net_revenue, order_count))
ORDER BY (date_key, city_id, product_id, campaign_key);
