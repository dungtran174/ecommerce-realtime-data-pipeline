-- =============================================================================
-- ClickHouse Silver Layer
-- Clean, transform, and merge related Bronze tables
-- All tables use ReplacingMergeTree (SCD Type 1: overwrite with latest)
-- =============================================================================

CREATE DATABASE IF NOT EXISTS silver;

-- =============================================
-- 1. USERS — from bronze.users
-- =============================================

CREATE TABLE IF NOT EXISTS silver.users (
    user_id           UInt32,
    username          String,
    registration_date Date,
    created_at        DateTime,
    updated_at        DateTime
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY user_id;

CREATE MATERIALIZED VIEW IF NOT EXISTS silver.mv_users TO silver.users AS
SELECT
    id                                          AS user_id,
    trim(username)                              AS username,
    toDate(created_at - INTERVAL 7 HOUR)        AS registration_date,
    created_at - INTERVAL 7 HOUR                AS created_at,
    now()                                       AS updated_at
FROM bronze.users;

-- =============================================
-- 2. LOCATIONS — merge bronze.provinces + bronze.regions
-- =============================================

CREATE TABLE IF NOT EXISTS silver.locations (
    province_id    UInt32,
    province_name  String,
    region_id      UInt32,
    region_name    String,
    updated_at     DateTime
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY province_id;

CREATE MATERIALIZED VIEW IF NOT EXISTS silver.mv_locations TO silver.locations AS
SELECT
    p.id                    AS province_id,
    trim(p.province_name)   AS province_name,
    r.id                    AS region_id,
    trim(r.region_name)     AS region_name,
    now()                   AS updated_at
FROM bronze.provinces AS p
INNER JOIN bronze.regions AS r ON p.region_id = r.id;

-- =============================================
-- 3. CAMPAIGNS — from bronze.adscampaigns
-- =============================================

CREATE TABLE IF NOT EXISTS silver.campaigns (
    campaign_id    UInt32,
    campaign_title String,
    updated_at     DateTime
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY campaign_id;

CREATE MATERIALIZED VIEW IF NOT EXISTS silver.mv_campaigns TO silver.campaigns AS
SELECT
    id                      AS campaign_id,
    trim(campaign_title)    AS campaign_title,
    now()                   AS updated_at
FROM bronze.adscampaigns;

-- =============================================
-- 4. ORDER_STATUS — from bronze.orderstatus
-- =============================================

CREATE TABLE IF NOT EXISTS silver.order_status (
    id         UInt32,
    name       String,
    updated_at DateTime
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY id;

CREATE MATERIALIZED VIEW IF NOT EXISTS silver.mv_order_status TO silver.order_status AS
SELECT
    id,
    trim(order_status_name) AS name,
    now()                   AS updated_at
FROM bronze.orderstatus;

-- =============================================
-- 5. ORDERS — merge bronze.orders + bronze.addresses + bronze.discounts
--    Enriched with city_id, campaign_key, payment/shipping method names
-- =============================================

CREATE TABLE IF NOT EXISTS silver.orders (
    order_id           UInt32,
    user_id            UInt32,
    city_id            UInt32,
    campaign_key       UInt32,
    order_status_id    UInt32,
    payment_method     LowCardinality(String),
    shipping_method    LowCardinality(String),
    total_amount       Decimal(18, 2),
    order_amount       Decimal(18, 2),
    discount_amount    Decimal(18, 2),
    created_at         DateTime,
    updated_at         DateTime
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY order_id;

CREATE MATERIALIZED VIEW IF NOT EXISTS silver.mv_orders TO silver.orders AS
SELECT
    o.id                                            AS order_id,
    o.user_id                                       AS user_id,
    COALESCE(a.province_id, 0)                      AS city_id,
    COALESCE(d.adscampaign_id, 0)                   AS campaign_key,
    COALESCE(o.order_status_id, 0)                  AS order_status_id,
    COALESCE(trim(pm.payment_method_name), 'Unknown')   AS payment_method,
    COALESCE(trim(sm.shipping_method_name), 'Unknown')  AS shipping_method,
    o.total_amount                                  AS total_amount,
    o.order_amount                                  AS order_amount,
    o.discount_amount                               AS discount_amount,
    o.created_at - INTERVAL 7 HOUR                  AS created_at,
    now()                                           AS updated_at
FROM bronze.orders AS o
LEFT JOIN bronze.addresses AS a ON o.address_id = a.id
LEFT JOIN bronze.discounts AS d ON o.discount_id = d.id
LEFT JOIN bronze.paymentmethods AS pm ON o.payment_method_id = pm.id
LEFT JOIN bronze.shippingmethods AS sm ON o.shipping_method_id = sm.id;

-- =============================================
-- 6. PRODUCTS — merge bronze.products + categories (parent+child) + brands
-- =============================================

CREATE TABLE IF NOT EXISTS silver.products (
    product_id       UInt32,
    product_name     String,
    brand_id         UInt32,
    brand_name       String,
    category_id      UInt32,
    category_name    String,
    subcategory_id   UInt32,
    subcategory_name String,
    unit_cost        Decimal(18, 2),
    current_price    Decimal(18, 2),
    updated_at       DateTime
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY product_id;

CREATE MATERIALIZED VIEW IF NOT EXISTS silver.mv_products TO silver.products AS
SELECT
    p.id                                                AS product_id,
    trim(p.product_name)                                AS product_name,
    COALESCE(p.brand_id, 0)                             AS brand_id,
    COALESCE(trim(b.brand_name), 'Unknown')             AS brand_name,
    COALESCE(parent_cat.id, 0)                          AS category_id,
    COALESCE(trim(parent_cat.category_name), 'Unknown') AS category_name,
    COALESCE(sub_cat.id, 0)                             AS subcategory_id,
    COALESCE(trim(sub_cat.category_name), 'Unknown')    AS subcategory_name,
    p.unit_cost                                         AS unit_cost,
    p.product_price                                     AS current_price,
    now()                                               AS updated_at
FROM bronze.products AS p
LEFT JOIN bronze.brands AS b ON p.brand_id = b.id
LEFT JOIN bronze.categories AS sub_cat ON p.category_id = sub_cat.id
LEFT JOIN bronze.categories AS parent_cat ON sub_cat.category_id = parent_cat.id;

-- =============================================
-- 7. ORDER_ITEMS — from bronze.orderdetails
-- =============================================

CREATE TABLE IF NOT EXISTS silver.order_items (
    order_id      UInt32,
    product_id    UInt32,
    quantity      Int32,
    current_price Decimal(18, 2),
    gmv           Decimal(18, 2),
    updated_at    DateTime
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (order_id, product_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS silver.mv_order_items TO silver.order_items AS
SELECT
    order_id,
    product_id,
    quantity,
    product_price                           AS current_price,
    toDecimal64(quantity * product_price, 2) AS gmv,
    now()                                   AS updated_at
FROM bronze.orderdetails;
