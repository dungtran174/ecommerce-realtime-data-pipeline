-- =============================================================================
-- ClickHouse Bronze Layer
-- Kafka Engine → Materialized View → ReplacingMergeTree
-- Each OLTP table maps to: kafka_* → mv_* → bronze.*
-- =============================================================================

CREATE DATABASE IF NOT EXISTS bronze;

-- =============================================
-- 1. USERS
-- =============================================

CREATE TABLE IF NOT EXISTS bronze.kafka_users (
    id           Int32,
    username     String,
    password     String,
    email        String,
    mobile       Nullable(String),
    created_at   Int64,
    __op         String,
    __table      String,
    __source_ts_ms Int64
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'ecommerce_cdc.public.users',
    kafka_group_name = 'clickhouse_bronze_users',
    kafka_format = 'JSONEachRow',
    kafka_skip_broken_messages = 10;

CREATE TABLE IF NOT EXISTS bronze.users (
    id           UInt32,
    username     String,
    password     String,
    email        String,
    mobile       Nullable(String),
    created_at   DateTime,
    _version     UInt64
) ENGINE = ReplacingMergeTree(_version)
ORDER BY id;

CREATE MATERIALIZED VIEW IF NOT EXISTS bronze.mv_users TO bronze.users AS
SELECT
    id,
    username,
    password,
    email,
    mobile,
    COALESCE(fromUnixTimestamp64Milli(created_at), now()) AS created_at,
    __source_ts_ms AS _version
FROM bronze.kafka_users
WHERE __op != 'd';

-- =============================================
-- 2. ROLES
-- =============================================

CREATE TABLE IF NOT EXISTS bronze.kafka_roles (
    id           Int32,
    role_name    String,
    role_title   String,
    created_at   Int64,
    __op         String,
    __table      String,
    __source_ts_ms Int64
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'ecommerce_cdc.public.roles',
    kafka_group_name = 'clickhouse_bronze_roles',
    kafka_format = 'JSONEachRow',
    kafka_skip_broken_messages = 10;

CREATE TABLE IF NOT EXISTS bronze.roles (
    id           UInt32,
    role_name    String,
    role_title   String,
    created_at   DateTime,
    _version     UInt64
) ENGINE = ReplacingMergeTree(_version)
ORDER BY id;

CREATE MATERIALIZED VIEW IF NOT EXISTS bronze.mv_roles TO bronze.roles AS
SELECT
    id,
    role_name,
    role_title,
    COALESCE(fromUnixTimestamp64Milli(created_at), now()) AS created_at,
    __source_ts_ms AS _version
FROM bronze.kafka_roles
WHERE __op != 'd';

-- =============================================
-- 3. ROLE_USER
-- =============================================

CREATE TABLE IF NOT EXISTS bronze.kafka_role_user (
    id           Int32,
    user_id      Int32,
    role_id      Int32,
    created_at   Int64,
    __op         String,
    __table      String,
    __source_ts_ms Int64
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'ecommerce_cdc.public.role_user',
    kafka_group_name = 'clickhouse_bronze_role_user',
    kafka_format = 'JSONEachRow',
    kafka_skip_broken_messages = 10;

CREATE TABLE IF NOT EXISTS bronze.role_user (
    id           UInt32,
    user_id      UInt32,
    role_id      UInt32,
    created_at   DateTime,
    _version     UInt64
) ENGINE = ReplacingMergeTree(_version)
ORDER BY id;

CREATE MATERIALIZED VIEW IF NOT EXISTS bronze.mv_role_user TO bronze.role_user AS
SELECT
    id,
    user_id,
    role_id,
    COALESCE(fromUnixTimestamp64Milli(created_at), now()) AS created_at,
    __source_ts_ms AS _version
FROM bronze.kafka_role_user
WHERE __op != 'd';

-- =============================================
-- 4. REGIONS
-- =============================================

CREATE TABLE IF NOT EXISTS bronze.kafka_regions (
    id           Int32,
    region_name  String,
    created_at   Int64,
    __op         String,
    __table      String,
    __source_ts_ms Int64
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'ecommerce_cdc.public.regions',
    kafka_group_name = 'clickhouse_bronze_regions',
    kafka_format = 'JSONEachRow',
    kafka_skip_broken_messages = 10;

CREATE TABLE IF NOT EXISTS bronze.regions (
    id           UInt32,
    region_name  String,
    created_at   DateTime,
    _version     UInt64
) ENGINE = ReplacingMergeTree(_version)
ORDER BY id;

CREATE MATERIALIZED VIEW IF NOT EXISTS bronze.mv_regions TO bronze.regions AS
SELECT
    id,
    region_name,
    COALESCE(fromUnixTimestamp64Milli(created_at), now()) AS created_at,
    __source_ts_ms AS _version
FROM bronze.kafka_regions
WHERE __op != 'd';

-- =============================================
-- 5. PROVINCES
-- =============================================

CREATE TABLE IF NOT EXISTS bronze.kafka_provinces (
    id              Int32,
    province_name   String,
    region_id       Int32,
    latitude        Nullable(String),
    longitude       Nullable(String),
    created_at      Int64,
    __op            String,
    __table         String,
    __source_ts_ms  Int64
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'ecommerce_cdc.public.provinces',
    kafka_group_name = 'clickhouse_bronze_provinces',
    kafka_format = 'JSONEachRow',
    kafka_skip_broken_messages = 10;

CREATE TABLE IF NOT EXISTS bronze.provinces (
    id              UInt32,
    province_name   String,
    region_id       UInt32,
    latitude        Nullable(Float64),
    longitude       Nullable(Float64),
    created_at      DateTime,
    _version        UInt64
) ENGINE = ReplacingMergeTree(_version)
ORDER BY id;

CREATE MATERIALIZED VIEW IF NOT EXISTS bronze.mv_provinces TO bronze.provinces AS
SELECT
    id,
    province_name,
    region_id,
    toFloat64OrNull(latitude)  AS latitude,
    toFloat64OrNull(longitude) AS longitude,
    COALESCE(fromUnixTimestamp64Milli(created_at), now()) AS created_at,
    __source_ts_ms AS _version
FROM bronze.kafka_provinces
WHERE __op != 'd';

-- =============================================
-- 6. ADDRESSES
-- =============================================

CREATE TABLE IF NOT EXISTS bronze.kafka_addresses (
    id              Int32,
    title           Nullable(String),
    user_id         Int32,
    province_id     Int32,
    region_id       Int32,
    full_address    Nullable(String),
    created_at      Int64,
    __op            String,
    __table         String,
    __source_ts_ms  Int64
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'ecommerce_cdc.public.addresses',
    kafka_group_name = 'clickhouse_bronze_addresses',
    kafka_format = 'JSONEachRow',
    kafka_skip_broken_messages = 10;

CREATE TABLE IF NOT EXISTS bronze.addresses (
    id              UInt32,
    title           Nullable(String),
    user_id         UInt32,
    province_id     UInt32,
    region_id       UInt32,
    full_address    Nullable(String),
    created_at      DateTime,
    _version        UInt64
) ENGINE = ReplacingMergeTree(_version)
ORDER BY id;

CREATE MATERIALIZED VIEW IF NOT EXISTS bronze.mv_addresses TO bronze.addresses AS
SELECT
    id,
    title,
    user_id,
    province_id,
    region_id,
    full_address,
    COALESCE(fromUnixTimestamp64Milli(created_at), now()) AS created_at,
    __source_ts_ms AS _version
FROM bronze.kafka_addresses
WHERE __op != 'd';

-- =============================================
-- 7. CATEGORIES (self-referencing)
-- =============================================

CREATE TABLE IF NOT EXISTS bronze.kafka_categories (
    id              Int32,
    category_name   String,
    category_id     Nullable(Int32),
    slug            Nullable(String),
    created_at      Int64,
    __op            String,
    __table         String,
    __source_ts_ms  Int64
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'ecommerce_cdc.public.categories',
    kafka_group_name = 'clickhouse_bronze_categories',
    kafka_format = 'JSONEachRow',
    kafka_skip_broken_messages = 10;

CREATE TABLE IF NOT EXISTS bronze.categories (
    id              UInt32,
    category_name   String,
    category_id     Nullable(UInt32),
    slug            Nullable(String),
    created_at      DateTime,
    _version        UInt64
) ENGINE = ReplacingMergeTree(_version)
ORDER BY id;

CREATE MATERIALIZED VIEW IF NOT EXISTS bronze.mv_categories TO bronze.categories AS
SELECT
    id,
    category_name,
    category_id,
    slug,
    COALESCE(fromUnixTimestamp64Milli(created_at), now()) AS created_at,
    __source_ts_ms AS _version
FROM bronze.kafka_categories
WHERE __op != 'd';

-- =============================================
-- 8. BRANDS
-- =============================================

CREATE TABLE IF NOT EXISTS bronze.kafka_brands (
    id           Int32,
    brand_name   String,
    created_at   Int64,
    __op         String,
    __table      String,
    __source_ts_ms Int64
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'ecommerce_cdc.public.brands',
    kafka_group_name = 'clickhouse_bronze_brands',
    kafka_format = 'JSONEachRow',
    kafka_skip_broken_messages = 10;

CREATE TABLE IF NOT EXISTS bronze.brands (
    id           UInt32,
    brand_name   String,
    created_at   DateTime,
    _version     UInt64
) ENGINE = ReplacingMergeTree(_version)
ORDER BY id;

CREATE MATERIALIZED VIEW IF NOT EXISTS bronze.mv_brands TO bronze.brands AS
SELECT
    id,
    brand_name,
    COALESCE(fromUnixTimestamp64Milli(created_at), now()) AS created_at,
    __source_ts_ms AS _version
FROM bronze.kafka_brands
WHERE __op != 'd';

-- =============================================
-- 9. TAGS
-- =============================================

CREATE TABLE IF NOT EXISTS bronze.kafka_tags (
    id           Int32,
    tag_name     String,
    created_at   Int64,
    __op         String,
    __table      String,
    __source_ts_ms Int64
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'ecommerce_cdc.public.tags',
    kafka_group_name = 'clickhouse_bronze_tags',
    kafka_format = 'JSONEachRow',
    kafka_skip_broken_messages = 10;

CREATE TABLE IF NOT EXISTS bronze.tags (
    id           UInt32,
    tag_name     String,
    created_at   DateTime,
    _version     UInt64
) ENGINE = ReplacingMergeTree(_version)
ORDER BY id;

CREATE MATERIALIZED VIEW IF NOT EXISTS bronze.mv_tags TO bronze.tags AS
SELECT
    id,
    tag_name,
    COALESCE(fromUnixTimestamp64Milli(created_at), now()) AS created_at,
    __source_ts_ms AS _version
FROM bronze.kafka_tags
WHERE __op != 'd';

-- =============================================
-- 10. PRODUCTS
-- =============================================

CREATE TABLE IF NOT EXISTS bronze.kafka_products (
    id                  Int32,
    product_name        String,
    category_id         Nullable(Int32),
    brand_id            Nullable(Int32),
    product_price       Nullable(String),
    unit_cost           Nullable(String),
    product_quantity    Nullable(Int32),
    created_at          Int64,
    __op                String,
    __table             String,
    __source_ts_ms      Int64
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'ecommerce_cdc.public.products',
    kafka_group_name = 'clickhouse_bronze_products',
    kafka_format = 'JSONEachRow',
    kafka_skip_broken_messages = 10;

CREATE TABLE IF NOT EXISTS bronze.products (
    id                  UInt32,
    product_name        String,
    category_id         Nullable(UInt32),
    brand_id            Nullable(UInt32),
    product_price       Decimal(15, 2),
    unit_cost           Decimal(15, 2),
    product_quantity    Int32,
    created_at          DateTime,
    _version            UInt64
) ENGINE = ReplacingMergeTree(_version)
ORDER BY id;

CREATE MATERIALIZED VIEW IF NOT EXISTS bronze.mv_products TO bronze.products AS
SELECT
    id,
    product_name,
    category_id,
    brand_id,
    toDecimal64OrDefault(product_price, 2, toDecimal64(0, 2)) AS product_price,
    toDecimal64OrDefault(unit_cost, 2, toDecimal64(0, 2))     AS unit_cost,
    COALESCE(product_quantity, 0)              AS product_quantity,
    COALESCE(fromUnixTimestamp64Milli(created_at), now()) AS created_at,
    __source_ts_ms AS _version
FROM bronze.kafka_products
WHERE __op != 'd';

-- =============================================
-- 11. PRODUCT_TAG
-- =============================================

CREATE TABLE IF NOT EXISTS bronze.kafka_product_tag (
    id           Int32,
    product_id   Int32,
    tag_id       Int32,
    created_at   Int64,
    __op         String,
    __table      String,
    __source_ts_ms Int64
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'ecommerce_cdc.public.product_tag',
    kafka_group_name = 'clickhouse_bronze_product_tag',
    kafka_format = 'JSONEachRow',
    kafka_skip_broken_messages = 10;

CREATE TABLE IF NOT EXISTS bronze.product_tag (
    id           UInt32,
    product_id   UInt32,
    tag_id       UInt32,
    created_at   DateTime,
    _version     UInt64
) ENGINE = ReplacingMergeTree(_version)
ORDER BY id;

CREATE MATERIALIZED VIEW IF NOT EXISTS bronze.mv_product_tag TO bronze.product_tag AS
SELECT
    id,
    product_id,
    tag_id,
    COALESCE(fromUnixTimestamp64Milli(created_at), now()) AS created_at,
    __source_ts_ms AS _version
FROM bronze.kafka_product_tag
WHERE __op != 'd';

-- =============================================
-- 12. ORDER_STATUS
-- =============================================

CREATE TABLE IF NOT EXISTS bronze.kafka_orderstatus (
    id                  Int32,
    order_status_name   String,
    created_at          Int64,
    __op                String,
    __table             String,
    __source_ts_ms      Int64
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'ecommerce_cdc.public.order_status',
    kafka_group_name = 'clickhouse_bronze_orderstatus',
    kafka_format = 'JSONEachRow',
    kafka_skip_broken_messages = 10;

CREATE TABLE IF NOT EXISTS bronze.orderstatus (
    id                  UInt32,
    order_status_name   String,
    created_at          DateTime,
    _version            UInt64
) ENGINE = ReplacingMergeTree(_version)
ORDER BY id;

CREATE MATERIALIZED VIEW IF NOT EXISTS bronze.mv_orderstatus TO bronze.orderstatus AS
SELECT
    id,
    order_status_name,
    COALESCE(fromUnixTimestamp64Milli(created_at), now()) AS created_at,
    __source_ts_ms AS _version
FROM bronze.kafka_orderstatus
WHERE __op != 'd';

-- =============================================
-- 13. PAYMENT_STATUS
-- =============================================

CREATE TABLE IF NOT EXISTS bronze.kafka_paymentstatus (
    id                      Int32,
    payment_status_name     String,
    created_at              Int64,
    __op                    String,
    __table                 String,
    __source_ts_ms          Int64
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'ecommerce_cdc.public.payment_status',
    kafka_group_name = 'clickhouse_bronze_paymentstatus',
    kafka_format = 'JSONEachRow',
    kafka_skip_broken_messages = 10;

CREATE TABLE IF NOT EXISTS bronze.paymentstatus (
    id                      UInt32,
    payment_status_name     String,
    created_at              DateTime,
    _version                UInt64
) ENGINE = ReplacingMergeTree(_version)
ORDER BY id;

CREATE MATERIALIZED VIEW IF NOT EXISTS bronze.mv_paymentstatus TO bronze.paymentstatus AS
SELECT
    id,
    payment_status_name,
    COALESCE(fromUnixTimestamp64Milli(created_at), now()) AS created_at,
    __source_ts_ms AS _version
FROM bronze.kafka_paymentstatus
WHERE __op != 'd';

-- =============================================
-- 14. SHIPPING_STATUS
-- =============================================

CREATE TABLE IF NOT EXISTS bronze.kafka_shippingstatus (
    id                      Int32,
    shipping_status_name    String,
    created_at              Int64,
    __op                    String,
    __table                 String,
    __source_ts_ms          Int64
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'ecommerce_cdc.public.shipping_status',
    kafka_group_name = 'clickhouse_bronze_shippingstatus',
    kafka_format = 'JSONEachRow',
    kafka_skip_broken_messages = 10;

CREATE TABLE IF NOT EXISTS bronze.shippingstatus (
    id                      UInt32,
    shipping_status_name    String,
    created_at              DateTime,
    _version                UInt64
) ENGINE = ReplacingMergeTree(_version)
ORDER BY id;

CREATE MATERIALIZED VIEW IF NOT EXISTS bronze.mv_shippingstatus TO bronze.shippingstatus AS
SELECT
    id,
    shipping_status_name,
    COALESCE(fromUnixTimestamp64Milli(created_at), now()) AS created_at,
    __source_ts_ms AS _version
FROM bronze.kafka_shippingstatus
WHERE __op != 'd';

-- =============================================
-- 15. PAYMENT_METHODS
-- =============================================

CREATE TABLE IF NOT EXISTS bronze.kafka_paymentmethods (
    id                      Int32,
    payment_method_name     String,
    created_at              Int64,
    __op                    String,
    __table                 String,
    __source_ts_ms          Int64
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'ecommerce_cdc.public.payment_methods',
    kafka_group_name = 'clickhouse_bronze_paymentmethods',
    kafka_format = 'JSONEachRow',
    kafka_skip_broken_messages = 10;

CREATE TABLE IF NOT EXISTS bronze.paymentmethods (
    id                      UInt32,
    payment_method_name     String,
    created_at              DateTime,
    _version                UInt64
) ENGINE = ReplacingMergeTree(_version)
ORDER BY id;

CREATE MATERIALIZED VIEW IF NOT EXISTS bronze.mv_paymentmethods TO bronze.paymentmethods AS
SELECT
    id,
    payment_method_name,
    COALESCE(fromUnixTimestamp64Milli(created_at), now()) AS created_at,
    __source_ts_ms AS _version
FROM bronze.kafka_paymentmethods
WHERE __op != 'd';

-- =============================================
-- 16. SHIPPING_METHODS
-- =============================================

CREATE TABLE IF NOT EXISTS bronze.kafka_shippingmethods (
    id                      Int32,
    shipping_method_name    String,
    created_at              Int64,
    __op                    String,
    __table                 String,
    __source_ts_ms          Int64
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'ecommerce_cdc.public.shipping_methods',
    kafka_group_name = 'clickhouse_bronze_shippingmethods',
    kafka_format = 'JSONEachRow',
    kafka_skip_broken_messages = 10;

CREATE TABLE IF NOT EXISTS bronze.shippingmethods (
    id                      UInt32,
    shipping_method_name    String,
    created_at              DateTime,
    _version                UInt64
) ENGINE = ReplacingMergeTree(_version)
ORDER BY id;

CREATE MATERIALIZED VIEW IF NOT EXISTS bronze.mv_shippingmethods TO bronze.shippingmethods AS
SELECT
    id,
    shipping_method_name,
    COALESCE(fromUnixTimestamp64Milli(created_at), now()) AS created_at,
    __source_ts_ms AS _version
FROM bronze.kafka_shippingmethods
WHERE __op != 'd';

-- =============================================
-- 17. ADS_CAMPAIGNS
-- =============================================

CREATE TABLE IF NOT EXISTS bronze.kafka_adscampaigns (
    id              Int32,
    campaign_title  String,
    started_at      Nullable(Int64),
    expired_at      Nullable(Int64),
    __op            String,
    __table         String,
    __source_ts_ms  Int64
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'ecommerce_cdc.public.ads_campaigns',
    kafka_group_name = 'clickhouse_bronze_adscampaigns',
    kafka_format = 'JSONEachRow',
    kafka_skip_broken_messages = 10;

CREATE TABLE IF NOT EXISTS bronze.adscampaigns (
    id              UInt32,
    campaign_title  String,
    started_at      Nullable(DateTime),
    expired_at      Nullable(DateTime),
    _version        UInt64
) ENGINE = ReplacingMergeTree(_version)
ORDER BY id;

CREATE MATERIALIZED VIEW IF NOT EXISTS bronze.mv_adscampaigns TO bronze.adscampaigns AS
SELECT
    id,
    campaign_title,
    if(started_at IS NOT NULL, fromUnixTimestamp64Milli(started_at), NULL) AS started_at,
    if(expired_at IS NOT NULL, fromUnixTimestamp64Milli(expired_at), NULL) AS expired_at,
    __source_ts_ms AS _version
FROM bronze.kafka_adscampaigns
WHERE __op != 'd';

-- =============================================
-- 18. DISCOUNTS
-- =============================================

CREATE TABLE IF NOT EXISTS bronze.kafka_discounts (
    id              Int32,
    adscampaign_id  Nullable(Int32),
    type            String,
    value           Nullable(String),
    code            Nullable(String),
    started_at      Nullable(Int64),
    expired_at      Nullable(Int64),
    __op            String,
    __table         String,
    __source_ts_ms  Int64
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'ecommerce_cdc.public.discounts',
    kafka_group_name = 'clickhouse_bronze_discounts',
    kafka_format = 'JSONEachRow',
    kafka_skip_broken_messages = 10;

CREATE TABLE IF NOT EXISTS bronze.discounts (
    id              UInt32,
    adscampaign_id  Nullable(UInt32),
    type            String,
    value           Decimal(15, 2),
    code            Nullable(String),
    started_at      Nullable(DateTime),
    expired_at      Nullable(DateTime),
    _version        UInt64
) ENGINE = ReplacingMergeTree(_version)
ORDER BY id;

CREATE MATERIALIZED VIEW IF NOT EXISTS bronze.mv_discounts TO bronze.discounts AS
SELECT
    id,
    adscampaign_id,
    type,
    toDecimal64OrDefault(value, 2, toDecimal64(0, 2)) AS value,
    code,
    if(started_at IS NOT NULL, fromUnixTimestamp64Milli(started_at), NULL) AS started_at,
    if(expired_at IS NOT NULL, fromUnixTimestamp64Milli(expired_at), NULL) AS expired_at,
    __source_ts_ms AS _version
FROM bronze.kafka_discounts
WHERE __op != 'd';

-- =============================================
-- 19. ORDERS (high throughput: 4 consumers)
-- =============================================

CREATE TABLE IF NOT EXISTS bronze.kafka_orders (
    id                  Int32,
    user_id             Int32,
    staff_id            Nullable(Int32),
    address_id          Nullable(Int32),
    order_amount        Nullable(String),
    discount_amount     Nullable(String),
    total_amount        Nullable(String),
    discount_id         Nullable(Int32),
    payment_method_id   Nullable(Int32),
    payment_status_id   Nullable(Int32),
    order_status_id     Nullable(Int32),
    shipping_method_id  Nullable(Int32),
    shipping_status_id  Nullable(Int32),
    shipped_at          Nullable(Int64),
    created_at          Int64,
    updated_at          Int64,
    __op                String,
    __table             String,
    __source_ts_ms      Int64
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'ecommerce_cdc.public.orders',
    kafka_group_name = 'clickhouse_bronze_orders',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 4,
    kafka_skip_broken_messages = 10;

CREATE TABLE IF NOT EXISTS bronze.orders (
    id                  UInt32,
    user_id             UInt32,
    staff_id            Nullable(UInt32),
    address_id          Nullable(UInt32),
    order_amount        Decimal(15, 2),
    discount_amount     Decimal(15, 2),
    total_amount        Decimal(15, 2),
    discount_id         Nullable(UInt32),
    payment_method_id   Nullable(UInt32),
    payment_status_id   Nullable(UInt32),
    order_status_id     Nullable(UInt32),
    shipping_method_id  Nullable(UInt32),
    shipping_status_id  Nullable(UInt32),
    shipped_at          Nullable(DateTime),
    created_at          DateTime,
    updated_at          DateTime,
    _version            UInt64
) ENGINE = ReplacingMergeTree(_version)
ORDER BY id;

CREATE MATERIALIZED VIEW IF NOT EXISTS bronze.mv_orders TO bronze.orders AS
SELECT
    id,
    user_id,
    staff_id,
    address_id,
    toDecimal64OrDefault(order_amount, 2, toDecimal64(0, 2))    AS order_amount,
    toDecimal64OrDefault(discount_amount, 2, toDecimal64(0, 2)) AS discount_amount,
    toDecimal64OrDefault(total_amount, 2, toDecimal64(0, 2))    AS total_amount,
    discount_id,
    payment_method_id,
    payment_status_id,
    order_status_id,
    shipping_method_id,
    shipping_status_id,
    if(shipped_at IS NOT NULL, fromUnixTimestamp64Milli(shipped_at), NULL) AS shipped_at,
    COALESCE(fromUnixTimestamp64Milli(created_at), now()) AS created_at,
    COALESCE(fromUnixTimestamp64Milli(updated_at), now()) AS updated_at,
    __source_ts_ms AS _version
FROM bronze.kafka_orders
WHERE __op != 'd';

-- =============================================
-- 20. ORDERDETAILS (high throughput: 4 consumers)
-- =============================================

CREATE TABLE IF NOT EXISTS bronze.kafka_orderdetails (
    id              Int32,
    order_id        Int32,
    product_id      Int32,
    quantity        Int32,
    product_price   Nullable(String),
    product_tax     Nullable(String),
    subtotal_amount Nullable(String),
    created_at      Int64,
    __op            String,
    __table         String,
    __source_ts_ms  Int64
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'ecommerce_cdc.public.orderdetails',
    kafka_group_name = 'clickhouse_bronze_orderdetails',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 4,
    kafka_skip_broken_messages = 10;

CREATE TABLE IF NOT EXISTS bronze.orderdetails (
    id              UInt32,
    order_id        UInt32,
    product_id      UInt32,
    quantity        Int32,
    product_price   Decimal(15, 2),
    product_tax     Decimal(15, 2),
    subtotal_amount Decimal(15, 2),
    created_at      DateTime,
    _version        UInt64
) ENGINE = ReplacingMergeTree(_version)
ORDER BY id;

CREATE MATERIALIZED VIEW IF NOT EXISTS bronze.mv_orderdetails TO bronze.orderdetails AS
SELECT
    id,
    order_id,
    product_id,
    quantity,
    toDecimal64OrDefault(product_price, 2, toDecimal64(0, 2))   AS product_price,
    toDecimal64OrDefault(product_tax, 2, toDecimal64(0, 2))      AS product_tax,
    toDecimal64OrDefault(subtotal_amount, 2, toDecimal64(0, 2))  AS subtotal_amount,
    COALESCE(fromUnixTimestamp64Milli(created_at), now()) AS created_at,
    __source_ts_ms AS _version
FROM bronze.kafka_orderdetails
WHERE __op != 'd';

-- =============================================
-- 21. ORDER_STATUS_HISTORY
-- =============================================

CREATE TABLE IF NOT EXISTS bronze.kafka_orderstatushistory (
    id              Int32,
    order_id        Int32,
    order_status_id Int32,
    staff_id        Nullable(Int32),
    comments        Nullable(String),
    created_at      Int64,
    __op            String,
    __table         String,
    __source_ts_ms  Int64
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'ecommerce_cdc.public.order_status_history',
    kafka_group_name = 'clickhouse_bronze_orderstatushistory',
    kafka_format = 'JSONEachRow',
    kafka_skip_broken_messages = 10;

CREATE TABLE IF NOT EXISTS bronze.orderstatushistory (
    id              UInt32,
    order_id        UInt32,
    order_status_id UInt32,
    staff_id        Nullable(UInt32),
    comments        Nullable(String),
    created_at      DateTime,
    _version        UInt64
) ENGINE = ReplacingMergeTree(_version)
ORDER BY id;

CREATE MATERIALIZED VIEW IF NOT EXISTS bronze.mv_orderstatushistory TO bronze.orderstatushistory AS
SELECT
    id,
    order_id,
    order_status_id,
    staff_id,
    comments,
    COALESCE(fromUnixTimestamp64Milli(created_at), now()) AS created_at,
    __source_ts_ms AS _version
FROM bronze.kafka_orderstatushistory
WHERE __op != 'd';

-- =============================================
-- 22. TRANSACTIONS
-- =============================================

CREATE TABLE IF NOT EXISTS bronze.kafka_transactions (
    id                  Int32,
    order_id            Int32,
    transaction_type    String,
    status              Nullable(Int8),
    created_at          Int64,
    description         Nullable(String),
    __op                String,
    __table             String,
    __source_ts_ms      Int64
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'ecommerce_cdc.public.transactions',
    kafka_group_name = 'clickhouse_bronze_transactions',
    kafka_format = 'JSONEachRow',
    kafka_skip_broken_messages = 10;

CREATE TABLE IF NOT EXISTS bronze.transactions (
    id                  UInt32,
    order_id            UInt32,
    transaction_type    String,
    status              UInt8,
    created_at          DateTime,
    description         Nullable(String),
    _version            UInt64
) ENGINE = ReplacingMergeTree(_version)
ORDER BY id;

CREATE MATERIALIZED VIEW IF NOT EXISTS bronze.mv_transactions TO bronze.transactions AS
SELECT
    id,
    order_id,
    transaction_type,
    COALESCE(status, 1)  AS status,
    COALESCE(fromUnixTimestamp64Milli(created_at), now()) AS created_at,
    description,
    __source_ts_ms AS _version
FROM bronze.kafka_transactions
WHERE __op != 'd';
