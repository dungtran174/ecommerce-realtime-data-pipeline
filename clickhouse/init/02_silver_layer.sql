-- Current state is resolved when queried. Incremental JOIN materialized views
-- cannot refresh an order when only an address, discount or method changes.
-- FINAL chooses the largest Kafka offset per source key; tombstones stay in
-- Bronze for audit but are excluded from these business-facing views.
CREATE DATABASE IF NOT EXISTS silver;

CREATE VIEW IF NOT EXISTS silver.users AS
SELECT id AS user_id, trim(username) AS username,
       toDate(toTimeZone(created_at, 'Asia/Ho_Chi_Minh')) AS registration_date,
       created_at, created_at AS updated_at
FROM bronze.users FINAL WHERE _deleted = 0;

CREATE VIEW IF NOT EXISTS silver.locations AS
SELECT p.id AS province_id, trim(p.province_name) AS province_name,
       r.id AS region_id, trim(r.region_name) AS region_name,
       p.created_at AS updated_at
FROM (SELECT * FROM bronze.provinces FINAL WHERE _deleted = 0) AS p
LEFT JOIN (SELECT * FROM bronze.regions FINAL WHERE _deleted = 0) AS r
    ON p.region_id = r.id;

CREATE VIEW IF NOT EXISTS silver.campaigns AS
SELECT id AS campaign_id, trim(campaign_title) AS campaign_title,
       now() AS updated_at
FROM bronze.adscampaigns FINAL WHERE _deleted = 0;

CREATE VIEW IF NOT EXISTS silver.order_status AS
SELECT id, trim(order_status_name) AS name, created_at AS updated_at
FROM bronze.orderstatus FINAL WHERE _deleted = 0;

CREATE VIEW IF NOT EXISTS silver.orders AS
SELECT o.id AS order_id, o.user_id AS user_id,
       ifNull(a.province_id, 0) AS city_id,
       ifNull(d.adscampaign_id, 0) AS campaign_key,
       ifNull(o.order_status_id, 0) AS order_status_id,
       ifNull(trim(pm.payment_method_name), 'Unknown') AS payment_method,
       ifNull(trim(sm.shipping_method_name), 'Unknown') AS shipping_method,
       o.total_amount AS total_amount,
       o.order_amount AS order_amount,
       o.discount_amount AS discount_amount,
       o.created_at AS created_at,
       o.updated_at AS updated_at
FROM (SELECT * FROM bronze.orders FINAL WHERE _deleted = 0) AS o
LEFT JOIN (SELECT * FROM bronze.addresses FINAL WHERE _deleted = 0) AS a
    ON o.address_id = a.id
LEFT JOIN (SELECT * FROM bronze.discounts FINAL WHERE _deleted = 0) AS d
    ON o.discount_id = d.id
LEFT JOIN (SELECT * FROM bronze.paymentmethods FINAL WHERE _deleted = 0) AS pm
    ON o.payment_method_id = pm.id
LEFT JOIN (SELECT * FROM bronze.shippingmethods FINAL WHERE _deleted = 0) AS sm
    ON o.shipping_method_id = sm.id;

CREATE VIEW IF NOT EXISTS silver.products AS
SELECT p.id AS product_id, trim(p.product_name) AS product_name,
       ifNull(p.brand_id, 0) AS brand_id,
       ifNull(trim(b.brand_name), 'Unknown') AS brand_name,
       ifNull(parent_cat.id, 0) AS category_id,
       ifNull(trim(parent_cat.category_name), 'Unknown') AS category_name,
       ifNull(sub_cat.id, 0) AS subcategory_id,
       ifNull(trim(sub_cat.category_name), 'Unknown') AS subcategory_name,
       p.unit_cost AS unit_cost, p.product_price AS current_price,
       p.created_at AS updated_at
FROM (SELECT * FROM bronze.products FINAL WHERE _deleted = 0) AS p
LEFT JOIN (SELECT * FROM bronze.brands FINAL WHERE _deleted = 0) AS b
    ON p.brand_id = b.id
LEFT JOIN (SELECT * FROM bronze.categories FINAL WHERE _deleted = 0) AS sub_cat
    ON p.category_id = sub_cat.id
LEFT JOIN (SELECT * FROM bronze.categories FINAL WHERE _deleted = 0) AS parent_cat
    ON sub_cat.category_id = parent_cat.id;

-- id is the order line's business key; (order_id, product_id) is not unique.
CREATE VIEW IF NOT EXISTS silver.order_items AS
SELECT id AS order_item_id, order_id, product_id, quantity,
       product_price AS current_price, product_tax,
       subtotal_amount, unit_cost_at_order,
       toDecimal64(quantity, 2) * product_price AS gmv,
       created_at AS updated_at
FROM bronze.orderdetails FINAL WHERE _deleted = 0;
