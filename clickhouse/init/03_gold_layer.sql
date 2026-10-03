-- Gold reads current Silver state. Re-running a query cannot duplicate counts;
-- updates, deletes and late order lines are visible after CDC catches up.
CREATE DATABASE IF NOT EXISTS gold;

CREATE VIEW IF NOT EXISTS gold.dim_date AS
SELECT date AS date_key, toDayOfMonth(date) AS day,
       toMonth(date) AS month, toQuarter(date) AS quarter,
       toYear(date) AS year, toDayOfWeek(date) AS day_of_week,
       toUInt8(toDayOfWeek(date) IN (6, 7)) AS is_weekend
FROM (SELECT toDate('2000-01-01') + number AS date FROM numbers(36525));

CREATE VIEW IF NOT EXISTS gold.dim_products AS
SELECT product_id, product_name, brand_name, category_name,
       subcategory_name, unit_cost, current_price, updated_at
FROM silver.products;

CREATE VIEW IF NOT EXISTS gold.dim_locations AS
SELECT province_id, province_name, region_id, region_name, updated_at
FROM silver.locations;

CREATE VIEW IF NOT EXISTS gold.dim_campaigns AS
SELECT campaign_id, campaign_title, updated_at FROM silver.campaigns;

CREATE VIEW IF NOT EXISTS gold.dim_order_status AS
SELECT id, name, updated_at FROM silver.order_status;

CREATE VIEW IF NOT EXISTS gold.FACT_USER_REGISTRATION AS
SELECT registration_date, count() AS user_amount
FROM silver.users GROUP BY registration_date;

-- One current order contributes to exactly one status bucket.
CREATE VIEW IF NOT EXISTS gold.FACT_ORDER_OVERVIEW AS
SELECT toDate(toTimeZone(created_at, 'Asia/Ho_Chi_Minh')) AS date_key, city_id, order_status_id,
       payment_method, shipping_method,
       count() AS order_count, sum(total_amount) AS total_gmv
FROM silver.orders
GROUP BY date_key, city_id, order_status_id, payment_method, shipping_method;

-- Product order count counts distinct orders *within each product*; it is not
-- additive across products. Overall order counts come from FACT_ORDER_OVERVIEW.
-- The last line of an order absorbs the gross discount rounding remainder.
-- Discount before VAT uses the line's pre-VAT share of its VAT-inclusive total.
CREATE VIEW IF NOT EXISTS gold.sales_line_base AS
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

CREATE VIEW IF NOT EXISTS gold.sales_line_preliminary AS
SELECT *,
       if(order_amount > 0,
          round(discount_amount * subtotal_amount / order_amount, 2),
          toDecimal64(0, 4)) AS preliminary_discount,
       row_number() OVER (PARTITION BY order_id ORDER BY order_item_id DESC) AS reverse_line_number,
       sum(if(order_amount > 0,
              round(discount_amount * subtotal_amount / order_amount, 2),
              toDecimal64(0, 4))) OVER (PARTITION BY order_id) AS preliminary_total
FROM gold.sales_line_base;

CREATE VIEW IF NOT EXISTS gold.sales_line_allocated AS
SELECT *,
       if(reverse_line_number = 1,
          preliminary_discount + discount_amount - preliminary_total,
          preliminary_discount) AS discount_gross
FROM gold.sales_line_preliminary;

CREATE VIEW IF NOT EXISTS gold.sales_line_measured AS
SELECT *,
       if(subtotal_amount > 0,
          round(discount_gross * gmv / subtotal_amount, 2),
          toDecimal128(0, 8)) AS discount_ex_vat
FROM gold.sales_line_allocated;

CREATE VIEW IF NOT EXISTS gold.FACT_SALES_PRODUCT AS
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
