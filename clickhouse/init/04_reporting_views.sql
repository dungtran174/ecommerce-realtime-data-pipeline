-- =============================================================================
-- ClickHouse Reporting Views
-- These views are consumed directly by Metabase for BI dashboards
-- =============================================================================

CREATE DATABASE IF NOT EXISTS report;

-- =============================================
-- 1. view_marketing_dashboard
-- Source: FACT_SALES_PRODUCT + dim_campaigns + dim_products + dim_locations
-- Purpose: Marketing team — campaign performance, product analysis
-- =============================================

CREATE OR REPLACE VIEW report.view_marketing_dashboard AS
SELECT
    f.date_key                          AS date,
    dd.month                            AS month,
    dd.quarter                          AS quarter,
    dd.year                             AS year,
    f.city_id                           AS province_id,
    dl.province_name                    AS province,
    dl.region_name                      AS region,
    f.product_id                        AS product_id,
    dp.category_name                    AS parent_category,
    dp.subcategory_name                 AS subcategory,
    dp.product_name                     AS product_name,
    f.campaign_key                      AS campaign_id,
    dc.campaign_title                   AS campaign_title,
    sum(f.gmv)                          AS total_gmv,
    sum(f.net_revenue)                  AS total_net_revenue,
    sum(f.quantity)                     AS quantity_sold,
    sum(f.order_count)                  AS product_order_count,
    sum(f.net_revenue) / nullIf(sum(f.order_count), 0)
                                        AS product_aov_ex_vat,
    if(sum(f.quantity) > 0,
        sum(f.gmv) / sum(f.quantity),
        0
    )                                   AS avg_product_price
FROM gold.FACT_SALES_PRODUCT AS f
LEFT JOIN gold.dim_date AS dd ON f.date_key = dd.date_key
LEFT JOIN gold.dim_locations AS dl ON f.city_id = dl.province_id
LEFT JOIN gold.dim_products AS dp ON f.product_id = dp.product_id
LEFT JOIN gold.dim_campaigns AS dc ON f.campaign_key = dc.campaign_id
GROUP BY
    date, month, quarter, year,
    province_id, province, region,
    product_id, parent_category, subcategory, product_name,
    campaign_id, campaign_title;

-- =============================================
-- 2. view_overview_sales
-- Source: FACT_SALES_PRODUCT + dim_products
-- Purpose: Sales overview — revenue, quantity, by product/category/city
-- =============================================

CREATE OR REPLACE VIEW report.view_overview_sales AS
SELECT
    f.date_key                          AS date,
    dd.month                            AS month,
    dd.year                             AS year,
    f.city_id                           AS province_id,
    dl.province_name                    AS province,
    f.product_id                        AS product_id,
    dp.product_name                     AS product,
    dp.category_name                    AS category,
    sum(f.gmv)                          AS total_gmv,
    sum(f.net_revenue)                  AS total_net_revenue,
    sum(f.quantity)                     AS total_quantity_sold,
    sum(f.order_count)                  AS product_order_count
FROM gold.FACT_SALES_PRODUCT AS f
LEFT JOIN gold.dim_date AS dd ON f.date_key = dd.date_key
LEFT JOIN gold.dim_locations AS dl ON f.city_id = dl.province_id
LEFT JOIN gold.dim_products AS dp ON f.product_id = dp.product_id
GROUP BY
    date, month, year,
    province_id, province, product_id, product, category;

-- =============================================
-- 3. view_overview_orders
-- Source: FACT_ORDER_OVERVIEW + dim_order_status
-- Purpose: Order monitoring — count, revenue by status/method
-- =============================================

CREATE OR REPLACE VIEW report.view_overview_orders AS
SELECT
    f.date_key                          AS date,
    dd.month                            AS month,
    f.city_id                           AS province_id,
    dl.province_name                    AS province,
    f.order_status_id                   AS order_status_id,
    dos.name                            AS order_status,
    f.payment_method                    AS payment_method,
    f.shipping_method                   AS shipping_method,
    sum(f.order_count)                  AS order_count,
    sum(f.total_gmv)                    AS order_revenue
FROM gold.FACT_ORDER_OVERVIEW AS f
LEFT JOIN gold.dim_date AS dd ON f.date_key = dd.date_key
LEFT JOIN gold.dim_locations AS dl ON f.city_id = dl.province_id
LEFT JOIN gold.dim_order_status AS dos ON f.order_status_id = dos.id
GROUP BY
    date, month, province_id, province,
    order_status_id, order_status, payment_method, shipping_method;

-- =============================================
-- 4. view_overview_users
-- Source: FACT_USER_REGISTRATION
-- Purpose: User growth tracking — new registrations per day/month
-- =============================================

CREATE OR REPLACE VIEW report.view_overview_users AS
SELECT
    registration_date                   AS registration_date,
    toMonth(registration_date)          AS month,
    sum(user_amount)                    AS new_user_count
FROM gold.FACT_USER_REGISTRATION
GROUP BY registration_date;

-- Overall delivered-order AOV uses one order per denominator, regardless of
-- how many products it contains. Product-level order counts are non-additive.
CREATE OR REPLACE VIEW report.view_delivered_order_aov AS
SELECT
    toDate(toTimeZone(created_at, 'Asia/Ho_Chi_Minh')) AS date,
    city_id,
    uniqExact(order_id) AS delivered_order_count,
    sum(gmv) AS gmv_ex_vat,
    sum(gmv - discount_ex_vat) AS net_sales_ex_vat,
    sum(gmv - discount_ex_vat) / nullIf(uniqExact(order_id), 0) AS aov_ex_vat
FROM gold.sales_line_measured
GROUP BY date, city_id;
