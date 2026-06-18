-- =============================================================================
-- ETL: FACT_SALES_PRODUCT
-- Runs every 5 minutes via Airflow DAG
-- Joins: silver.orders + silver.order_items + silver.products
-- Filter: Only delivered orders (order_status_id = 4)
-- =============================================================================
--
-- Discount allocation formula per product:
--   discount_item = (unit_price × qty / order_amount) × discount_amount
--
-- This ensures discounts are proportionally distributed across line items
-- based on their contribution to the total order value.
-- =============================================================================

INSERT INTO gold.FACT_SALES_PRODUCT
SELECT
    toDate(o.created_at)                                    AS date_key,
    o.city_id                                               AS city_id,
    oi.product_id                                           AS product_id,
    o.campaign_key                                          AS campaign_key,
    -- Measures
    toUInt64(sum(oi.quantity))                               AS quantity,
    sum(oi.gmv)                                             AS gmv,
    sum(toDecimal64(oi.quantity, 2) * p.unit_cost)          AS total_cost,
    -- Discount allocation: proportional to each item's share of order total
    sum(
        if(o.order_amount > 0,
            (oi.current_price * toDecimal64(oi.quantity, 2) / o.order_amount) * o.discount_amount,
            toDecimal64(0, 2)
        )
    )                                                       AS discount_val,
    -- Net revenue = GMV - discount
    sum(oi.gmv) - sum(
        if(o.order_amount > 0,
            (oi.current_price * toDecimal64(oi.quantity, 2) / o.order_amount) * o.discount_amount,
            toDecimal64(0, 2)
        )
    )                                                       AS net_revenue,
    count(DISTINCT o.order_id)                              AS order_count
FROM silver.orders AS o
INNER JOIN silver.order_items AS oi ON o.order_id = oi.order_id
INNER JOIN silver.products AS p ON oi.product_id = p.product_id
WHERE o.order_status_id = 4  -- delivered only
  AND toDate(o.created_at) >= today() - 1  -- only process recent data
GROUP BY
    date_key, city_id, product_id, campaign_key;
