-- Query the current-state fact to validate the CDC-to-Gold path.
-- The fact is a view, so scheduled INSERTs would duplicate old states.
SELECT count() AS product_rows, sum(gmv) AS gmv
FROM gold.FACT_SALES_PRODUCT;
