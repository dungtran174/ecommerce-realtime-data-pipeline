"""
DAG: etl_fact_sales_product
Schedule: Every 5 minutes
Purpose: Run ETL to populate gold.FACT_SALES_PRODUCT in ClickHouse.
         This cannot be a Materialized View because it requires
         a 3-table JOIN (orders + order_items + products) and data
         may arrive at different times. The 5-minute buffer ensures
         all related records are available before aggregation.
"""

import os
import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_HTTP_PORT = os.getenv("CLICKHOUSE_HTTP_PORT", "8123")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")

ETL_SQL_PATH = "/opt/airflow/plugins/helpers/etl_fact_sales_product.sql"


def run_etl(**kwargs):
    """
    Execute the ETL SQL against ClickHouse via HTTP interface.
    Uses the clickhouse HTTP API (port 8123) — no driver needed.
    """
    import urllib.request
    import urllib.parse

    # Read the SQL file
    sql_path = ETL_SQL_PATH
    # Fallback: read from mounted scripts directory
    if not os.path.exists(sql_path):
        sql_path = "/opt/airflow/dags/etl_fact_sales_product.sql"

    # If SQL file not found, use inline query
    if os.path.exists(sql_path):
        with open(sql_path, "r") as f:
            sql = f.read()
        logger.info(f"Loaded ETL SQL from {sql_path}")
    else:
        logger.warning("SQL file not found, using inline query")
        sql = """
        INSERT INTO gold.FACT_SALES_PRODUCT
        SELECT
            toDate(o.created_at) AS date_key,
            o.city_id,
            oi.product_id,
            o.campaign_key,
            toUInt64(sum(oi.quantity)) AS quantity,
            sum(oi.gmv) AS gmv,
            sum(toDecimal64(oi.quantity, 2) * p.unit_cost) AS total_cost,
            sum(if(o.order_amount > 0,
                (oi.current_price * toDecimal64(oi.quantity, 2) / o.order_amount) * o.discount_amount,
                toDecimal64(0, 2))) AS discount_val,
            sum(oi.gmv) - sum(if(o.order_amount > 0,
                (oi.current_price * toDecimal64(oi.quantity, 2) / o.order_amount) * o.discount_amount,
                toDecimal64(0, 2))) AS net_revenue,
            count(DISTINCT o.order_id) AS order_count
        FROM silver.orders AS o
        INNER JOIN silver.order_items AS oi ON o.order_id = oi.order_id
        INNER JOIN silver.products AS p ON oi.product_id = p.product_id
        WHERE o.order_status_id = 4
          AND toDate(o.created_at) >= today() - 1
        GROUP BY date_key, city_id, product_id, campaign_key
        """

    # Execute via ClickHouse HTTP API
    url = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_HTTP_PORT}/"

    params = {}
    if CLICKHOUSE_USER:
        params["user"] = CLICKHOUSE_USER
    if CLICKHOUSE_PASSWORD:
        params["password"] = CLICKHOUSE_PASSWORD

    query_url = url + "?" + urllib.parse.urlencode(params) if params else url

    req = urllib.request.Request(
        query_url,
        data=sql.encode("utf-8"),
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=60) as response:
            result = response.read().decode("utf-8")
            logger.info(f"ETL completed successfully. Response: {result}")
            return "ETL FACT_SALES_PRODUCT completed"
    except Exception as e:
        logger.error(f"ETL failed: {e}")
        raise


with DAG(
    dag_id="etl_fact_sales_product",
    description="ETL: Populate gold.FACT_SALES_PRODUCT every 5 minutes",
    schedule="*/5 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "etl", "clickhouse", "gold"],
) as dag:

    PythonOperator(
        task_id="run_etl_fact_sales_product",
        python_callable=run_etl,
    )
