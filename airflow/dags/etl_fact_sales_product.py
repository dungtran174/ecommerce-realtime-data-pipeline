"""Validate the current-state sales fact every five minutes.

The fact is a ClickHouse view over the latest CDC state. No periodic INSERT is
needed, so reruns cannot duplicate historical sales.
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

ETL_SQL_PATH = "/opt/airflow/dags/sql/etl_fact_sales_product.sql"


def run_etl(**kwargs):
    """Query the fact through ClickHouse HTTP to detect pipeline failures."""
    import urllib.request
    import urllib.parse

    with open(ETL_SQL_PATH, encoding="utf-8") as sql_file:
        sql = sql_file.read()
    logger.info("Loaded fact validation SQL from %s", ETL_SQL_PATH)

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
            logger.info("Gold fact validation completed. Response: %s", result)
            return "FACT_SALES_PRODUCT validated"
    except Exception as e:
        logger.error("Gold fact validation failed: %s", e)
        raise


with DAG(
    dag_id="etl_fact_sales_product",
    description="Validate the current-state sales fact every 5 minutes",
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
