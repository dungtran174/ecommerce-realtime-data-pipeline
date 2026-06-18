"""
DAG: ecommerce_test_single_order_manual
Schedule: None (manual trigger only)
Purpose: Generate exactly 1 order for end-to-end pipeline validation.
         Used to test: PostgreSQL → Debezium → Kafka → ClickHouse → Metabase
         Trigger manually from Airflow UI and trace the order through all layers.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from ecommerce_generate_order_process import generate_bulk_orders


with DAG(
    dag_id="ecommerce_test_single_order_manual",
    description="Manual trigger: generate 1 order for end-to-end pipeline testing",
    schedule=None,  # Manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ecommerce", "test", "manual"],
) as dag:

    PythonOperator(
        task_id="generate_single_order",
        python_callable=generate_bulk_orders,
        op_kwargs={"count": 1},
    )
