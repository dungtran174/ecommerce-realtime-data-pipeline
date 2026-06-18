"""
DAG: ecommerce_generate_tag
Schedule: @once (run only once during system initialization)
Purpose: Seed product tags into the OLTP database.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from helpers.db_helpers import execute_values_insert
from helpers.faker_generators import FakeDataGenerator


def generate_tags(**kwargs):
    """Insert predefined product tags."""
    gen = FakeDataGenerator()
    tags = gen.generate_tags()

    execute_values_insert(
        "INSERT INTO tags (tag_name) VALUES %s ON CONFLICT (tag_name) DO NOTHING",
        tags,
    )
    return f"Inserted {len(tags)} tags"


with DAG(
    dag_id="ecommerce_generate_tag",
    description="Seed product tags (run once)",
    schedule="@once",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ecommerce", "seed", "foundation"],
) as dag:

    PythonOperator(
        task_id="generate_tags",
        python_callable=generate_tags,
    )
