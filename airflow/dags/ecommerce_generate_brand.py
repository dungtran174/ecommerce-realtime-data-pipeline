"""
DAG: ecommerce_generate_brand
Schedule: @weekly
Purpose: Generate new product brands periodically to simulate business growth.
         Also publishes an Airflow Dataset to trigger downstream product DAG.
"""

from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator
from datetime import datetime

from helpers.db_helpers import execute_values_insert
from helpers.faker_generators import FakeDataGenerator

# Dataset URI — downstream DAGs can listen for updates
BRAND_DATASET = Dataset("postgres://ecommerce/brands")


def generate_brands(**kwargs):
    """Generate 3-5 new brands per week."""
    gen = FakeDataGenerator()
    count = 5
    brands = gen.generate_brands(count=count)

    execute_values_insert(
        "INSERT INTO brands (brand_name) VALUES %s ON CONFLICT (brand_name) DO NOTHING",
        brands,
    )
    return f"Generated {count} brands"


with DAG(
    dag_id="ecommerce_generate_brand",
    description="Generate new brands weekly",
    schedule="@weekly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ecommerce", "weekly", "business"],
) as dag:

    PythonOperator(
        task_id="generate_brands",
        python_callable=generate_brands,
        outlets=[BRAND_DATASET],  # Notify downstream when brands are updated
    )
