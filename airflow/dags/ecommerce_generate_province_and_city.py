"""
DAG: ecommerce_generate_province_and_city
Schedule: @once (run only once during system initialization)
Purpose: Seed 8 regions and 63 provinces of Vietnam into the OLTP database.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from helpers.db_helpers import execute_values_insert, fetch_all
from helpers.faker_generators import FakeDataGenerator


def generate_regions(**kwargs):
    """Insert 8 Vietnam regions."""
    gen = FakeDataGenerator()
    regions = gen.generate_regions()

    execute_values_insert(
        "INSERT INTO regions (region_name) VALUES %s ON CONFLICT DO NOTHING",
        regions,
    )
    return f"Inserted {len(regions)} regions"


def generate_provinces(**kwargs):
    """Insert 63 provinces, linked to their region IDs."""
    # Fetch region IDs from DB
    rows = fetch_all("SELECT id, region_name FROM regions")
    region_id_map = {row["region_name"]: row["id"] for row in rows}

    gen = FakeDataGenerator()
    provinces = gen.generate_provinces(region_id_map)

    execute_values_insert(
        "INSERT INTO provinces (province_name, region_id, latitude, longitude) "
        "VALUES %s ON CONFLICT (province_name) DO NOTHING",
        provinces,
    )
    return f"Inserted {len(provinces)} provinces"


with DAG(
    dag_id="ecommerce_generate_province_and_city",
    description="Seed Vietnam regions and provinces (run once)",
    schedule="@once",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ecommerce", "seed", "foundation"],
) as dag:

    task_regions = PythonOperator(
        task_id="generate_regions",
        python_callable=generate_regions,
    )

    task_provinces = PythonOperator(
        task_id="generate_provinces",
        python_callable=generate_provinces,
    )

    task_regions >> task_provinces
