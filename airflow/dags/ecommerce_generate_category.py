"""
DAG: ecommerce_generate_category
Schedule: @weekly
Purpose: Generate product categories with parent-child hierarchy.
         Also publishes an Airflow Dataset to trigger downstream product DAG.
"""

from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator
from datetime import datetime

from helpers.db_helpers import execute_values_insert, fetch_all
from helpers.faker_generators import FakeDataGenerator

# Dataset URI — downstream DAGs can listen for updates
CATEGORY_DATASET = Dataset("postgres://ecommerce/categories")


def generate_parent_categories(**kwargs):
    """Insert 10 parent categories (top-level)."""
    gen = FakeDataGenerator()
    categories = gen.generate_parent_categories()

    execute_values_insert(
        "INSERT INTO categories (category_name, category_id, slug) "
        "VALUES %s ON CONFLICT (slug) DO NOTHING",
        categories,
    )
    return f"Inserted {len(categories)} parent categories"


def generate_sub_categories(**kwargs):
    """Insert sub-categories linked to their parent category IDs."""
    # Fetch parent categories (those without a parent)
    rows = fetch_all(
        "SELECT id, category_name FROM categories WHERE category_id IS NULL"
    )
    parent_id_map = {row["category_name"]: row["id"] for row in rows}

    gen = FakeDataGenerator()
    subs = gen.generate_sub_categories(parent_id_map)

    execute_values_insert(
        "INSERT INTO categories (category_name, category_id, slug) "
        "VALUES %s ON CONFLICT (slug) DO NOTHING",
        subs,
    )
    return f"Inserted {len(subs)} sub-categories"


with DAG(
    dag_id="ecommerce_generate_category",
    description="Generate product categories with parent-child hierarchy",
    schedule="@weekly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ecommerce", "weekly", "business"],
) as dag:

    task_parents = PythonOperator(
        task_id="generate_parent_categories",
        python_callable=generate_parent_categories,
    )

    task_subs = PythonOperator(
        task_id="generate_sub_categories",
        python_callable=generate_sub_categories,
        outlets=[CATEGORY_DATASET],
    )

    task_parents >> task_subs
