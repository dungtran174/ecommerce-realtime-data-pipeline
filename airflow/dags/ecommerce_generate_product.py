"""
DAG: ecommerce_generate_product
Schedule: Dataset-triggered (runs when brands OR categories are updated)
Purpose: Generate new products and assign random tags.
         This DAG demonstrates Airflow's event-driven scheduling.
"""

from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator
from datetime import datetime
import random

from helpers.db_helpers import execute_values_insert, fetch_all
from helpers.faker_generators import FakeDataGenerator

# Listen for updates from brand and category DAGs
BRAND_DATASET = Dataset("postgres://ecommerce/brands")
CATEGORY_DATASET = Dataset("postgres://ecommerce/categories")


def generate_products(**kwargs):
    """Generate 10 new products linked to existing categories and brands."""
    # Fetch sub-categories (those WITH a parent)
    categories = fetch_all(
        "SELECT id FROM categories WHERE category_id IS NOT NULL"
    )
    category_ids = [row["id"] for row in categories]

    # Fetch all brands
    brands = fetch_all("SELECT id FROM brands")
    brand_ids = [row["id"] for row in brands]

    if not category_ids or not brand_ids:
        return "No categories or brands found, skipping product generation"

    gen = FakeDataGenerator()
    products = gen.generate_products(category_ids, brand_ids, count=10)

    execute_values_insert(
        "INSERT INTO products "
        "(product_name, category_id, brand_id, product_price, unit_cost, product_quantity) "
        "VALUES %s",
        products,
    )
    return f"Generated {len(products)} products"


def assign_product_tags(**kwargs):
    """Assign 1-3 random tags to each newly created product."""
    # Fetch products that have no tags yet
    products_without_tags = fetch_all(
        "SELECT p.id FROM products p "
        "LEFT JOIN product_tag pt ON p.id = pt.product_id "
        "WHERE pt.id IS NULL"
    )
    if not products_without_tags:
        return "All products already have tags"

    tags = fetch_all("SELECT id FROM tags")
    tag_ids = [row["id"] for row in tags]

    if not tag_ids:
        return "No tags found, skipping"

    product_tags = []
    for product in products_without_tags:
        # Assign 1-3 random tags per product
        num_tags = random.randint(1, min(3, len(tag_ids)))
        selected_tags = random.sample(tag_ids, num_tags)
        for tag_id in selected_tags:
            product_tags.append((product["id"], tag_id))

    execute_values_insert(
        "INSERT INTO product_tag (product_id, tag_id) VALUES %s",
        product_tags,
    )
    return f"Assigned tags to {len(products_without_tags)} products"


with DAG(
    dag_id="ecommerce_generate_product",
    description="Generate products when brands or categories are updated",
    schedule=[BRAND_DATASET, CATEGORY_DATASET],
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ecommerce", "event-driven", "business"],
) as dag:

    task_products = PythonOperator(
        task_id="generate_products",
        python_callable=generate_products,
    )

    task_tags = PythonOperator(
        task_id="assign_product_tags",
        python_callable=assign_product_tags,
    )

    task_products >> task_tags
