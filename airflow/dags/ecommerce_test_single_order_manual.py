"""
DAG: ecommerce_test_single_order_manual
Schedule: None (manual trigger only)
Purpose: Generate exactly 1 order for end-to-end pipeline validation.
         Used to test: PostgreSQL → Debezium → Kafka → ClickHouse → Metabase
         Trigger manually from Airflow UI and trace the order through all layers.
One task commits the order, line items, transaction and status history together.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import random
import logging
import os

from helpers.db_helpers import create_complete_order, fetch_all
from helpers.faker_generators import FakeDataGenerator

logger = logging.getLogger(__name__)


def generate_one_order(ti=None, **kwargs):
    """
    Generate 1 order and its line items, pushing the order_id to XCom.
    """
    gen = FakeDataGenerator()
    seed = os.getenv("ECOMMERCE_RANDOM_SEED")
    if seed is not None:
        random.seed(int(seed))

    order_by = "u.id, a.id" if seed is not None else "RANDOM()"
    users_with_addresses = fetch_all(
        "SELECT u.id AS user_id, a.id AS address_id "
        "FROM users u "
        "INNER JOIN addresses a ON u.id = a.user_id "
        f"ORDER BY {order_by} LIMIT 50"
    )
    if not users_with_addresses:
        raise ValueError("No users with addresses found. Run user_registration DAG first.")

    products = fetch_all(
        "SELECT id, product_price FROM products WHERE product_price > 0 ORDER BY id"
    )
    if not products:
        raise ValueError("No products found. Run product DAG first.")

    payment_methods = fetch_all("SELECT id, payment_method_name FROM payment_methods")
    shipping_methods = fetch_all("SELECT id FROM shipping_methods")
    order_statuses = fetch_all("SELECT id, order_status_name FROM order_status")
    payment_statuses = fetch_all("SELECT id, payment_status_name FROM payment_status")
    shipping_statuses = fetch_all("SELECT id, shipping_status_name FROM shipping_status")
    discounts = fetch_all("SELECT id, type, value FROM discounts")

    pm_ids = [r["id"] for r in payment_methods]
    sm_ids = [r["id"] for r in shipping_methods]
    pending_order_id = next(r["id"] for r in order_statuses if r["order_status_name"] == "pending")
    pending_shipping_id = next(r["id"] for r in shipping_statuses if r["shipping_status_name"] == "pending")
    completed_payment_id = next(r["id"] for r in payment_statuses if r["payment_status_name"] == "completed")
    pending_payment_id = next(r["id"] for r in payment_statuses if r["payment_status_name"] == "pending")
    method_names = {r["id"]: r["payment_method_name"] for r in payment_methods}

    user_addr = random.choice(users_with_addresses)
    discount = random.choice(discounts) if discounts and random.random() > 0.5 else None

    order_data, order_details = gen.generate_order(
        user_id=user_addr["user_id"],
        address_id=user_addr["address_id"],
        product_list=products,
        payment_method_ids=pm_ids,
        shipping_method_ids=sm_ids,
        order_status_ids=[pending_order_id],
        payment_status_ids=[completed_payment_id],
        shipping_status_ids=[pending_shipping_id],
        discount=discount,
    )

    if method_names[order_data["payment_method_id"]] == "COD":
        order_data["payment_status_id"] = pending_payment_id
        order_data["payment_completed"] = False
    else:
        order_data["payment_completed"] = True

    order_id = create_complete_order(order_data, order_details)

    logger.info("Generated complete order ID=%s with %s items", order_id, len(order_details))

    if ti:
        ti.xcom_push(key="order_id", value=order_id)
    return order_id


with DAG(
    dag_id="ecommerce_test_single_order_manual",
    description="Manual trigger: generate 1 order for end-to-end pipeline testing",
    schedule=None,  # Manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ecommerce", "test", "manual"],
) as dag:

    PythonOperator(
        task_id="generate_one_order",
        python_callable=generate_one_order,
    )
