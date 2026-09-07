"""
DAG: ecommerce_test_single_order_manual
Schedule: None (manual trigger only)
Purpose: Generate exactly 1 order for end-to-end pipeline validation.
         Used to test: PostgreSQL → Debezium → Kafka → ClickHouse → Metabase
         Trigger manually from Airflow UI and trace the order through all layers.
Tasks:
  1. generate_one_order: Create 1 order + line items (orderdetails) + status history.
  2. generate_transaction: Record payment transaction for the created order.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import random
import logging

from helpers.db_helpers import execute_query, execute_values_insert, fetch_all
from helpers.faker_generators import FakeDataGenerator

logger = logging.getLogger(__name__)


def generate_one_order(ti=None, **kwargs):
    """
    Generate 1 order and its line items, pushing the order_id to XCom.
    """
    gen = FakeDataGenerator()

    users_with_addresses = fetch_all(
        "SELECT u.id AS user_id, a.id AS address_id "
        "FROM users u "
        "INNER JOIN addresses a ON u.id = a.user_id "
        "ORDER BY RANDOM() LIMIT 50"
    )
    if not users_with_addresses:
        raise ValueError("No users with addresses found. Run user_registration DAG first.")

    products = fetch_all(
        "SELECT id, product_price FROM products WHERE product_price > 0"
    )
    if not products:
        raise ValueError("No products found. Run product DAG first.")

    payment_methods = fetch_all("SELECT id FROM payment_methods")
    shipping_methods = fetch_all("SELECT id FROM shipping_methods")
    order_statuses = fetch_all("SELECT id FROM order_status")
    payment_statuses = fetch_all("SELECT id FROM payment_status")
    shipping_statuses = fetch_all("SELECT id FROM shipping_status")
    discounts = fetch_all("SELECT id FROM discounts")

    pm_ids = [r["id"] for r in payment_methods]
    sm_ids = [r["id"] for r in shipping_methods]
    os_ids = [r["id"] for r in order_statuses]
    ps_ids = [r["id"] for r in payment_statuses]
    ss_ids = [r["id"] for r in shipping_statuses]

    user_addr = random.choice(users_with_addresses)
    discount_id = random.choice(discounts)["id"] if discounts and random.random() > 0.5 else None

    order_data, order_details = gen.generate_order(
        user_id=user_addr["user_id"],
        address_id=user_addr["address_id"],
        product_list=products,
        payment_method_ids=pm_ids,
        shipping_method_ids=sm_ids,
        order_status_ids=os_ids,
        payment_status_ids=ps_ids,
        shipping_status_ids=ss_ids,
        discount_id=discount_id,
    )

    # 1. Insert order
    result = execute_query(
        """
        INSERT INTO orders (
            user_id, staff_id, address_id,
            order_amount, discount_amount, total_amount,
            discount_id, payment_method_id, payment_status_id,
            order_status_id, shipping_method_id, shipping_status_id
        ) VALUES (
            %(user_id)s, %(staff_id)s, %(address_id)s,
            %(order_amount)s, %(discount_amount)s, %(total_amount)s,
            %(discount_id)s, %(payment_method_id)s, %(payment_status_id)s,
            %(order_status_id)s, %(shipping_method_id)s, %(shipping_status_id)s
        ) RETURNING id
        """,
        order_data,
        fetch=True,
    )
    order_id = result[0]["id"]

    # 2. Insert order details
    details_tuples = [
        (
            order_id,
            d["product_id"],
            d["quantity"],
            d["product_price"],
            d["product_tax"],
            d["subtotal_amount"],
        )
        for d in order_details
    ]
    execute_values_insert(
        "INSERT INTO orderdetails "
        "(order_id, product_id, quantity, product_price, product_tax, subtotal_amount) "
        "VALUES %s",
        details_tuples,
    )

    # 3. Insert order status history
    execute_values_insert(
        "INSERT INTO order_status_history "
        "(order_id, order_status_id, comments) VALUES %s",
        [(order_id, order_data["order_status_id"], f"Order #{order_id} created")],
    )

    logger.info(f"Successfully generated single order ID={order_id} with {len(details_tuples)} items")

    if ti:
        ti.xcom_push(key="order_id", value=order_id)
    return order_id


def generate_transaction(ti=None, **kwargs):
    """
    Insert payment transaction for the generated order.
    """
    order_id = None
    if ti:
        order_id = ti.xcom_pull(task_ids="generate_one_order", key="order_id")

    if not order_id:
        # Fallback: get latest order_id from database
        latest = execute_query("SELECT id FROM orders ORDER BY id DESC LIMIT 1", fetch=True)
        if latest:
            order_id = latest[0]["id"]
        else:
            raise ValueError("No order found to generate transaction for")

    execute_values_insert(
        "INSERT INTO transactions (order_id, transaction_type, status, description) VALUES %s",
        [(order_id, "payment", True, f"Payment for order #{order_id}")],
    )
    logger.info(f"Payment transaction recorded for order ID={order_id}")
    return f"Transaction created for order #{order_id}"


with DAG(
    dag_id="ecommerce_test_single_order_manual",
    description="Manual trigger: generate 1 order for end-to-end pipeline testing",
    schedule=None,  # Manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ecommerce", "test", "manual"],
) as dag:

    task_order = PythonOperator(
        task_id="generate_one_order",
        python_callable=generate_one_order,
    )

    task_tx = PythonOperator(
        task_id="generate_transaction",
        python_callable=generate_transaction,
    )

    task_order >> task_tx
