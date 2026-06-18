"""
DAG: ecommerce_generate_order_process
Schedule: Every 3 minutes (simulation mode)
Purpose: Simulate the complete order lifecycle:
         1. Create order with random products
         2. Insert order details (line items)
         3. Record payment transaction
         4. Update order status history
         This is the core DAG that feeds the entire CDC pipeline.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import random
import logging

from helpers.db_helpers import (
    execute_query, execute_values_insert, fetch_all, fetch_one
)
from helpers.faker_generators import FakeDataGenerator

logger = logging.getLogger(__name__)


def generate_bulk_orders(count=3, **kwargs):
    """
    Generate multiple complete orders with details, transactions, and status history.

    Each order goes through:
    1. Pick a random user + address
    2. Select 1-5 random products
    3. Calculate amounts (subtotal, discount, total)
    4. Insert order → order_details → transaction → status_history
    """
    gen = FakeDataGenerator()

    # Fetch required reference data
    users_with_addresses = fetch_all(
        "SELECT u.id AS user_id, a.id AS address_id "
        "FROM users u "
        "INNER JOIN addresses a ON u.id = a.user_id "
        "ORDER BY RANDOM() LIMIT 50"
    )
    if not users_with_addresses:
        logger.warning("No users with addresses found. Run user_registration DAG first.")
        return "Skipped: no users with addresses"

    products = fetch_all(
        "SELECT id, product_price FROM products WHERE product_price > 0"
    )
    if not products:
        logger.warning("No products found. Run product DAG first.")
        return "Skipped: no products"

    payment_methods = fetch_all("SELECT id FROM payment_methods")
    shipping_methods = fetch_all("SELECT id FROM shipping_methods")
    order_statuses = fetch_all("SELECT id FROM order_status")
    payment_statuses = fetch_all("SELECT id FROM payment_status")
    shipping_statuses = fetch_all("SELECT id FROM shipping_status")

    # Optional: pick a random discount (50% chance of having a discount)
    discounts = fetch_all("SELECT id FROM discounts")

    pm_ids = [r["id"] for r in payment_methods]
    sm_ids = [r["id"] for r in shipping_methods]
    os_ids = [r["id"] for r in order_statuses]
    ps_ids = [r["id"] for r in payment_statuses]
    ss_ids = [r["id"] for r in shipping_statuses]

    orders_created = 0

    for _ in range(count):
        # Pick random user
        user_addr = random.choice(users_with_addresses)
        user_id = user_addr["user_id"]
        address_id = user_addr["address_id"]

        # 50% chance of applying a discount
        discount_id = None
        if discounts and random.random() > 0.5:
            discount_id = random.choice(discounts)["id"]

        # Generate order data
        order_data, order_details = gen.generate_order(
            user_id=user_id,
            address_id=address_id,
            product_list=products,
            payment_method_ids=pm_ids,
            shipping_method_ids=sm_ids,
            order_status_ids=os_ids,
            payment_status_ids=ps_ids,
            shipping_status_ids=ss_ids,
            discount_id=discount_id,
        )

        # 1) Insert order and get order_id
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

        # 2) Insert order details (line items)
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

        # 3) Insert payment transaction
        execute_values_insert(
            "INSERT INTO transactions "
            "(order_id, transaction_type, status, description) VALUES %s",
            [(order_id, "payment", True, f"Payment for order #{order_id}")],
        )

        # 4) Insert order status history
        execute_values_insert(
            "INSERT INTO order_status_history "
            "(order_id, order_status_id, comments) VALUES %s",
            [(order_id, order_data["order_status_id"], f"Order #{order_id} created")],
        )

        orders_created += 1

    return f"Created {orders_created} orders with details, transactions, and status history"


with DAG(
    dag_id="ecommerce_generate_order_process",
    description="Simulate complete order lifecycle every 3 minutes",
    schedule="*/3 * * * *",  # Every 3 minutes for real-time simulation
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "realtime", "simulation", "core"],
) as dag:

    PythonOperator(
        task_id="generate_bulk_orders",
        python_callable=generate_bulk_orders,
        op_kwargs={"count": 3},  # 3 orders per run
    )
