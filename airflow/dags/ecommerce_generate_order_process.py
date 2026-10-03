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
import os

from helpers.db_helpers import create_complete_order, fetch_all, get_connection
from helpers.faker_generators import FakeDataGenerator

logger = logging.getLogger(__name__)


def advance_order_lifecycle(limit=50):
    """Move existing orders one valid fulfillment step per scheduler run."""
    transitions = {
        "pending": ("confirmed", "pending"),
        "confirmed": ("shipping", "picked_up"),
        "shipping": ("delivered", "delivered"),
    }
    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """SELECT o.id, os.order_status_name, pm.payment_method_name
                       FROM orders o
                       JOIN order_status os ON o.order_status_id = os.id
                       JOIN payment_methods pm ON o.payment_method_id = pm.id
                       WHERE os.order_status_name IN ('pending', 'confirmed', 'shipping')
                       ORDER BY o.id LIMIT %s FOR UPDATE OF o SKIP LOCKED""",
                    (limit,),
                )
                orders = cur.fetchall()
                for order_id, current, method in orders:
                    next_status, next_shipping = transitions[current]
                    cod_delivered = method == "COD" and next_status == "delivered"
                    cur.execute(
                        """UPDATE orders SET
                             order_status_id = (SELECT id FROM order_status WHERE order_status_name=%s),
                             shipping_status_id = (SELECT id FROM shipping_status WHERE shipping_status_name=%s),
                             payment_status_id = CASE WHEN %s THEN
                                 (SELECT id FROM payment_status WHERE payment_status_name='completed')
                                 ELSE payment_status_id END,
                             shipped_at = CASE WHEN %s THEN now() ELSE shipped_at END,
                             updated_at = now()
                           WHERE id=%s""",
                        (next_status, next_shipping, cod_delivered,
                         next_status == "shipping", order_id),
                    )
                    if cod_delivered:
                        cur.execute(
                            """UPDATE transactions SET status=TRUE,
                               description=%s WHERE order_id=%s AND transaction_type='payment'""",
                            (f"COD payment received for order #{order_id}", order_id),
                        )
                    cur.execute(
                        """INSERT INTO order_status_history
                           (order_id, order_status_id, comments)
                           VALUES (%s,
                             (SELECT id FROM order_status WHERE order_status_name=%s), %s)""",
                        (order_id, next_status, f"Order #{order_id} moved to {next_status}"),
                    )
        return len(orders)
    finally:
        conn.close()


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
    seed = os.getenv("ECOMMERCE_RANDOM_SEED")
    if seed is not None:
        random.seed(int(seed))
    advanced = advance_order_lifecycle()
    logger.info("Advanced %s existing orders by one status", advanced)

    # Fetch required reference data
    order_by = "u.id, a.id" if seed is not None else "RANDOM()"
    users_with_addresses = fetch_all(
        "SELECT u.id AS user_id, a.id AS address_id "
        "FROM users u "
        "INNER JOIN addresses a ON u.id = a.user_id "
        f"ORDER BY {order_by} LIMIT 50"
    )
    if not users_with_addresses:
        logger.warning("No users with addresses found. Run user_registration DAG first.")
        return "Skipped: no users with addresses"

    products = fetch_all(
        "SELECT id, product_price FROM products WHERE product_price > 0 ORDER BY id"
    )
    if not products:
        logger.warning("No products found. Run product DAG first.")
        return "Skipped: no products"

    payment_methods = fetch_all("SELECT id, payment_method_name FROM payment_methods")
    shipping_methods = fetch_all("SELECT id FROM shipping_methods")
    order_statuses = fetch_all("SELECT id, order_status_name FROM order_status")
    payment_statuses = fetch_all("SELECT id, payment_status_name FROM payment_status")
    shipping_statuses = fetch_all("SELECT id, shipping_status_name FROM shipping_status")

    # Optional: pick a random discount (50% chance of having a discount)
    discounts = fetch_all("SELECT id, type, value FROM discounts")

    pm_ids = [r["id"] for r in payment_methods]
    sm_ids = [r["id"] for r in shipping_methods]
    pending_order_id = next(r["id"] for r in order_statuses if r["order_status_name"] == "pending")
    pending_shipping_id = next(r["id"] for r in shipping_statuses if r["shipping_status_name"] == "pending")
    completed_payment_id = next(r["id"] for r in payment_statuses if r["payment_status_name"] == "completed")
    pending_payment_id = next(r["id"] for r in payment_statuses if r["payment_status_name"] == "pending")
    method_names = {r["id"]: r["payment_method_name"] for r in payment_methods}

    orders_created = 0

    for _ in range(count):
        # Pick random user
        user_addr = random.choice(users_with_addresses)
        user_id = user_addr["user_id"]
        address_id = user_addr["address_id"]

        # 50% chance of applying a discount
        discount = None
        if discounts and random.random() > 0.5:
            discount = random.choice(discounts)

        # Generate order data
        order_data, order_details = gen.generate_order(
            user_id=user_id,
            address_id=address_id,
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

        create_complete_order(order_data, order_details)

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
