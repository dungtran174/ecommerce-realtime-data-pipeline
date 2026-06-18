"""
DAG: ecommerce_user_registration
Schedule: Every 2 minutes (simulation mode) or @daily (backfill mode)
Purpose: Simulate new user registrations and assign addresses.
         Supports two modes:
         - Backfill: Generate bulk users for historical data
         - Simulation: Generate small batches continuously for real-time demo
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import random

from helpers.db_helpers import execute_values_insert, fetch_all, execute_query
from helpers.faker_generators import FakeDataGenerator


def register_users(**kwargs):
    """Generate and insert new user accounts."""
    gen = FakeDataGenerator()
    users = gen.generate_users(count=5)

    execute_values_insert(
        "INSERT INTO users (username, password, email, mobile) "
        "VALUES %s ON CONFLICT DO NOTHING",
        users,
    )
    return f"Registered {len(users)} new users"


def assign_roles(**kwargs):
    """Assign 'user' role to all users who don't have a role yet."""
    # Get the 'user' role ID
    role = execute_query(
        "SELECT id FROM roles WHERE role_name = 'user'",
        fetch=True,
    )
    if not role:
        return "Role 'user' not found"

    user_role_id = role[0]["id"]

    # Find users without any role
    users_without_role = fetch_all(
        "SELECT u.id FROM users u "
        "LEFT JOIN role_user ru ON u.id = ru.user_id "
        "WHERE ru.id IS NULL"
    )

    if not users_without_role:
        return "All users already have roles"

    role_assignments = [(u["id"], user_role_id) for u in users_without_role]

    execute_values_insert(
        "INSERT INTO role_user (user_id, role_id) VALUES %s",
        role_assignments,
    )
    return f"Assigned roles to {len(role_assignments)} users"


def create_addresses(**kwargs):
    """Create a delivery address for users who don't have one."""
    gen = FakeDataGenerator()

    # Fetch users without addresses
    users_without_addr = fetch_all(
        "SELECT u.id FROM users u "
        "LEFT JOIN addresses a ON u.id = a.user_id "
        "WHERE a.id IS NULL"
    )

    if not users_without_addr:
        return "All users already have addresses"

    # Fetch all provinces
    provinces = fetch_all("SELECT id, region_id FROM provinces")
    if not provinces:
        return "No provinces found, run province DAG first"

    addresses = []
    for user in users_without_addr:
        province = random.choice(provinces)
        addresses.append((
            "Home",
            user["id"],
            province["id"],
            province["region_id"],
            gen.fake.address(),
        ))

    execute_values_insert(
        "INSERT INTO addresses (title, user_id, province_id, region_id, full_address) "
        "VALUES %s",
        addresses,
    )
    return f"Created {len(addresses)} addresses"


with DAG(
    dag_id="ecommerce_user_registration",
    description="Simulate user registrations with roles and addresses",
    schedule="*/2 * * * *",  # Every 2 minutes for real-time simulation
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "realtime", "simulation"],
) as dag:

    task_users = PythonOperator(
        task_id="register_users",
        python_callable=register_users,
    )

    task_roles = PythonOperator(
        task_id="assign_roles",
        python_callable=assign_roles,
    )

    task_addresses = PythonOperator(
        task_id="create_addresses",
        python_callable=create_addresses,
    )

    task_users >> task_roles >> task_addresses
