"""
DAG: ecommerce_generate_discount_and_campaign
Schedule: @weekly
Purpose: Generate marketing campaigns and their associated discount codes.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from helpers.db_helpers import execute_values_insert, fetch_all
from helpers.faker_generators import FakeDataGenerator


def generate_campaigns(**kwargs):
    """Create 3 new marketing campaigns."""
    gen = FakeDataGenerator()
    campaigns = gen.generate_campaigns(count=3)

    execute_values_insert(
        "INSERT INTO ads_campaigns (campaign_title, started_at, expired_at) VALUES %s",
        campaigns,
    )
    return f"Created {len(campaigns)} campaigns"


def generate_discounts(**kwargs):
    """Create discount codes linked to the latest campaigns."""
    # Fetch the 3 most recent campaigns
    rows = fetch_all(
        "SELECT id FROM ads_campaigns ORDER BY id DESC LIMIT 3"
    )
    campaign_ids = [row["id"] for row in rows]

    if not campaign_ids:
        return "No campaigns found, skipping discounts"

    gen = FakeDataGenerator()
    discounts = gen.generate_discounts(campaign_ids, count_per_campaign=2)

    execute_values_insert(
        "INSERT INTO discounts "
        "(adscampaign_id, type, value, code, started_at, expired_at) VALUES %s",
        discounts,
    )
    return f"Created {len(discounts)} discount codes"


with DAG(
    dag_id="ecommerce_generate_discount_and_campaign",
    description="Generate campaigns and discount codes weekly",
    schedule="@weekly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ecommerce", "weekly", "marketing"],
) as dag:

    task_campaigns = PythonOperator(
        task_id="generate_campaigns",
        python_callable=generate_campaigns,
    )

    task_discounts = PythonOperator(
        task_id="generate_discounts",
        python_callable=generate_discounts,
    )

    task_campaigns >> task_discounts
