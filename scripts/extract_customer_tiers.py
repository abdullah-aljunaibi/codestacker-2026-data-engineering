"""
Extract customer tier data from CSV file.
Fixes: preserve customer tier history, atomic swap, validation, env credentials.
"""
import csv
import os
from datetime import date

import psycopg2


ALLOWED_TIERS = {"Bronze", "Silver", "Gold", "Platinum"}


def _validate_customer_tier_rows(rows):
    seen_history_keys = set()

    for index, row in enumerate(rows, start=2):
        customer_id = (row.get("customer_id") or "").strip()
        tier = (row.get("tier") or "").strip()
        tier_updated_date_raw = (row.get("tier_updated_date") or "").strip()

        if not customer_id:
            raise ValueError(f"Row {index}: missing customer_id")

        if not tier:
            raise ValueError(f"Row {index}: invalid tier value ''")

        if tier not in ALLOWED_TIERS:
            raise ValueError(f"Row {index}: invalid tier value '{tier}'")

        if not tier_updated_date_raw:
            raise ValueError(f"Row {index}: missing tier_updated_date")

        try:
            tier_updated_date = date.fromisoformat(tier_updated_date_raw)
        except ValueError as exc:
            raise ValueError(
                f"Row {index}: invalid tier_updated_date '{tier_updated_date_raw}'"
            ) from exc

        history_key = (customer_id, tier_updated_date)
        if history_key in seen_history_keys:
            raise ValueError(
                "Duplicate same-day history rows for "
                f"customer_id '{customer_id}' on {tier_updated_date.isoformat()}"
            )

        seen_history_keys.add(history_key)


def extract_customer_tiers_from_csv():
    print("Starting customer tier extraction...")

    csv_path = os.environ.get("TIERS_CSV_PATH", "/opt/airflow/data/customer_tiers.csv")

    with open(csv_path, "r") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    print(f"Loaded {len(rows)} rows from CSV")
    _validate_customer_tier_rows(rows)

    conn = psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "postgres"),
        database=os.environ.get("POSTGRES_DB", "airflow"),
        user=os.environ.get("POSTGRES_USER", "airflow"),
        password=os.environ.get("POSTGRES_PASSWORD", "airflow"),
    )
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS staging.customer_tiers (
            customer_id VARCHAR(50) NOT NULL,
            customer_name VARCHAR(200),
            tier VARCHAR(50) NOT NULL,
            tier_updated_date DATE NOT NULL,
            CONSTRAINT customer_tiers_customer_id_tier_updated_date_key
                UNIQUE (customer_id, tier_updated_date),
            CONSTRAINT customer_tiers_tier_check
                CHECK (tier IN ('Bronze', 'Silver', 'Gold', 'Platinum'))
        );
    """)
    cursor.execute("TRUNCATE TABLE staging.customer_tiers;")

    for row in rows:
        cursor.execute(
            """INSERT INTO staging.customer_tiers
               (customer_id, customer_name, tier, tier_updated_date)
               VALUES (%s, %s, %s, %s);""",
            (row["customer_id"], row["customer_name"],
             row["tier"], row["tier_updated_date"]),
        )

    conn.commit()

    print(f"Loaded {len(rows)} customer tier history rows into staging.customer_tiers")
    cursor.close()
    conn.close()
    print("Customer tier extraction completed")
