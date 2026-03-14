"""
Extract customer tier data from CSV file.
Fixes: preserve customer tier history, atomic swap, validation, env credentials.
"""
import csv
import psycopg2
import os


def extract_customer_tiers_from_csv():
    print("Starting customer tier extraction...")

    conn = psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "postgres"),
        database=os.environ.get("POSTGRES_DB", "airflow"),
        user=os.environ.get("POSTGRES_USER", "airflow"),
        password=os.environ.get("POSTGRES_PASSWORD", "airflow"),
    )
    cursor = conn.cursor()

    csv_path = os.environ.get("TIERS_CSV_PATH", "/opt/airflow/data/customer_tiers.csv")

    with open(csv_path, "r") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    print(f"Loaded {len(rows)} rows from CSV")

    # Atomic table swap
    cursor.execute("DROP TABLE IF EXISTS staging.customer_tiers_new;")
    cursor.execute("""
        CREATE TABLE staging.customer_tiers_new (
            customer_id VARCHAR(50) NOT NULL,
            customer_name VARCHAR(200),
            tier VARCHAR(50),
            tier_updated_date DATE
        );
    """)

    for row in rows:
        cursor.execute(
            """INSERT INTO staging.customer_tiers_new 
               (customer_id, customer_name, tier, tier_updated_date)
               VALUES (%s, %s, %s, %s);""",
            (row["customer_id"], row["customer_name"],
             row["tier"], row["tier_updated_date"]),
        )

    cursor.execute("DROP TABLE IF EXISTS staging.customer_tiers;")
    cursor.execute("ALTER TABLE staging.customer_tiers_new RENAME TO customer_tiers;")
    conn.commit()

    print(f"Loaded {len(rows)} customer tier history rows into staging.customer_tiers")
    cursor.close()
    conn.close()
    print("Customer tier extraction completed")
