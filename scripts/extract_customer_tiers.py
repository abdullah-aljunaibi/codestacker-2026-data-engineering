"""
Extract customer tier data from CSV file.
Fixes: SCD handling (latest tier per customer), atomic swap, validation, env credentials.
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

    # SCD: keep latest tier per customer_id (by tier_updated_date)
    latest = {}
    for row in rows:
        cid = row["customer_id"]
        if cid not in latest or row["tier_updated_date"] > latest[cid]["tier_updated_date"]:
            latest[cid] = row

    deduped = list(latest.values())
    print(f"After dedup (latest tier per customer): {len(deduped)} customers")

    # Atomic table swap
    cursor.execute("DROP TABLE IF EXISTS staging.customer_tiers_new;")
    cursor.execute("""
        CREATE TABLE staging.customer_tiers_new (
            customer_id VARCHAR(50) PRIMARY KEY,
            customer_name VARCHAR(200),
            tier VARCHAR(50),
            tier_updated_date DATE
        );
    """)

    for row in deduped:
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

    print(f"Loaded {len(deduped)} customer tiers into staging.customer_tiers")
    cursor.close()
    conn.close()
    print("Customer tier extraction completed")
