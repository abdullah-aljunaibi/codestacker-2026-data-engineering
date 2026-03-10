"""
Extract shipment data from the external API.
Fixes: parameterized SQL, retry logic, data validation, dedup, atomic swap, env credentials.
"""
import requests
import psycopg2
import os
import time


def extract_shipments_from_api():
    print("Starting shipment data extraction...")

    conn = psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "postgres"),
        database=os.environ.get("POSTGRES_DB", "airflow"),
        user=os.environ.get("POSTGRES_USER", "airflow"),
        password=os.environ.get("POSTGRES_PASSWORD", "airflow"),
    )
    cursor = conn.cursor()

    api_url = os.environ.get("API_URL", "http://api:8000/api/shipments")

    # Retry logic for flaky API (returns 500 intermittently)
    shipments = None
    for attempt in range(1, 4):
        try:
            print(f"  API request attempt {attempt}/3...")
            response = requests.get(api_url, timeout=30)
            response.raise_for_status()
            data = response.json()
            shipments = data["data"]
            break
        except Exception as e:
            print(f"  Attempt {attempt} failed: {e}")
            if attempt < 3:
                print("  Retrying in 5s...")
                time.sleep(5)
            else:
                raise RuntimeError(f"API failed after 3 attempts: {e}")

    print(f"Fetched {len(shipments)} raw shipments from API")

    # Validate and filter
    valid = []
    rejected = 0
    for s in shipments:
        sid = s.get("shipment_id", "???")
        cost = s.get("shipping_cost")
        cid = s.get("customer_id")
        status = s.get("status", "")

        if cost is not None and float(cost) < 0:
            print(f"  REJECTED: {sid}: negative shipping_cost ({cost})")
            rejected += 1
            continue
        if not cid:
            print(f"  REJECTED: {sid}: missing customer_id")
            rejected += 1
            continue
        if status and status.lower() == "cancelled":
            print(f"  REJECTED: {sid}: cancelled shipment excluded")
            rejected += 1
            continue
        valid.append(s)

    print(f"Valid: {len(valid)}, Rejected: {rejected}")

    # Deduplicate by shipment_id (keep last occurrence)
    seen = {}
    for s in valid:
        seen[s["shipment_id"]] = s
    deduped = list(seen.values())
    print(f"After deduplication: {len(deduped)} unique shipments")

    # Atomic table swap
    cursor.execute("DROP TABLE IF EXISTS staging.shipments_new;")
    cursor.execute("""
        CREATE TABLE staging.shipments_new (
            shipment_id VARCHAR(50) PRIMARY KEY,
            customer_id VARCHAR(50) NOT NULL,
            shipping_cost DECIMAL(10,2) NOT NULL CHECK (shipping_cost >= 0),
            shipment_date DATE NOT NULL,
            status VARCHAR(50),
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    for s in deduped:
        cursor.execute(
            """INSERT INTO staging.shipments_new 
               (shipment_id, customer_id, shipping_cost, shipment_date, status)
               VALUES (%s, %s, %s, %s, %s);""",
            (s["shipment_id"], s["customer_id"], s["shipping_cost"],
             s["shipment_date"], s.get("status")),
        )

    cursor.execute("DROP TABLE IF EXISTS staging.shipments;")
    cursor.execute("ALTER TABLE staging.shipments_new RENAME TO shipments;")
    conn.commit()

    print(f"Loaded {len(deduped)} shipments into staging.shipments")
    cursor.close()
    conn.close()
    print("Shipment data extraction completed")
