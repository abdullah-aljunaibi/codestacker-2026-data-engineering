"""
Extract shipment data from the external API.
Fixes: parameterized SQL, retry logic, data validation, dedup, atomic swap, env credentials.
"""
import requests
import psycopg2
import os
import time


REJECTION_NEGATIVE_COST = "NEGATIVE_SHIPPING_COST"
REJECTION_MISSING_CUSTOMER_ID = "MISSING_CUSTOMER_ID"
REJECTION_CANCELLED = "CANCELLED_SHIPMENT"
REJECTION_MISSING_SHIPMENT_ID = "MISSING_SHIPMENT_ID"
REJECTION_INVALID_SHIPPING_COST = "INVALID_SHIPPING_COST"
REJECTION_INVALID_SHIPMENT_DATE = "INVALID_SHIPMENT_DATE"


def extract_shipments_from_api():
    print("Starting shipment data extraction...")

    postgres_host = os.environ.get("POSTGRES_HOST")
    default_host = postgres_host or "postgres"
    default_port = "5433" if default_host in {"127.0.0.1", "localhost"} else "5432"

    db_config = {
        "host": default_host,
        "port": int(os.environ.get("POSTGRES_PORT", default_port)),
        "database": os.environ.get("POSTGRES_DB", "airflow"),
        "user": os.environ.get("POSTGRES_USER", "airflow"),
        "password": os.environ.get("POSTGRES_PASSWORD", "airflow"),
    }

    try:
        conn = psycopg2.connect(**db_config)
    except psycopg2.OperationalError as exc:
        if (
            "POSTGRES_HOST" in os.environ
            or "POSTGRES_PORT" in os.environ
            or "could not translate host name" not in str(exc)
        ):
            raise

        fallback_config = {**db_config, "host": "127.0.0.1", "port": 5433}
        conn = psycopg2.connect(**fallback_config)

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

    cursor.execute("CREATE SCHEMA IF NOT EXISTS raw;")
    cursor.execute("DROP TABLE IF EXISTS raw.shipments_raw;")
    cursor.execute("""
        CREATE TABLE raw.shipments_raw (
            load_order INTEGER NOT NULL,
            shipment_id TEXT,
            customer_id TEXT,
            shipping_cost TEXT,
            shipment_date TEXT,
            status TEXT,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    cursor.execute("DROP TABLE IF EXISTS raw.shipment_rejections;")
    cursor.execute("""
        CREATE TABLE raw.shipment_rejections (
            load_order INTEGER NOT NULL,
            shipment_id TEXT,
            customer_id TEXT,
            shipping_cost TEXT,
            shipment_date TEXT,
            status TEXT,
            rejection_reason_code VARCHAR(100) NOT NULL,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    for load_order, shipment in enumerate(shipments, start=1):
        cursor.execute(
            """INSERT INTO raw.shipments_raw
               (load_order, shipment_id, customer_id, shipping_cost, shipment_date, status)
               VALUES (%s, %s, %s, %s, %s, %s);""",
            (
                load_order,
                shipment.get("shipment_id"),
                shipment.get("customer_id"),
                shipment.get("shipping_cost"),
                shipment.get("shipment_date"),
                shipment.get("status"),
            ),
        )

    print(f"Persisted {len(shipments)} rows into raw.shipments_raw")

    classification_cte = """
        WITH normalized AS (
            SELECT
                load_order,
                shipment_id,
                customer_id,
                shipping_cost,
                shipment_date,
                status,
                NULLIF(BTRIM(shipment_id), '') AS shipment_id_clean,
                NULLIF(BTRIM(customer_id), '') AS customer_id_clean,
                NULLIF(BTRIM(status), '') AS status_clean,
                CASE
                    WHEN shipping_cost IS NULL OR BTRIM(shipping_cost) = '' THEN NULL
                    WHEN BTRIM(shipping_cost) ~ '^-?\\d+(\\.\\d{1,2})?$'
                        THEN BTRIM(shipping_cost)::DECIMAL(10,2)
                END AS shipping_cost_value,
                CASE
                    WHEN shipment_date IS NOT NULL
                     AND BTRIM(shipment_date) ~ '^\\d{4}-\\d{2}-\\d{2}$'
                     AND TO_CHAR(TO_DATE(BTRIM(shipment_date), 'YYYY-MM-DD'), 'YYYY-MM-DD') = BTRIM(shipment_date)
                        THEN TO_DATE(BTRIM(shipment_date), 'YYYY-MM-DD')
                END AS shipment_date_value
            FROM raw.shipments_raw
        ),
        classified AS (
            SELECT
                load_order,
                shipment_id,
                customer_id,
                shipping_cost,
                shipment_date,
                status,
                shipment_id_clean,
                customer_id_clean,
                status_clean,
                shipping_cost_value,
                shipment_date_value,
                CASE
                    WHEN shipment_id_clean IS NULL THEN %s
                    WHEN shipment_date_value IS NULL THEN %s
                    WHEN shipping_cost_value IS NULL THEN %s
                    WHEN shipping_cost_value < 0 THEN %s
                    WHEN customer_id_clean IS NULL THEN %s
                    WHEN LOWER(COALESCE(status_clean, '')) = 'cancelled' THEN %s
                END AS rejection_reason_code
            FROM normalized
        )
    """
    classification_params = (
        REJECTION_MISSING_SHIPMENT_ID,
        REJECTION_INVALID_SHIPMENT_DATE,
        REJECTION_INVALID_SHIPPING_COST,
        REJECTION_NEGATIVE_COST,
        REJECTION_MISSING_CUSTOMER_ID,
        REJECTION_CANCELLED,
    )

    cursor.execute(
        classification_cte + """
        INSERT INTO raw.shipment_rejections
            (load_order, shipment_id, customer_id, shipping_cost, shipment_date, status, rejection_reason_code)
        SELECT
            load_order,
            shipment_id,
            customer_id,
            shipping_cost,
            shipment_date,
            status,
            rejection_reason_code
        FROM classified
        WHERE rejection_reason_code IS NOT NULL;
    """,
        classification_params,
    )

    cursor.execute("SELECT COUNT(*) FROM raw.shipment_rejections;")
    rejected = cursor.fetchone()[0]
    print(f"Valid: {len(shipments) - rejected}, Rejected: {rejected}")

    cursor.execute("""
        SELECT shipment_id, rejection_reason_code, shipping_cost, customer_id, status
        FROM raw.shipment_rejections
        ORDER BY load_order;
    """)
    for shipment_id, reason_code, shipping_cost, customer_id, status in cursor.fetchall():
        if reason_code == REJECTION_NEGATIVE_COST:
            print(f"  REJECTED: {shipment_id}: negative shipping_cost ({shipping_cost})")
        elif reason_code == REJECTION_MISSING_CUSTOMER_ID:
            print(f"  REJECTED: {shipment_id}: missing customer_id")
        elif reason_code == REJECTION_CANCELLED:
            print(f"  REJECTED: {shipment_id}: cancelled shipment excluded")

    # Deduplicate by shipment_id (keep last occurrence)
    cursor.execute(
        classification_cte + """
        SELECT shipment_id_clean, customer_id_clean, shipping_cost_value, shipment_date_value, status_clean
        FROM (
            SELECT
                shipment_id_clean,
                customer_id_clean,
                shipping_cost_value,
                shipment_date_value,
                status_clean,
                ROW_NUMBER() OVER (
                    PARTITION BY shipment_id_clean
                    ORDER BY load_order DESC
                ) AS row_num
            FROM classified
            WHERE rejection_reason_code IS NULL
        ) ranked
        WHERE row_num = 1
        ORDER BY shipment_id_clean;
    """,
        classification_params,
    )
    deduped = cursor.fetchall()
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

    for shipment_id, customer_id, shipping_cost, shipment_date, status in deduped:
        cursor.execute(
            """INSERT INTO staging.shipments_new 
               (shipment_id, customer_id, shipping_cost, shipment_date, status)
               VALUES (%s, %s, %s, %s, %s);""",
            (shipment_id, customer_id, shipping_cost, shipment_date, status),
        )

    cursor.execute("DROP TABLE IF EXISTS staging.shipments;")
    cursor.execute("ALTER TABLE staging.shipments_new RENAME TO shipments;")
    conn.commit()

    print(f"Loaded {len(deduped)} shipments into staging.shipments")
    cursor.close()
    conn.close()
    print("Shipment data extraction completed")
