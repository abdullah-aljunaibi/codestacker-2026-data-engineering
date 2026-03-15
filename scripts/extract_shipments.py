"""
Extract shipment data from the external API.
Fixes: parameterized SQL, retry logic, data validation, dedup, atomic swap, env credentials.
"""
import time

import requests

from common.config import get_api_url
from common.db import get_connection, transaction
from common.logging_utils import get_logger
from common.run_context import get_pipeline_run_id, stage_run


REJECTION_NEGATIVE_COST = "NEGATIVE_SHIPPING_COST"
REJECTION_MISSING_CUSTOMER_ID = "MISSING_CUSTOMER_ID"
REJECTION_CANCELLED = "CANCELLED_SHIPMENT"
REJECTION_MISSING_SHIPMENT_ID = "MISSING_SHIPMENT_ID"
REJECTION_INVALID_SHIPPING_COST = "INVALID_SHIPPING_COST"
REJECTION_INVALID_SHIPMENT_DATE = "INVALID_SHIPMENT_DATE"

logger = get_logger(__name__)


def extract_shipments_from_api(pipeline_run_id=None):
    pipeline_run_id = get_pipeline_run_id(pipeline_run_id)
    logger.info("Starting shipment data extraction for pipeline_run_id=%s", pipeline_run_id)

    conn = get_connection()
    cursor = conn.cursor()

    api_url = get_api_url()

    # Retry logic for flaky API (returns 500 intermittently)
    shipments = None
    retry_count = 0
    for attempt in range(1, 4):
        try:
            logger.info("API request attempt %s/3", attempt)
            response = requests.get(api_url, timeout=30)
            response.raise_for_status()
            data = response.json()
            shipments = data["data"]
            break
        except Exception as e:
            retry_count = attempt
            logger.warning("API request attempt %s failed: %s", attempt, e)
            if attempt < 3:
                logger.info("Retrying in 5s")
                time.sleep(5)
            else:
                raise RuntimeError(f"API failed after 3 attempts: {e}")

    logger.info("Fetched %s raw shipments from API", len(shipments))

    classification_cte = """
        WITH normalized AS (
            SELECT
                pipeline_run_id,
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
            WHERE pipeline_run_id = %s
        ),
        classified AS (
            SELECT
                pipeline_run_id,
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
        pipeline_run_id,
        REJECTION_MISSING_SHIPMENT_ID,
        REJECTION_INVALID_SHIPMENT_DATE,
        REJECTION_INVALID_SHIPPING_COST,
        REJECTION_NEGATIVE_COST,
        REJECTION_MISSING_CUSTOMER_ID,
        REJECTION_CANCELLED,
    )

    with stage_run(pipeline_run_id, "extract_shipments", retry_count=retry_count) as metrics:
        with transaction(conn):
            cursor.execute("CREATE SCHEMA IF NOT EXISTS raw;")
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS raw.shipments_raw (
                    pipeline_run_id TEXT NOT NULL,
                    load_order INTEGER NOT NULL,
                    shipment_id TEXT,
                    customer_id TEXT,
                    shipping_cost TEXT,
                    shipment_date TEXT,
                    status TEXT,
                    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS shipments_raw_pipeline_run_id_idx ON raw.shipments_raw (pipeline_run_id);"
            )
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS raw.shipment_rejections (
                    pipeline_run_id TEXT NOT NULL,
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
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS shipment_rejections_pipeline_run_id_idx ON raw.shipment_rejections (pipeline_run_id);"
            )

            for load_order, shipment in enumerate(shipments, start=1):
                cursor.execute(
                    """INSERT INTO raw.shipments_raw
                       (pipeline_run_id, load_order, shipment_id, customer_id, shipping_cost, shipment_date, status)
                       VALUES (%s, %s, %s, %s, %s, %s, %s);""",
                    (
                        pipeline_run_id,
                        load_order,
                        shipment.get("shipment_id"),
                        shipment.get("customer_id"),
                        shipment.get("shipping_cost"),
                        shipment.get("shipment_date"),
                        shipment.get("status"),
                    ),
                )

            logger.info("Persisted %s rows into raw.shipments_raw", len(shipments))

            cursor.execute(
                classification_cte + """
                INSERT INTO raw.shipment_rejections
                    (pipeline_run_id, load_order, shipment_id, customer_id, shipping_cost, shipment_date, status, rejection_reason_code)
                SELECT
                    pipeline_run_id,
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

            cursor.execute(
                "SELECT COUNT(*) FROM raw.shipment_rejections WHERE pipeline_run_id = %s;",
                (pipeline_run_id,),
            )
            rejected = cursor.fetchone()[0]
            logger.info("Valid: %s, Rejected: %s", len(shipments) - rejected, rejected)

            cursor.execute(
                """
                SELECT shipment_id, rejection_reason_code, shipping_cost, customer_id, status
                FROM raw.shipment_rejections
                WHERE pipeline_run_id = %s
                ORDER BY load_order;
                """,
                (pipeline_run_id,),
            )
            for shipment_id, reason_code, shipping_cost, customer_id, status in cursor.fetchall():
                if reason_code == REJECTION_NEGATIVE_COST:
                    logger.info("REJECTED: %s: negative shipping_cost (%s)", shipment_id, shipping_cost)
                elif reason_code == REJECTION_MISSING_CUSTOMER_ID:
                    logger.info("REJECTED: %s: missing customer_id", shipment_id)
                elif reason_code == REJECTION_CANCELLED:
                    logger.info("REJECTED: %s: cancelled shipment excluded", shipment_id)

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
            logger.info("After deduplication: %s unique shipments", len(deduped))

            if not deduped:
                metrics["rows_read"] = len(shipments)
                metrics["rows_written"] = 0
                metrics["rows_rejected"] = rejected
                raise RuntimeError("No valid shipments remain after validation — aborting extract")

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

            metrics["rows_read"] = len(shipments)
            metrics["rows_written"] = len(deduped)
            metrics["rows_rejected"] = rejected

    logger.info("Loaded %s shipments into staging.shipments", len(deduped))
    cursor.close()
    conn.close()
    logger.info("Shipment data extraction completed")
    return pipeline_run_id


if __name__ == "__main__":
    extract_shipments_from_api()
