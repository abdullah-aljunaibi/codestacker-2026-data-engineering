"""
Extract customer tier data from CSV file.
Fixes: preserve customer tier history, atomic swap, validation, env credentials.
"""
import csv
from datetime import date

from common.config import get_tiers_csv_path
from common.db import get_connection, transaction
from common.logging_utils import get_logger
from common.run_context import get_pipeline_run_id, stage_run


ALLOWED_TIERS = {"Bronze", "Silver", "Gold", "Platinum"}
logger = get_logger(__name__)


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


def _validate_customer_tier_rows_with_reasons(rows):
    seen_history_keys = set()
    validated_rows = []
    rejected_rows = []

    for index, row in enumerate(rows, start=2):
        customer_id = (row.get("customer_id") or "").strip()
        tier = (row.get("tier") or "").strip()
        tier_updated_date_raw = (row.get("tier_updated_date") or "").strip()

        rejection_reason_code = None
        error_message = None

        if not customer_id:
            rejection_reason_code = "MISSING_CUSTOMER_ID"
            error_message = f"Row {index}: missing customer_id"
        elif not tier:
            rejection_reason_code = "INVALID_TIER"
            error_message = "Row {index}: invalid tier value ''".format(index=index)
        elif tier not in ALLOWED_TIERS:
            rejection_reason_code = "INVALID_TIER"
            error_message = f"Row {index}: invalid tier value '{tier}'"
        elif not tier_updated_date_raw:
            rejection_reason_code = "MISSING_TIER_UPDATED_DATE"
            error_message = f"Row {index}: missing tier_updated_date"
        else:
            try:
                tier_updated_date = date.fromisoformat(tier_updated_date_raw)
            except ValueError:
                rejection_reason_code = "INVALID_TIER_UPDATED_DATE"
                error_message = (
                    f"Row {index}: invalid tier_updated_date '{tier_updated_date_raw}'"
                )
            else:
                history_key = (customer_id, tier_updated_date)
                if history_key in seen_history_keys:
                    rejection_reason_code = "DUPLICATE_HISTORY_KEY"
                    error_message = (
                        "Duplicate same-day history rows for "
                        f"customer_id '{customer_id}' on {tier_updated_date.isoformat()}"
                    )
                else:
                    seen_history_keys.add(history_key)

        normalized_row = {
            "source_row_number": index,
            "customer_id": customer_id,
            "customer_name": (row.get("customer_name") or "").strip(),
            "tier": tier,
            "tier_updated_date": tier_updated_date_raw,
        }

        if rejection_reason_code is None:
            validated_rows.append(normalized_row)
        else:
            rejected_rows.append({
                **normalized_row,
                "rejection_reason_code": rejection_reason_code,
                "error_message": error_message,
            })

    return validated_rows, rejected_rows


def extract_customer_tiers_from_csv(pipeline_run_id=None):
    pipeline_run_id = get_pipeline_run_id(pipeline_run_id)
    logger.info("Starting customer tier extraction for pipeline_run_id=%s", pipeline_run_id)

    csv_path = get_tiers_csv_path()

    with open(csv_path, "r") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    logger.info("Loaded %s rows from CSV", len(rows))
    if not rows:
        raise RuntimeError("Customer tier CSV is empty")

    validated_rows, rejected_rows = _validate_customer_tier_rows_with_reasons(rows)
    conn = get_connection()
    cursor = conn.cursor()

    with stage_run(pipeline_run_id, "extract_customer_tiers") as metrics:
        with transaction(conn):
            cursor.execute("CREATE SCHEMA IF NOT EXISTS raw;")
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS raw.customer_tiers_raw (
                    pipeline_run_id TEXT NOT NULL,
                    source_row_number INTEGER NOT NULL,
                    customer_id VARCHAR(50),
                    customer_name VARCHAR(200),
                    tier VARCHAR(50),
                    tier_updated_date VARCHAR(50),
                    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS customer_tiers_raw_pipeline_run_id_idx ON raw.customer_tiers_raw (pipeline_run_id);"
            )
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS raw.customer_tier_rejections (
                    pipeline_run_id TEXT NOT NULL,
                    source_row_number INTEGER NOT NULL,
                    customer_id VARCHAR(50),
                    customer_name VARCHAR(200),
                    tier VARCHAR(50),
                    tier_updated_date VARCHAR(50),
                    rejection_reason_code VARCHAR(100) NOT NULL,
                    error_message TEXT NOT NULL,
                    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS customer_tier_rejections_pipeline_run_id_idx ON raw.customer_tier_rejections (pipeline_run_id);"
            )

            for source_row_number, row in enumerate(rows, start=2):
                cursor.execute(
                    """INSERT INTO raw.customer_tiers_raw
                       (pipeline_run_id, source_row_number, customer_id, customer_name, tier, tier_updated_date)
                       VALUES (%s, %s, %s, %s, %s, %s);""",
                    (
                        pipeline_run_id,
                        source_row_number,
                        (row.get("customer_id") or "").strip() or None,
                        (row.get("customer_name") or "").strip() or None,
                        (row.get("tier") or "").strip() or None,
                        (row.get("tier_updated_date") or "").strip() or None,
                    ),
                )

            for row in rejected_rows:
                cursor.execute(
                    """INSERT INTO raw.customer_tier_rejections
                       (pipeline_run_id, source_row_number, customer_id, customer_name, tier, tier_updated_date,
                        rejection_reason_code, error_message)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s);""",
                    (
                        pipeline_run_id,
                        row["source_row_number"],
                        row["customer_id"] or None,
                        row["customer_name"] or None,
                        row["tier"] or None,
                        row["tier_updated_date"] or None,
                        row["rejection_reason_code"],
                        row["error_message"],
                    ),
                )

            logger.info("Persisted %s rows into raw.customer_tiers_raw", len(rows))
            logger.info(
                "Rejected %s customer tier rows into raw.customer_tier_rejections",
                len(rejected_rows),
            )

            if rejected_rows:
                metrics["rows_read"] = len(rows)
                metrics["rows_written"] = 0
                metrics["rows_rejected"] = len(rejected_rows)
                raise ValueError(rejected_rows[0]["error_message"])

            cursor.execute("CREATE SCHEMA IF NOT EXISTS staging;")
            cursor.execute("DROP TABLE IF EXISTS staging.customer_tiers_new;")
            cursor.execute("""
                CREATE TABLE staging.customer_tiers_new (
                    customer_id VARCHAR(50) NOT NULL,
                    customer_name VARCHAR(200),
                    tier VARCHAR(50) NOT NULL,
                    tier_updated_date DATE NOT NULL,
                    UNIQUE (customer_id, tier_updated_date),
                    CHECK (tier IN ('Bronze', 'Silver', 'Gold', 'Platinum'))
                );
            """)
            cursor.execute(
                """
                INSERT INTO staging.customer_tiers_new
                    (customer_id, customer_name, tier, tier_updated_date)
                SELECT
                    customer_id,
                    customer_name,
                    tier,
                    tier_updated_date::DATE
                FROM raw.customer_tiers_raw
                WHERE pipeline_run_id = %s
                  AND source_row_number NOT IN (
                      SELECT source_row_number
                      FROM raw.customer_tier_rejections
                      WHERE pipeline_run_id = %s
                  )
                ORDER BY source_row_number;
                """,
                (pipeline_run_id, pipeline_run_id),
            )
            cursor.execute("SELECT COUNT(*) FROM staging.customer_tiers_new;")
            staged_row_count = cursor.fetchone()[0]

            if staged_row_count == 0:
                metrics["rows_read"] = len(rows)
                metrics["rows_written"] = 0
                metrics["rows_rejected"] = len(rejected_rows)
                raise RuntimeError("No customer tiers found in staging — aborting extract")

            cursor.execute("DROP TABLE IF EXISTS staging.customer_tiers;")
            cursor.execute("ALTER TABLE staging.customer_tiers_new RENAME TO customer_tiers;")

            metrics["rows_read"] = len(rows)
            metrics["rows_written"] = staged_row_count
            metrics["rows_rejected"] = len(rejected_rows)

    logger.info("Loaded %s customer tier history rows into staging.customer_tiers", staged_row_count)
    cursor.close()
    conn.close()
    logger.info("Customer tier extraction completed")
    return pipeline_run_id


if __name__ == "__main__":
    extract_customer_tiers_from_csv()
