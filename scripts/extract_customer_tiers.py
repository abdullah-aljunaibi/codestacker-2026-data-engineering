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


def extract_customer_tiers_from_csv():
    print("Starting customer tier extraction...")

    csv_path = os.environ.get("TIERS_CSV_PATH", "/opt/airflow/data/customer_tiers.csv")

    with open(csv_path, "r") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    print(f"Loaded {len(rows)} rows from CSV")
    if not rows:
        raise RuntimeError("Customer tier CSV is empty")

    validated_rows, rejected_rows = _validate_customer_tier_rows_with_reasons(rows)

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

    cursor.execute("CREATE SCHEMA IF NOT EXISTS raw;")
    cursor.execute("DROP TABLE IF EXISTS raw.customer_tiers_raw;")
    cursor.execute("""
        CREATE TABLE raw.customer_tiers_raw (
            source_row_number INTEGER NOT NULL,
            customer_id VARCHAR(50),
            customer_name VARCHAR(200),
            tier VARCHAR(50),
            tier_updated_date VARCHAR(50),
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    cursor.execute("DROP TABLE IF EXISTS raw.customer_tier_rejections;")
    cursor.execute("""
        CREATE TABLE raw.customer_tier_rejections (
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

    for source_row_number, row in enumerate(rows, start=2):
        cursor.execute(
            """INSERT INTO raw.customer_tiers_raw
               (source_row_number, customer_id, customer_name, tier, tier_updated_date)
               VALUES (%s, %s, %s, %s, %s);""",
            (
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
               (source_row_number, customer_id, customer_name, tier, tier_updated_date,
                rejection_reason_code, error_message)
               VALUES (%s, %s, %s, %s, %s, %s, %s);""",
            (
                row["source_row_number"],
                row["customer_id"] or None,
                row["customer_name"] or None,
                row["tier"] or None,
                row["tier_updated_date"] or None,
                row["rejection_reason_code"],
                row["error_message"],
            ),
        )

    print(f"Persisted {len(rows)} rows into raw.customer_tiers_raw")
    print(f"Rejected {len(rejected_rows)} customer tier rows into raw.customer_tier_rejections")

    if rejected_rows:
        conn.commit()
        cursor.close()
        conn.close()
        raise ValueError(rejected_rows[0]["error_message"])

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
    cursor.execute("""
        INSERT INTO staging.customer_tiers
            (customer_id, customer_name, tier, tier_updated_date)
        SELECT
            customer_id,
            customer_name,
            tier,
            tier_updated_date::DATE
        FROM raw.customer_tiers_raw
        WHERE source_row_number NOT IN (
            SELECT source_row_number
            FROM raw.customer_tier_rejections
        )
        ORDER BY source_row_number;
    """)

    conn.commit()

    print(f"Loaded {len(validated_rows)} customer tier history rows into staging.customer_tiers")
    cursor.close()
    conn.close()
    print("Customer tier extraction completed")
