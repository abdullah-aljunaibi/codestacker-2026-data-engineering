"""
Transform: join shipments with customer tiers.
Fixes: effective-dated LEFT JOIN with COALESCE for orphan customers
       (mapped to 'Unknown'), atomic table swap, source validation.
"""
import psycopg2
import os


def transform_shipment_data():
    print("Starting data transformation...")

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

    # Validate sources exist
    cursor.execute("SELECT COUNT(*) FROM staging.shipments;")
    ship_count = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM staging.customer_tiers;")
    tier_count = cursor.fetchone()[0]
    print(f"  Source: {ship_count} shipments")
    print(f"  Source: {tier_count} customer tiers")

    if ship_count == 0:
        raise RuntimeError("No shipments in staging — cannot transform")

    # Atomic table swap with LEFT JOIN
    cursor.execute("DROP TABLE IF EXISTS staging.shipments_with_tiers_new;")
    cursor.execute("""
        CREATE TABLE staging.shipments_with_tiers_new AS
        WITH tier_history AS (
            SELECT
                customer_id,
                customer_name,
                tier,
                tier_updated_date AS effective_from,
                LEAD(tier_updated_date) OVER (
                    PARTITION BY customer_id
                    ORDER BY tier_updated_date
                ) AS effective_to
            FROM staging.customer_tiers
        )
        SELECT
            s.shipment_id,
            s.customer_id,
            s.shipping_cost,
            s.shipment_date,
            s.status,
            COALESCE(ct.customer_name, 'Unknown') AS customer_name,
            COALESCE(ct.tier, 'Unknown') AS tier
        FROM staging.shipments s
        LEFT JOIN tier_history ct
            ON s.customer_id = ct.customer_id
           AND s.shipment_date >= ct.effective_from
           AND (s.shipment_date < ct.effective_to OR ct.effective_to IS NULL);
    """)

    # Check for orphans
    cursor.execute("""
        SELECT COUNT(*) FROM staging.shipments_with_tiers_new WHERE tier = 'Unknown';
    """)
    orphans = cursor.fetchone()[0]
    if orphans > 0:
        print(f"  WARNING: {orphans} shipments have no matching customer tier (mapped to 'Unknown')")

    cursor.execute("SELECT COUNT(*) FROM staging.shipments_with_tiers_new;")
    joined_count = cursor.fetchone()[0]
    print(f"  Joined result: {joined_count} rows")

    cursor.execute("DROP TABLE IF EXISTS staging.shipments_with_tiers;")
    cursor.execute("ALTER TABLE staging.shipments_with_tiers_new RENAME TO shipments_with_tiers;")
    conn.commit()

    cursor.close()
    conn.close()
    print("Data transformation completed")
