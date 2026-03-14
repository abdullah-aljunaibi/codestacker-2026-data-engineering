"""
Load analytics: aggregate shipping spend per tier per month.
Fixes: idempotent via TRUNCATE before INSERT, env credentials, summary output.
"""
import psycopg2
import os


def load_analytics_data():
    print("Starting analytics data load...")

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

    # Validate source
    cursor.execute("SELECT COUNT(*) FROM staging.shipments_with_tiers;")
    source_count = cursor.fetchone()[0]
    print(f"  Source: {source_count} rows in shipments_with_tiers")

    if source_count == 0:
        raise RuntimeError("No data in shipments_with_tiers — cannot load analytics")

    # Ensure analytics table exists
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS analytics.shipping_spend_by_tier (
            tier VARCHAR(50),
            year_month VARCHAR(7),
            total_shipping_spend DECIMAL(12,2),
            shipment_count INTEGER,
            calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    # Idempotent: truncate then insert
    cursor.execute("TRUNCATE TABLE analytics.shipping_spend_by_tier;")
    cursor.execute("""
        INSERT INTO analytics.shipping_spend_by_tier 
            (tier, year_month, total_shipping_spend, shipment_count, calculated_at)
        SELECT 
            tier,
            TO_CHAR(shipment_date, 'YYYY-MM') AS year_month,
            SUM(shipping_cost) AS total_shipping_spend,
            COUNT(*) AS shipment_count,
            NOW() AS calculated_at
        FROM staging.shipments_with_tiers
        GROUP BY tier, TO_CHAR(shipment_date, 'YYYY-MM')
        ORDER BY year_month, tier;
    """)

    cursor.execute("SELECT COUNT(*) FROM analytics.shipping_spend_by_tier;")
    row_count = cursor.fetchone()[0]
    print(f"  Inserted {row_count} rows into analytics.shipping_spend_by_tier")

    conn.commit()

    # Print summary
    cursor.execute("""
        SELECT tier, year_month, total_shipping_spend, shipment_count
        FROM analytics.shipping_spend_by_tier
        ORDER BY year_month, tier;
    """)
    rows = cursor.fetchall()

    print(f"\n  {'=== Analytics Summary ===':^50}")
    print(f"  {'Tier':<15} {'Month':<10} {'Spend':>12} {'Count':>6}")
    print(f"  {'-'*15} {'-'*10} {'-'*12} {'-'*6}")
    for tier, month, spend, count in rows:
        print(f"  {tier:<15} {month:<10} ${spend:>10,.2f} {count:>6}")

    cursor.close()
    conn.close()
    print("\nAnalytics data load completed")
