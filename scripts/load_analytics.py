"""
Load analytics: aggregate shipping spend per tier per month.
Fixes: idempotent via TRUNCATE before INSERT, env credentials, summary output.
"""
from common.db import get_connection, transaction
from common.logging_utils import get_logger
from common.run_context import complete_pipeline_run, get_pipeline_run_id, stage_run


logger = get_logger(__name__)


def load_analytics_data(pipeline_run_id=None):
    pipeline_run_id = get_pipeline_run_id(pipeline_run_id)
    logger.info("Starting analytics data load for pipeline_run_id=%s", pipeline_run_id)

    conn = get_connection()
    cursor = conn.cursor()

    with stage_run(pipeline_run_id, "load_analytics") as metrics:
        with transaction(conn):
            # Validate source
            cursor.execute("SELECT COUNT(*) FROM staging.shipments_with_tiers;")
            source_count = cursor.fetchone()[0]
            logger.info("Source: %s rows in shipments_with_tiers", source_count)

            if source_count == 0:
                raise RuntimeError("No data in shipments_with_tiers — cannot load analytics")

            cursor.execute("CREATE SCHEMA IF NOT EXISTS analytics;")
            cursor.execute("DROP TABLE IF EXISTS analytics.shipping_spend_by_tier_new;")
            cursor.execute("""
                CREATE TABLE analytics.shipping_spend_by_tier_new (
                    tier VARCHAR(50) NOT NULL,
                    year_month VARCHAR(7) NOT NULL,
                    total_shipping_spend DECIMAL(12,2) NOT NULL
                        CHECK (total_shipping_spend >= 0),
                    shipment_count INTEGER NOT NULL
                        CHECK (shipment_count > 0),
                    calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        NOT NULL,
                    PRIMARY KEY (tier, year_month)
                );
            """)
            cursor.execute("""
                INSERT INTO analytics.shipping_spend_by_tier_new
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

            cursor.execute("SELECT COUNT(*) FROM analytics.shipping_spend_by_tier_new;")
            row_count = cursor.fetchone()[0]
            if row_count == 0:
                metrics["rows_read"] = source_count
                metrics["rows_written"] = 0
                metrics["rows_rejected"] = 0
                raise RuntimeError("No analytics rows generated — aborting load")

            cursor.execute("DROP TABLE IF EXISTS analytics.shipping_spend_by_tier;")
            cursor.execute(
                "ALTER TABLE analytics.shipping_spend_by_tier_new RENAME TO shipping_spend_by_tier;"
            )
            logger.info("Inserted %s rows into analytics.shipping_spend_by_tier", row_count)

            metrics["rows_read"] = source_count
            metrics["rows_written"] = row_count
            metrics["rows_rejected"] = 0

            complete_pipeline_run(pipeline_run_id, "success")

            # Print summary
            cursor.execute("""
                SELECT tier, year_month, total_shipping_spend, shipment_count
                FROM analytics.shipping_spend_by_tier
                ORDER BY year_month, tier;
            """)
            rows = cursor.fetchall()

    logger.info("=== Analytics Summary ===")
    logger.info("%-15s %-10s %12s %6s", "Tier", "Month", "Spend", "Count")
    logger.info("%-15s %-10s %12s %6s", "-" * 15, "-" * 10, "-" * 12, "-" * 6)
    for tier, month, spend, count in rows:
        logger.info("%-15s %-10s $%10.2f %6s", tier, month, float(spend), count)

    cursor.close()
    conn.close()
    logger.info("Analytics data load completed")
    return pipeline_run_id


if __name__ == "__main__":
    load_analytics_data()
