"""
Transform: join shipments with customer tiers.
Fixes: effective-dated LEFT JOIN with COALESCE for orphan customers
       (mapped to 'Unknown'), atomic table swap, source validation.
"""
from common.db import get_connection, transaction
from common.logging_utils import get_logger
from common.run_context import get_pipeline_run_id, stage_run


logger = get_logger(__name__)


def transform_shipment_data(pipeline_run_id=None):
    pipeline_run_id = get_pipeline_run_id(pipeline_run_id)
    logger.info("Starting data transformation for pipeline_run_id=%s", pipeline_run_id)

    conn = get_connection()
    cursor = conn.cursor()

    with transaction(conn):
        with stage_run(pipeline_run_id, "transform_data") as metrics:
            # Validate sources exist
            cursor.execute("SELECT COUNT(*) FROM staging.shipments;")
            ship_count = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM staging.customer_tiers;")
            tier_count = cursor.fetchone()[0]
            logger.info("Source: %s shipments", ship_count)
            logger.info("Source: %s customer tiers", tier_count)

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
                logger.warning(
                    "%s shipments have no matching customer tier (mapped to 'Unknown')",
                    orphans,
                )

            cursor.execute("SELECT COUNT(*) FROM staging.shipments_with_tiers_new;")
            joined_count = cursor.fetchone()[0]
            logger.info("Joined result: %s rows", joined_count)

            cursor.execute("DROP TABLE IF EXISTS staging.shipments_with_tiers;")
            cursor.execute("ALTER TABLE staging.shipments_with_tiers_new RENAME TO shipments_with_tiers;")

            metrics["rows_read"] = ship_count
            metrics["rows_written"] = joined_count
            metrics["rows_rejected"] = 0

    cursor.close()
    conn.close()
    logger.info("Data transformation completed")
