"""
Tests for the shipment analytics pipeline.
Validates extraction, transformation, analytics, and idempotency.
Runs from host against postgres on port 5433.
"""
import pytest
from conftest import get_conn


class TestExtraction:
    """Test data extraction stage."""

    def test_shipments_extracted(self):
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM staging.shipments;")
        assert cur.fetchone()[0] > 0, "No shipments extracted"
        cur.close(); conn.close()

    def test_expected_shipment_count(self):
        """21 raw - 1 dupe (SHP002) - 1 neg cost - 1 null cust - 1 cancelled = 17."""
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM staging.shipments;")
        assert cur.fetchone()[0] == 17, "Expected 17 valid shipments"
        cur.close(); conn.close()

    def test_no_duplicate_shipment_ids(self):
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT shipment_id, COUNT(*) FROM staging.shipments 
            GROUP BY shipment_id HAVING COUNT(*) > 1;
        """)
        dupes = cur.fetchall()
        cur.close(); conn.close()
        assert len(dupes) == 0, f"Duplicate shipment_ids: {dupes}"

    def test_no_negative_shipping_costs(self):
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM staging.shipments WHERE shipping_cost < 0;")
        assert cur.fetchone()[0] == 0, "Negative costs in staging"
        cur.close(); conn.close()

    def test_no_null_customer_ids(self):
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM staging.shipments WHERE customer_id IS NULL;")
        assert cur.fetchone()[0] == 0, "Null customer_ids in staging"
        cur.close(); conn.close()

    def test_no_cancelled_shipments(self):
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM staging.shipments WHERE status = 'cancelled';")
        assert cur.fetchone()[0] == 0, "Cancelled shipments in staging"
        cur.close(); conn.close()

    def test_customer_tiers_deduped(self):
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT customer_id, COUNT(*) FROM staging.customer_tiers 
            GROUP BY customer_id HAVING COUNT(*) > 1;
        """)
        dupes = cur.fetchall()
        cur.close(); conn.close()
        assert len(dupes) == 0, f"Duplicate customer tiers: {dupes}"

    def test_cust002_latest_tier_is_gold(self):
        """CUST002 has two CSV entries: Platinum (Jan) and Gold (Feb 15). Latest = Gold."""
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT tier FROM staging.customer_tiers WHERE customer_id = 'CUST002';")
        row = cur.fetchone()
        cur.close(); conn.close()
        assert row is not None, "CUST002 not found"
        assert row[0] == 'Gold', f"Expected Gold, got {row[0]}"


class TestTransformation:
    """Test data transformation stage."""

    def test_join_preserves_row_count(self):
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM staging.shipments;")
        ship = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM staging.shipments_with_tiers;")
        joined = cur.fetchone()[0]
        cur.close(); conn.close()
        assert joined == ship, f"Row mismatch: joined={joined}, shipments={ship}"

    def test_orphan_customer_mapped_to_unknown(self):
        """CUST999 has no tier — should be mapped to 'Unknown'."""
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT tier FROM staging.shipments_with_tiers WHERE customer_id = 'CUST999';")
        row = cur.fetchone()
        cur.close(); conn.close()
        assert row is not None, "CUST999 not found"
        assert row[0] == 'Unknown', f"Expected 'Unknown', got {row[0]}"

    def test_all_tiers_present(self):
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT tier FROM staging.shipments_with_tiers ORDER BY tier;")
        tiers = {r[0] for r in cur.fetchall()}
        cur.close(); conn.close()
        assert tiers == {'Bronze', 'Gold', 'Platinum', 'Silver', 'Unknown'}


class TestAnalytics:
    """Test final analytics output."""

    def test_analytics_has_data(self):
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM analytics.shipping_spend_by_tier;")
        assert cur.fetchone()[0] > 0
        cur.close(); conn.close()

    def test_expected_analytics_rows(self):
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM analytics.shipping_spend_by_tier;")
        assert cur.fetchone()[0] == 10, "Expected 10 analytics rows"
        cur.close(); conn.close()

    def test_no_negative_spend(self):
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM analytics.shipping_spend_by_tier WHERE total_shipping_spend < 0;")
        assert cur.fetchone()[0] == 0
        cur.close(); conn.close()

    def test_total_spend_matches_source(self):
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT SUM(total_shipping_spend) FROM analytics.shipping_spend_by_tier;")
        analytics = float(cur.fetchone()[0])
        cur.execute("SELECT SUM(shipping_cost) FROM staging.shipments_with_tiers;")
        staging = float(cur.fetchone()[0])
        cur.close(); conn.close()
        assert abs(analytics - staging) < 0.01, f"Mismatch: analytics={analytics}, staging={staging}"

    def test_no_duplicate_tier_month(self):
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT tier, year_month, COUNT(*) FROM analytics.shipping_spend_by_tier 
            GROUP BY tier, year_month HAVING COUNT(*) > 1;
        """)
        dupes = cur.fetchall()
        cur.close(); conn.close()
        assert len(dupes) == 0, f"Duplicates: {dupes}"


class TestIdempotency:
    """Test idempotency at the SQL level."""

    def test_reload_produces_same_result(self):
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT tier, year_month, total_shipping_spend, shipment_count 
            FROM analytics.shipping_spend_by_tier ORDER BY year_month, tier;
        """)
        before = cur.fetchall()

        cur.execute("TRUNCATE TABLE analytics.shipping_spend_by_tier;")
        cur.execute("""
            INSERT INTO analytics.shipping_spend_by_tier 
                (tier, year_month, total_shipping_spend, shipment_count, calculated_at)
            SELECT tier, TO_CHAR(shipment_date, 'YYYY-MM'),
                   SUM(shipping_cost), COUNT(*), NOW()
            FROM staging.shipments_with_tiers
            GROUP BY tier, TO_CHAR(shipment_date, 'YYYY-MM')
            ORDER BY TO_CHAR(shipment_date, 'YYYY-MM'), tier;
        """)
        conn.commit()

        cur.execute("""
            SELECT tier, year_month, total_shipping_spend, shipment_count 
            FROM analytics.shipping_spend_by_tier ORDER BY year_month, tier;
        """)
        after = cur.fetchall()
        cur.close(); conn.close()

        assert len(before) == len(after)
        for b, a in zip(before, after):
            assert b[0] == a[0] and b[1] == a[1]
            assert abs(float(b[2]) - float(a[2])) < 0.01
            assert b[3] == a[3]
