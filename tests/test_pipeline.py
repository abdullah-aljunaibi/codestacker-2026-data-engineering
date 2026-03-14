"""
Tests for the shipment analytics pipeline.
Validates extraction, transformation, analytics, and idempotency.
Runs from host against postgres on port 5433.
"""
from pathlib import Path
import sys

import pytest
from conftest import get_conn

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from extract_customer_tiers import extract_customer_tiers_from_csv


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

    def test_customer_tier_history_preserved(self):
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM staging.customer_tiers;")
        row_count = cur.fetchone()[0]
        cur.execute("""
            SELECT COUNT(*) FROM staging.customer_tiers
            WHERE customer_id = 'CUST002';
        """)
        cust002_count = cur.fetchone()[0]
        cur.close(); conn.close()
        assert row_count == 7, f"Expected 7 customer tier history rows, got {row_count}"
        assert cust002_count == 2, f"Expected 2 history rows for CUST002, got {cust002_count}"

    def test_customer_tiers_contract_enforced(self):
        conn = get_conn()
        cur = conn.cursor()

        cur.execute("""
            SELECT column_name, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'staging' AND table_name = 'customer_tiers'
              AND column_name IN ('customer_id', 'tier', 'tier_updated_date')
            ORDER BY column_name;
        """)
        columns = {column_name: is_nullable for column_name, is_nullable in cur.fetchall()}

        cur.execute("""
            SELECT ARRAY_AGG(att.attname ORDER BY att.attname)
            FROM pg_constraint con
            JOIN pg_class rel ON rel.oid = con.conrelid
            JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace
            JOIN unnest(con.conkey) AS conkey(attnum) ON TRUE
            JOIN pg_attribute att
              ON att.attrelid = rel.oid AND att.attnum = conkey.attnum
            WHERE nsp.nspname = 'staging'
              AND rel.relname = 'customer_tiers'
              AND con.contype = 'u'
            GROUP BY con.oid;
        """)
        unique_constraints = cur.fetchall()

        cur.execute("""
            SELECT pg_get_constraintdef(con.oid)
            FROM pg_constraint con
            JOIN pg_class rel ON rel.oid = con.conrelid
            JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace
            WHERE nsp.nspname = 'staging'
              AND rel.relname = 'customer_tiers'
              AND con.contype = 'c';
        """)
        check_constraints = [row[0] for row in cur.fetchall()]

        cur.close(); conn.close()

        assert columns == {
            'customer_id': 'NO',
            'tier': 'NO',
            'tier_updated_date': 'NO',
        }, f"Unexpected nullability contract: {columns}"
        assert (['customer_id', 'tier_updated_date'],) in unique_constraints, (
            f"Missing UNIQUE(customer_id, tier_updated_date): {unique_constraints}"
        )
        assert any(
            "tier::text = ANY" in constraint
            and all(tier in constraint for tier in ('Bronze', 'Silver', 'Gold', 'Platinum'))
            for constraint in check_constraints
        ), f"Missing tier CHECK constraint: {check_constraints}"

    @pytest.mark.parametrize(
        ("csv_text", "error_message"),
        [
            (
                "customer_id,customer_name,tier,tier_updated_date\n"
                "CUST001,Acme Corporation,Gold,2024-01-01\n"
                "CUST001,Acme Corporation,Silver,2024-01-01\n",
                "Duplicate same-day history rows for customer_id 'CUST001' on 2024-01-01",
            ),
            (
                "customer_id,customer_name,tier,tier_updated_date\n"
                "CUST001,Acme Corporation,Gold,\n",
                "Row 2: missing tier_updated_date",
            ),
            (
                "customer_id,customer_name,tier,tier_updated_date\n"
                ",Acme Corporation,Gold,2024-01-01\n",
                "Row 2: missing customer_id",
            ),
            (
                "customer_id,customer_name,tier,tier_updated_date\n"
                "CUST001,Acme Corporation,Diamond,2024-01-01\n",
                "Row 2: invalid tier value 'Diamond'",
            ),
        ],
    )
    def test_invalid_customer_tier_history_rejected_deterministically(
        self, tmp_path, monkeypatch, csv_text, error_message
    ):
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM staging.customer_tiers;")
        before_count = cur.fetchone()[0]
        cur.close(); conn.close()

        csv_path = tmp_path / "customer_tiers_invalid.csv"
        csv_path.write_text(csv_text, encoding="utf-8")
        monkeypatch.setenv("TIERS_CSV_PATH", str(csv_path))

        with pytest.raises(ValueError, match=error_message):
            extract_customer_tiers_from_csv()

        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM staging.customer_tiers;")
        after_count = cur.fetchone()[0]
        cur.close(); conn.close()

        assert after_count == before_count == 7


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

    def test_effective_dated_tiers_for_cust002_shipments(self):
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT shipment_id, tier
            FROM staging.shipments_with_tiers
            WHERE shipment_id IN ('SHP002', 'SHP006', 'SHP018')
            ORDER BY shipment_id;
        """)
        rows = cur.fetchall()
        cur.close(); conn.close()
        assert rows == [
            ('SHP002', 'Platinum'),
            ('SHP006', 'Platinum'),
            ('SHP018', 'Gold'),
        ], f"Unexpected CUST002 shipment tiers: {rows}"

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
        assert cur.fetchone()[0] == 11, "Expected 11 analytics rows"
        cur.close(); conn.close()

    def test_monthly_totals_reflect_historical_tier_split(self):
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT tier, year_month, total_shipping_spend, shipment_count
            FROM analytics.shipping_spend_by_tier
            ORDER BY year_month, tier;
        """)
        rows = [
            (tier, year_month, float(total_shipping_spend), shipment_count)
            for tier, year_month, total_shipping_spend, shipment_count in cur.fetchall()
        ]
        cur.close(); conn.close()
        assert rows == [
            ('Gold', '2024-01', 55.50, 2),
            ('Platinum', '2024-01', 47.00, 1),
            ('Silver', '2024-01', 15.75, 1),
            ('Bronze', '2024-02', 20.00, 1),
            ('Gold', '2024-02', 84.00, 3),
            ('Platinum', '2024-02', 100.50, 2),
            ('Silver', '2024-02', 42.00, 1),
            ('Unknown', '2024-02', 18.50, 1),
            ('Gold', '2024-03', 83.25, 3),
            ('Platinum', '2024-03', 44.50, 1),
            ('Silver', '2024-03', 38.00, 1),
        ], f"Unexpected analytics rows: {rows}"

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
