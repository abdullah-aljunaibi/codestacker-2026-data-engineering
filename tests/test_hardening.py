"""Expanded hardening tests for the shipment analytics pipeline."""

from __future__ import annotations

from pathlib import Path
import sys
import uuid

import pytest

from conftest import get_conn

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import extract_shipments as extract_shipments_module
from extract_customer_tiers import extract_customer_tiers_from_csv
from extract_shipments import extract_shipments_from_api
from load_analytics import load_analytics_data
from transform_data import transform_shipment_data


BASELINE_SHIPMENTS = [
    {"shipment_id": "SHP001", "customer_id": "CUST001", "shipping_cost": 25.50, "shipment_date": "2024-01-15", "status": "delivered"},
    {"shipment_id": "SHP002", "customer_id": "CUST002", "shipping_cost": 45.00, "shipment_date": "2024-01-16", "status": "delivered"},
    {"shipment_id": "SHP003", "customer_id": "CUST003", "shipping_cost": 15.75, "shipment_date": "2024-01-20", "status": "delivered"},
    {"shipment_id": "SHP004", "customer_id": "CUST001", "shipping_cost": 30.00, "shipment_date": "2024-01-25", "status": "delivered"},
    {"shipment_id": "SHP005", "customer_id": "CUST004", "shipping_cost": 55.25, "shipment_date": "2024-02-01", "status": "delivered"},
    {"shipment_id": "SHP002", "customer_id": "CUST002", "shipping_cost": 47.00, "shipment_date": "2024-01-16", "status": "delivered"},
    {"shipment_id": "SHP006", "customer_id": "CUST002", "shipping_cost": 35.50, "shipment_date": "2024-02-05", "status": "delivered"},
    {"shipment_id": "SHP007", "customer_id": "CUST005", "shipping_cost": 20.00, "shipment_date": "2024-02-10", "status": "in_transit"},
    {"shipment_id": "SHP008", "customer_id": "CUST001", "shipping_cost": 28.75, "shipment_date": "2024-02-12", "status": "delivered"},
    {"shipment_id": "SHP009", "customer_id": "CUST003", "shipping_cost": 42.00, "shipment_date": "2024-02-15", "status": "delivered"},
    {"shipment_id": "SHP010", "customer_id": "CUST006", "shipping_cost": 65.00, "shipment_date": "2024-02-18", "status": "delivered"},
    {"shipment_id": "SHP011", "customer_id": "CUST999", "shipping_cost": 18.50, "shipment_date": "2024-02-20", "status": "delivered"},
    {"shipment_id": "SHP012", "customer_id": "CUST002", "shipping_cost": -5.00, "shipment_date": "2024-02-22", "status": "delivered"},
    {"shipment_id": "SHP013", "customer_id": "CUST004", "shipping_cost": 0.00, "shipment_date": "2024-02-25", "status": "delivered"},
    {"shipment_id": "SHP014", "customer_id": None, "shipping_cost": 30.00, "shipment_date": "2024-02-28", "status": "delivered"},
    {"shipment_id": "SHP015", "customer_id": "CUST001", "shipping_cost": 22.50, "shipment_date": "2024-03-01", "status": "delivered"},
    {"shipment_id": "SHP016", "customer_id": "CUST003", "shipping_cost": 38.00, "shipment_date": "2024-03-05", "status": "delivered"},
    {"shipment_id": "SHP017", "customer_id": "CUST005", "shipping_cost": 50.00, "shipment_date": "2024-03-10", "status": "cancelled"},
    {"shipment_id": "SHP018", "customer_id": "CUST002", "shipping_cost": 33.75, "shipment_date": "2024-03-15", "status": "delivered"},
    {"shipment_id": "SHP019", "customer_id": "CUST006", "shipping_cost": 44.50, "shipment_date": "2024-03-20", "status": "delivered"},
    {"shipment_id": "SHP020", "customer_id": "CUST004", "shipping_cost": 27.00, "shipment_date": "2024-03-25", "status": "delivered"},
]

STAGE_NAMES = {
    "extract_shipments",
    "extract_customer_tiers",
    "transform_data",
    "load_analytics",
}


class _FakeResponse:
    def __init__(self, *, payload=None, json_error=None, status_error=None):
        self._payload = payload
        self._json_error = json_error
        self._status_error = status_error

    def raise_for_status(self):
        if self._status_error is not None:
            raise self._status_error

    def json(self):
        if self._json_error is not None:
            raise self._json_error
        return self._payload


@pytest.fixture
def hardening_harness(tmp_path, monkeypatch):
    tiers_source = Path(__file__).resolve().parents[1] / "data" / "customer_tiers.csv"
    tiers_path = tmp_path / "customer_tiers.csv"
    tiers_path.write_text(tiers_source.read_text(encoding="utf-8"), encoding="utf-8")

    monkeypatch.setenv("TIERS_CSV_PATH", str(tiers_path))
    monkeypatch.setattr(extract_shipments_module.time, "sleep", lambda _: None)

    state = {"restore_needed": False}

    def set_shipments_response(shipments, request_log=None):
        def fake_get(url, timeout):
            if request_log is not None:
                request_log.append((url, timeout))
            return _FakeResponse(payload={"data": shipments})

        monkeypatch.setenv("API_URL", "http://mocked.local/api/shipments")
        monkeypatch.setattr(extract_shipments_module.requests, "get", fake_get)

    def run_extract(shipments, pipeline_run_id=None):
        state["restore_needed"] = True
        set_shipments_response(shipments)
        return extract_shipments_from_api(pipeline_run_id or f"hardening-{uuid.uuid4()}")

    def run_pipeline(shipments=None, pipeline_run_id=None):
        pipeline_run_id = pipeline_run_id or f"hardening-{uuid.uuid4()}"
        state["restore_needed"] = True
        set_shipments_response(shipments or BASELINE_SHIPMENTS)
        extract_shipments_from_api(pipeline_run_id)
        extract_customer_tiers_from_csv(pipeline_run_id)
        transform_shipment_data(pipeline_run_id)
        load_analytics_data(pipeline_run_id)
        return pipeline_run_id

    yield {
        "run_extract": run_extract,
        "run_pipeline": run_pipeline,
        "set_shipments_response": set_shipments_response,
    }

    if state["restore_needed"]:
        run_pipeline(BASELINE_SHIPMENTS, pipeline_run_id=f"hardening-restore-{uuid.uuid4()}")


class TestHardening:
    def test_extract_shipments_retries_three_times_on_api_failure(self, monkeypatch):
        request_log = []

        def fake_get(url, timeout):
            request_log.append((url, timeout))
            raise extract_shipments_module.requests.exceptions.ConnectionError("connection refused")

        monkeypatch.setenv("API_URL", "http://bad.local/api/shipments")
        monkeypatch.setattr(extract_shipments_module.requests, "get", fake_get)
        monkeypatch.setattr(extract_shipments_module.time, "sleep", lambda _: None)

        with pytest.raises(RuntimeError, match="API failed after 3 attempts: connection refused"):
            extract_shipments_from_api(f"retry-test-{uuid.uuid4()}")

        assert len(request_log) == 3
        assert {url for url, _ in request_log} == {"http://bad.local/api/shipments"}

    def test_extract_shipments_raises_clear_error_on_malformed_payload(self, monkeypatch):
        def fake_get(url, timeout):
            return _FakeResponse(json_error=ValueError("Malformed JSON payload"))

        monkeypatch.setenv("API_URL", "http://mocked.local/api/shipments")
        monkeypatch.setattr(extract_shipments_module.requests, "get", fake_get)
        monkeypatch.setattr(extract_shipments_module.time, "sleep", lambda _: None)

        with pytest.raises(RuntimeError) as excinfo:
            extract_shipments_from_api(f"malformed-test-{uuid.uuid4()}")

        message = str(excinfo.value)
        assert message == "API failed after 3 attempts: Malformed JSON payload"
        assert "Traceback" not in message

    def test_ops_audit_history_records_pipeline_and_stage_runs(self, hardening_harness):
        pipeline_run_id = hardening_harness["run_pipeline"]()

        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT status
            FROM ops.pipeline_runs
            WHERE pipeline_run_id = %s;
            """,
            (pipeline_run_id,),
        )
        pipeline_statuses = [row[0] for row in cur.fetchall()]
        cur.execute(
            """
            SELECT stage_name, rows_read, rows_written
            FROM ops.stage_runs
            WHERE pipeline_run_id = %s
            ORDER BY stage_name;
            """,
            (pipeline_run_id,),
        )
        stage_rows = cur.fetchall()
        cur.close()
        conn.close()

        assert pipeline_statuses
        assert set(pipeline_statuses).issubset({"running", "completed", "success", "failed"})
        assert {stage_name for stage_name, _, _ in stage_rows} == STAGE_NAMES
        assert all(rows_read is not None and rows_written is not None for _, rows_read, rows_written in stage_rows)

    def test_raw_shipments_raw_is_append_only_across_pipeline_runs(self, hardening_harness):
        first_run_id = hardening_harness["run_pipeline"](pipeline_run_id=f"append-audit-{uuid.uuid4()}")
        second_run_id = hardening_harness["run_pipeline"](pipeline_run_id=f"append-audit-{uuid.uuid4()}")

        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT COUNT(DISTINCT pipeline_run_id)
            FROM raw.shipments_raw
            WHERE pipeline_run_id IN (%s, %s);
            """,
            (first_run_id, second_run_id),
        )
        distinct_runs = cur.fetchone()[0]
        cur.close()
        conn.close()

        assert distinct_runs == 2

    def test_shipping_spend_by_tier_has_primary_key_on_tier_and_year_month(self):
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
             AND tc.table_name = kcu.table_name
            WHERE tc.table_schema = 'analytics'
              AND tc.table_name = 'shipping_spend_by_tier'
              AND tc.constraint_type = 'PRIMARY KEY'
            ORDER BY kcu.ordinal_position;
            """
        )
        columns = [row[0] for row in cur.fetchall()]
        cur.close()
        conn.close()

        assert columns == ["tier", "year_month"]

    def test_pipeline_run_indexes_exist_on_raw_tables(self):
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT schemaname, tablename, indexname, indexdef
            FROM pg_indexes
            WHERE schemaname = 'raw'
              AND tablename IN ('shipments_raw', 'customer_tiers_raw');
            """
        )
        indexes = cur.fetchall()
        cur.close()
        conn.close()

        assert any(
            table == "shipments_raw" and "(pipeline_run_id)" in indexdef
            for _, table, _, indexdef in indexes
        )
        assert any(
            table == "customer_tiers_raw" and "(pipeline_run_id)" in indexdef
            for _, table, _, indexdef in indexes
        )

    def test_required_schemas_exist(self):
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name IN ('raw', 'staging', 'analytics', 'ops');
            """
        )
        schemas = {row[0] for row in cur.fetchall()}
        cur.close()
        conn.close()

        assert schemas == {"raw", "staging", "analytics", "ops"}

    def test_extract_shipments_fails_fast_when_no_valid_shipments_remain(self, hardening_harness):
        invalid_shipments = [
            {
                "shipment_id": "BAD001",
                "customer_id": "CUST001",
                "shipping_cost": -5.00,
                "shipment_date": "2024-01-01",
                "status": "delivered",
            },
            {
                "shipment_id": "BAD002",
                "customer_id": None,
                "shipping_cost": 10.00,
                "shipment_date": "2024-01-02",
                "status": "delivered",
            },
            {
                "shipment_id": "BAD003",
                "customer_id": "CUST003",
                "shipping_cost": -1.25,
                "shipment_date": "2024-01-03",
                "status": "delivered",
            },
        ]

        with pytest.raises(RuntimeError, match="No valid shipments remain after validation — aborting extract"):
            hardening_harness["run_extract"](invalid_shipments, pipeline_run_id=f"zero-valid-{uuid.uuid4()}")
