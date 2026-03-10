"""
Conftest: verifies pipeline data exists in postgres, provides DB connection helper.
Connects from host to postgres on port 5433 (or inside Docker on default port).
"""
import pytest
import psycopg2
import os

DB_CONFIG = {
    "host": os.environ.get("POSTGRES_HOST", "127.0.0.1"),
    "port": int(os.environ.get("POSTGRES_PORT", "5433")),
    "database": os.environ.get("POSTGRES_DB", "airflow"),
    "user": os.environ.get("POSTGRES_USER", "airflow"),
    "password": os.environ.get("POSTGRES_PASSWORD", "airflow"),
}


def get_conn():
    return psycopg2.connect(**DB_CONFIG, connect_timeout=5)


@pytest.fixture(scope="session", autouse=True)
def verify_data():
    """Verify pipeline data exists before running tests."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM staging.shipments;")
    ship = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM analytics.shipping_spend_by_tier;")
    analytics = cur.fetchone()[0]
    cur.close()
    conn.close()
    if ship == 0 or analytics == 0:
        raise RuntimeError("Pipeline has not been run. Run it first via docker exec.")
    print(f"\nVerified: {ship} shipments, {analytics} analytics rows")
    yield
