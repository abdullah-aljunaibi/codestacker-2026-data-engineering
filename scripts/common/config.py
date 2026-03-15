"""Environment-backed configuration helpers."""

from __future__ import annotations

import os
import socket
from typing import Dict


def _default_host_and_port() -> tuple[str, int]:
    host = os.environ.get("POSTGRES_HOST", "postgres")
    port = int(os.environ.get("POSTGRES_PORT", "5433" if host in {"127.0.0.1", "localhost"} else "5432"))
    return host, port


def get_db_config() -> Dict[str, object]:
    host, port = _default_host_and_port()
    if host == "postgres":
        try:
            socket.getaddrinfo(host, port)
        except socket.gaierror:
            host, port = "127.0.0.1", 5433

    return {
        "host": host,
        "port": port,
        "database": os.environ.get("POSTGRES_DB", "airflow"),
        "user": os.environ.get("POSTGRES_USER", "airflow"),
        "password": os.environ.get("POSTGRES_PASSWORD", "airflow"),
    }


def get_api_url() -> str:
    return os.environ.get("API_URL", "http://api:8000/api/shipments")


def get_tiers_csv_path() -> str:
    return os.environ.get("TIERS_CSV_PATH", "/opt/airflow/data/customer_tiers.csv")

