"""Database helpers."""

from __future__ import annotations

from contextlib import contextmanager

import psycopg2

from common.config import get_db_config


def get_connection():
    return psycopg2.connect(**get_db_config())


@contextmanager
def transaction(conn):
    try:
        yield
    except Exception:
        conn.rollback()
        raise
    else:
        conn.commit()

