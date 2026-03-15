"""Pipeline and stage run tracking."""

from __future__ import annotations

import os
import uuid
from contextlib import contextmanager

from common.db import get_connection


PIPELINE_NAME = "shipment_analytics_pipeline"


def generate_run_id() -> str:
    return str(uuid.uuid4())


def get_pipeline_run_id(pipeline_run_id=None) -> str:
    if pipeline_run_id:
        return pipeline_run_id

    env_run_id = os.environ.get("PIPELINE_RUN_ID")
    if env_run_id:
        return env_run_id

    airflow_dag_id = os.environ.get("AIRFLOW_CTX_DAG_ID")
    airflow_run_id = os.environ.get("AIRFLOW_CTX_DAG_RUN_ID")
    if airflow_dag_id and airflow_run_id:
        return str(uuid.uuid5(uuid.NAMESPACE_URL, f"{airflow_dag_id}:{airflow_run_id}"))

    return generate_run_id()


def ensure_ops_schema(cursor):
    cursor.execute("CREATE SCHEMA IF NOT EXISTS ops;")
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS ops.pipeline_runs (
            pipeline_run_id TEXT PRIMARY KEY,
            pipeline_name TEXT NOT NULL,
            started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            finished_at TIMESTAMP,
            status TEXT NOT NULL,
            error_message TEXT
        );
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS ops.stage_runs (
            id SERIAL PRIMARY KEY,
            pipeline_run_id TEXT NOT NULL REFERENCES ops.pipeline_runs(pipeline_run_id),
            stage_name TEXT NOT NULL,
            started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            finished_at TIMESTAMP,
            status TEXT NOT NULL,
            rows_read INTEGER,
            rows_written INTEGER,
            rows_rejected INTEGER,
            retry_count INTEGER NOT NULL DEFAULT 0,
            error_message TEXT
        );
        """
    )


def _run_ops_write(callback):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        ensure_ops_schema(cursor)
        result = callback(cursor)
        conn.commit()
        return result
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


def ensure_pipeline_run(pipeline_run_id, pipeline_name=PIPELINE_NAME):
    def _callback(cursor):
        cursor.execute(
            """
            INSERT INTO ops.pipeline_runs (
                pipeline_run_id,
                pipeline_name,
                status,
                started_at,
                finished_at,
                error_message
            )
            VALUES (%s, %s, 'running', CURRENT_TIMESTAMP, NULL, NULL)
            ON CONFLICT (pipeline_run_id) DO UPDATE
            SET pipeline_name = EXCLUDED.pipeline_name,
                status = 'running',
                error_message = NULL,
                finished_at = NULL;
            """,
            (pipeline_run_id, pipeline_name),
        )

    _run_ops_write(_callback)


def complete_pipeline_run(pipeline_run_id, status, error_message=None):
    def _callback(cursor):
        cursor.execute(
            """
            UPDATE ops.pipeline_runs
            SET status = %s,
                finished_at = CURRENT_TIMESTAMP,
                error_message = %s
            WHERE pipeline_run_id = %s;
            """,
            (status, error_message, pipeline_run_id),
        )

    _run_ops_write(_callback)


@contextmanager
def stage_run(pipeline_run_id, stage_name, retry_count=0):
    ensure_pipeline_run(pipeline_run_id)

    def _insert_stage(cursor):
        cursor.execute(
            """
            INSERT INTO ops.stage_runs (
                pipeline_run_id,
                stage_name,
                status,
                retry_count
            )
            VALUES (%s, %s, 'running', %s)
            RETURNING id;
            """,
            (pipeline_run_id, stage_name, retry_count),
        )
        return cursor.fetchone()[0]

    stage_run_id = _run_ops_write(_insert_stage)
    metrics = {"rows_read": None, "rows_written": None, "rows_rejected": None}

    try:
        yield metrics
    except Exception as exc:
        def _fail_stage(cursor):
            cursor.execute(
                """
                UPDATE ops.stage_runs
                SET finished_at = CURRENT_TIMESTAMP,
                    status = 'failed',
                    rows_read = %s,
                    rows_written = %s,
                    rows_rejected = %s,
                    error_message = %s
                WHERE id = %s;
                """,
                (
                    metrics["rows_read"],
                    metrics["rows_written"],
                    metrics["rows_rejected"],
                    str(exc),
                    stage_run_id,
                ),
            )

        _run_ops_write(_fail_stage)
        complete_pipeline_run(pipeline_run_id, "failed", str(exc))
        raise
    else:
        def _complete_stage(cursor):
            cursor.execute(
                """
                UPDATE ops.stage_runs
                SET finished_at = CURRENT_TIMESTAMP,
                    status = 'success',
                    rows_read = %s,
                    rows_written = %s,
                    rows_rejected = %s,
                    error_message = NULL
                WHERE id = %s;
                """,
                (
                    metrics["rows_read"],
                    metrics["rows_written"],
                    metrics["rows_rejected"],
                    stage_run_id,
                ),
            )

        _run_ops_write(_complete_stage)
