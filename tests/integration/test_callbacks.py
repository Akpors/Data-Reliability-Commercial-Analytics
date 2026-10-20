"""record_dag_run: the DAG-level run log written by on_success/on_failure callbacks."""

import os
from datetime import UTC, date, datetime, timedelta
from types import SimpleNamespace

import pytest
from psycopg.conninfo import make_conninfo

from drct.common.sim_clock import init_clock
from drct.orchestration.callbacks import record_dag_run

START = datetime(2026, 10, 20, 9, 0, tzinfo=UTC)
END = START + timedelta(seconds=42)


@pytest.fixture
def warehouse(db_conn, monkeypatch):
    """Point the callback's default connection at the throwaway test database."""
    dsn = make_conninfo(os.environ["TEST_ADMIN_DSN"], dbname=db_conn.info.dbname)
    monkeypatch.setenv("WAREHOUSE_DSN", dsn)
    return db_conn


def _context(reason="success", end_date=END, run_type="asset_triggered"):
    dag_run = SimpleNamespace(
        dag_id="ingest_sales",
        run_id="asset_triggered__2026-10-20T09:00:00+00:00",
        run_type=run_type,
        start_date=START,
        end_date=end_date,
    )
    return {"dag_run": dag_run, "run_id": dag_run.run_id, "reason": reason}


def _rows(conn):
    return conn.execute(
        """
        select dag_id, run_id, task_id, run_type, status, started_at, ended_at, duration_s, sim_now
          from monitoring.pipeline_runs
        """
    ).fetchall()


def test_success_writes_dag_level_run_with_sim_now(warehouse):
    clock = init_clock(warehouse, date(2026, 4, 23), datetime(2026, 7, 22, 6, tzinfo=UTC))
    record_dag_run(_context())
    assert _rows(warehouse) == [
        (
            "ingest_sales",
            "asset_triggered__2026-10-20T09:00:00+00:00",
            None,
            "asset_triggered",
            "success",
            START,
            END,
            42,
            clock.now,
        )
    ]


def test_failure_reason_records_failed_and_backfill_run_type(warehouse):
    record_dag_run(_context(reason="task_failure", run_type="backfill"))
    [row] = _rows(warehouse)
    assert (row[3], row[4]) == ("backfill", "failed")


def test_missing_end_date_uses_now_and_missing_clock_gives_null_sim_now(warehouse):
    before = datetime.now(UTC)
    record_dag_run(_context(end_date=None))
    [row] = _rows(warehouse)
    assert row[6] >= before and row[8] is None


def test_database_errors_are_swallowed(warehouse, monkeypatch):
    monkeypatch.setenv("WAREHOUSE_DSN", "postgresql://nobody:wrong@127.0.0.1:1/none")
    record_dag_run(_context())  # must not raise into Airflow


def test_context_without_dag_run_is_ignored(warehouse):
    record_dag_run({"run_id": "manual__x", "reason": "success"})  # minimal callback context
    assert _rows(warehouse) == []
