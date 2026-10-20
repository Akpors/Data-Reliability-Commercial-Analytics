from datetime import UTC, date, datetime

import pytest

from drct.common.run_log import RunRecord, record_run

START = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
END = datetime(2026, 1, 1, 0, 1, tzinfo=UTC)


def _rec(**overrides):
    base = {
        "dag_id": "ingest_sales",
        "run_id": "r1",
        "run_type": "asset_triggered",
        "status": "success",
        "started_at": START,
        "ended_at": END,
        "task_id": "load",
    }
    return RunRecord(**{**base, **overrides})


@pytest.mark.db
def test_record_run_is_idempotent_and_computes_duration(db_conn):
    rec = RunRecord(
        "ingest_sales",
        "r1",
        "asset_triggered",
        "success",
        datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
        datetime(2026, 1, 1, 0, 1, tzinfo=UTC),
        task_id="load",
    )
    record_run(db_conn, rec)
    record_run(db_conn, rec)
    rows = db_conn.execute("select duration_s from monitoring.pipeline_runs").fetchall()
    assert rows == [(60,)]


@pytest.mark.db
def test_record_run_upsert_updates_status_and_duration(db_conn):
    record_run(db_conn, _rec(status="running", ended_at=None))
    row = db_conn.execute("select status, ended_at, duration_s from monitoring.pipeline_runs")
    assert row.fetchall() == [("running", None, None)]

    record_run(db_conn, _rec(rows_in=10, rows_rejected=2, business_date=date(2026, 1, 1)))
    rows = db_conn.execute(
        "select status, duration_s, rows_in, rows_rejected, business_date "
        "from monitoring.pipeline_runs"
    ).fetchall()
    assert rows == [("success", 60, 10, 2, date(2026, 1, 1))]


@pytest.mark.db
def test_record_run_duration_is_whole_seconds(db_conn):
    end = datetime(2026, 1, 1, 0, 0, 48, 900_000, tzinfo=UTC)
    record_run(db_conn, _rec(ended_at=end))
    assert db_conn.execute("select duration_s from monitoring.pipeline_runs").fetchall() == [(48,)]


@pytest.mark.db
def test_record_run_keys_on_task_and_try_number(db_conn):
    record_run(db_conn, _rec())
    record_run(db_conn, _rec(try_number=2))
    record_run(db_conn, _rec(task_id="other"))
    record_run(db_conn, _rec(task_id=None))
    record_run(db_conn, _rec(task_id=None))  # run-level row upserts onto itself
    count = db_conn.execute("select count(*) from monitoring.pipeline_runs").fetchone()[0]
    assert count == 4


@pytest.mark.db
def test_record_run_stores_sim_now(db_conn):
    sim = datetime(2026, 7, 18, 6, tzinfo=UTC)
    record_run(db_conn, _rec(sim_now=sim))
    assert db_conn.execute("select sim_now from monitoring.pipeline_runs").fetchone()[0] == sim
