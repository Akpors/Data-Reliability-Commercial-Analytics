"""Pipeline run log: one row per (dag, run, task, try) in monitoring.pipeline_runs."""

from dataclasses import dataclass
from datetime import date, datetime

import psycopg


@dataclass
class RunRecord:
    dag_id: str
    run_id: str
    run_type: str
    status: str
    started_at: datetime
    ended_at: datetime | None
    task_id: str | None = None
    try_number: int = 1
    business_date: date | None = None
    rows_in: int | None = None
    rows_rejected: int | None = None
    sim_now: datetime | None = None


def record_run(conn: psycopg.Connection, rec: RunRecord) -> None:
    """Upsert a run on its unique key; duration_s is whole seconds, null while ended_at is null."""
    duration_s = (
        int((rec.ended_at - rec.started_at).total_seconds()) if rec.ended_at is not None else None
    )
    conn.execute(
        """
        insert into monitoring.pipeline_runs
            (dag_id, run_id, task_id, run_type, status, try_number, started_at, ended_at,
             duration_s, business_date, rows_in, rows_rejected, sim_now)
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        on conflict (dag_id, run_id, (coalesce(task_id, '')), try_number) do update
            set run_type = excluded.run_type,
                status = excluded.status,
                started_at = excluded.started_at,
                ended_at = excluded.ended_at,
                duration_s = excluded.duration_s,
                business_date = excluded.business_date,
                rows_in = excluded.rows_in,
                rows_rejected = excluded.rows_rejected,
                sim_now = excluded.sim_now
        """,
        (
            rec.dag_id,
            rec.run_id,
            rec.task_id,
            rec.run_type,
            rec.status,
            rec.try_number,
            rec.started_at,
            rec.ended_at,
            duration_s,
            rec.business_date,
            rec.rows_in,
            rec.rows_rejected,
            rec.sim_now,
        ),
    )
