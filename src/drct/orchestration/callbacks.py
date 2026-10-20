"""DAG-level run-log callback, shared by ``on_success_callback`` and ``on_failure_callback``."""

from datetime import UTC, datetime
from typing import Any

from drct.common.db import connect
from drct.common.logging import get_logger
from drct.common.run_log import RunRecord, record_run
from drct.common.sim_clock import ClockNotInitialised, read_clock


def record_dag_run(context: dict[str, Any]) -> None:
    """Write the DAG run's ``RunRecord`` (``task_id`` None) to ``monitoring.pipeline_runs``.

    Status comes from the callback reason (``"success"`` for the success callback, a failure
    reason otherwise). ``end_date`` may not be stamped yet when the callback runs, so now() stands
    in. ``sim_now`` is the sim clock, or None before seed. Never raises into Airflow.
    """
    log = get_logger("drct.orchestration.callbacks")
    dag_run = context.get("dag_run")
    if dag_run is None:
        # Airflow passes a minimal context (no dag_run) when no task instance finished in the
        # outcome, e.g. a run whose only task was skipped.
        log.warning("dag_run_not_recorded", run_id=context.get("run_id"), reason="no dag_run")
        return
    try:
        with connect() as conn:
            try:
                sim_now = read_clock(conn).now
            except ClockNotInitialised:
                sim_now = None
            record_run(
                conn,
                RunRecord(
                    dag_id=dag_run.dag_id,
                    run_id=dag_run.run_id,
                    run_type=run_type_of(dag_run),
                    status="success" if context.get("reason") == "success" else "failed",
                    started_at=dag_run.start_date,
                    ended_at=dag_run.end_date or datetime.now(UTC),
                    task_id=None,
                    sim_now=sim_now,
                ),
            )
    except Exception:
        log.exception("dag_run_record_failed", dag_id=dag_run.dag_id, run_id=dag_run.run_id)


def run_type_of(dag_run: Any) -> str:
    """``dag_run.run_type`` as plain text (the SDK model carries a str-enum whose str() is
    ``"DagRunType.X"``)."""
    return getattr(dag_run.run_type, "value", dag_run.run_type)
