"""Manual re-ingestion of a date range from ``_resend/`` / ``_processed/``.

It emits every file source's ``raw.*`` asset, so ``dbt_transform`` re-runs on the reloaded data.
"""

from datetime import UTC, date, datetime
from typing import Any

from airflow.sdk import Param, dag, task

from drct.orchestration.assets import FILE_SOURCES, RAW_ASSETS
from drct.orchestration.callbacks import record_dag_run, run_type_of


def reload_range(start: date, end: date, context: dict[str, Any]) -> None:
    """Reprocess ``start``..``end`` for every file source and log a task-level run record."""
    from drct.common.config import get_settings
    from drct.common.db import connect
    from drct.common.logging import get_logger
    from drct.common.run_log import RunRecord, record_run
    from drct.common.sim_clock import read_clock
    from drct.ingestion.loader import reprocess_files

    log = get_logger("drct.dags.reprocess")
    started_at = datetime.now(UTC)
    dag_run = context["dag_run"]
    batch_id = f"{dag_run.dag_id}:{dag_run.run_id}"
    settings = get_settings()
    with connect(settings.warehouse_dsn) as conn:
        sim_now = read_clock(conn).now
        results = reprocess_files(
            conn, settings.landing_dir, start, end, sim_now, batch_id, sources=FILE_SOURCES
        )
        loaded = [r for r in results if r.status == "loaded"]
        rejected = [r for r in results if r.status == "quarantined"]
        record_run(
            conn,
            RunRecord(
                dag_id=dag_run.dag_id,
                run_id=dag_run.run_id,
                run_type=run_type_of(dag_run),
                status="success",
                started_at=started_at,
                ended_at=datetime.now(UTC),
                task_id="reload",
                try_number=context["ti"].try_number,
                business_date=max((r.business_date for r in loaded), default=None),
                rows_in=sum(r.rows_in for r in results),
                rows_rejected=sum(r.rows_in for r in rejected),
                sim_now=sim_now,
            ),
        )
    log.info(
        "reprocess_done",
        start=start.isoformat(),
        end=end.isoformat(),
        files=len(results),
        loaded=len(loaded),
        quarantined=len(rejected),
    )


@dag(
    dag_id="reprocess",
    schedule=None,
    start_date=datetime(2025, 1, 1, tzinfo=UTC),
    catchup=False,
    # One at a time: a retried trigger can queue a duplicate run, and two overlapping
    # delete-then-insert reloads of the same business date could duplicate rows.
    max_active_runs=1,
    params={
        "start": Param(type="string", format="date"),
        "end": Param(type="string", format="date"),
    },
    on_success_callback=record_dag_run,
    on_failure_callback=record_dag_run,
    tags=["ingestion", "manual"],
)
def reprocess():
    @task(task_id="reload", outlets=[RAW_ASSETS[s] for s in FILE_SOURCES])
    def reload(**context) -> None:
        params = context["params"]
        reload_range(
            date.fromisoformat(params["start"]), date.fromisoformat(params["end"]), context
        )

    reload()


reprocess()
