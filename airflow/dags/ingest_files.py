"""Event-driven file ingestion: one DAG per landing source, run when files arrive (no cron).

Each ``landing.<source>`` asset is watched by a ``FileArrivalTrigger`` in the triggerer; a new
file emits an asset event, which starts ``ingest_<source>``. Its ``load`` task loads every pending
file (so a duplicate event is a harmless no-op run) and emits ``raw.<source>``.
"""

from datetime import UTC, datetime, timedelta
from typing import Any

from airflow.sdk import dag, task

from drct.orchestration.assets import FILE_SOURCES, LANDING_ASSETS, RAW_ASSETS
from drct.orchestration.callbacks import record_dag_run, run_type_of


def load_source(source: str, context: dict[str, Any]) -> None:
    """Load the pending files of ``source`` and log a task-level run record."""
    from drct.common.config import get_settings
    from drct.common.db import connect
    from drct.common.logging import get_logger
    from drct.common.run_log import RunRecord, record_run
    from drct.common.sim_clock import read_clock
    from drct.ingestion.loader import load_pending

    log = get_logger("drct.dags.ingest_files", source=source)
    started_at = datetime.now(UTC)
    dag_run = context["dag_run"]
    batch_id = f"{dag_run.dag_id}:{dag_run.run_id}"
    settings = get_settings()
    with connect(settings.warehouse_dsn) as conn:
        # Raises ClockNotInitialised while a seed is still landing files; the retry covers that.
        sim_now = read_clock(conn).now
        results = load_pending(conn, source, settings.landing_dir, sim_now, batch_id)
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
                task_id="load",
                try_number=context["ti"].try_number,
                business_date=max((r.business_date for r in loaded), default=None),
                rows_in=sum(r.rows_in for r in results),
                rows_rejected=sum(r.rows_in for r in rejected),
                sim_now=sim_now,
            ),
        )
    log.info("ingest_done", files=len(results), loaded=len(loaded), quarantined=len(rejected))


def build_ingest_dag(source: str):
    @dag(
        dag_id=f"ingest_{source}",
        schedule=[LANDING_ASSETS[source]],
        start_date=datetime(2025, 1, 1, tzinfo=UTC),
        catchup=False,
        max_active_runs=1,
        on_success_callback=record_dag_run,
        on_failure_callback=record_dag_run,
        tags=["ingestion", "event-driven"],
    )
    def ingest():
        @task(
            task_id="load",
            outlets=[RAW_ASSETS[source]],
            retries=2,
            retry_delay=timedelta(seconds=30),
        )
        def load(**context) -> None:
            load_source(source, context)

        load()

    return ingest()


for _source in FILE_SOURCES:
    globals()[f"ingest_{_source}"] = build_ingest_dag(_source)
