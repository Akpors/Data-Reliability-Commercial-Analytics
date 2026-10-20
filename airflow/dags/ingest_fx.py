"""FX rates: the one cron-scheduled ingest (an API has no file arrival to watch).

Scheduled runs sync incrementally up to the sim business date (``sync_fx`` advances the
watermark). Native backfill runs (``airflow backfill create``) reload just their logical date
(``sync_fx_range``) and leave the watermark alone.
"""

from datetime import UTC, datetime, timedelta

from airflow.sdk import dag, task
from airflow.sdk.exceptions import AirflowException, AirflowSkipException

from drct.orchestration.assets import RAW_ASSETS
from drct.orchestration.callbacks import record_dag_run, run_type_of


@dag(
    dag_id="ingest_fx",
    schedule="*/10 * * * *",
    start_date=datetime(2025, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,  # sync_fx reads then writes the watermark: no concurrent writers
    on_success_callback=record_dag_run,
    on_failure_callback=record_dag_run,
    tags=["ingestion", "api"],
)
def ingest_fx():
    @task(
        task_id="sync",
        outlets=[RAW_ASSETS["fx_rates"]],
        retries=3,
        retry_delay=timedelta(seconds=15),
        # Airflow 3.3 treats this as a multiplier (True == 1.0 means a constant delay);
        # 2.0 doubles the delay per try: ~15s, 30s, 60s.
        retry_exponential_backoff=2.0,
    )
    def sync(**context) -> None:
        from drct.chaos.scenarios import fx_should_fail
        from drct.chaos.state import active
        from drct.common.config import get_settings
        from drct.common.db import connect
        from drct.common.logging import get_logger
        from drct.common.sim_clock import (
            ClockNotInitialised,
            current_business_date,
            read_clock,
        )
        from drct.ingestion.fx import make_fx_client, sync_fx, sync_fx_range

        log = get_logger("drct.dags.ingest_fx")
        dag_run = context["dag_run"]
        batch_id = f"{dag_run.dag_id}:{dag_run.run_id}"
        settings = get_settings()
        with connect(settings.warehouse_dsn) as conn:
            try:
                clock = read_clock(conn)
            except ClockNotInitialised:
                raise AirflowSkipException(
                    "sim clock not initialised: run `make seed` first"
                ) from None
            # The one DAG-side chaos read: api_flaky makes the first tries fail, so retries show.
            if fx_should_fail(active(conn), context["ti"].try_number):
                raise AirflowException("chaos: FX API unavailable")
            client = make_fx_client(settings)
            if run_type_of(dag_run) == "backfill":
                day = context["logical_date"].date()
                written = sync_fx_range(conn, client, day, day, clock.now, batch_id)
            else:
                through = current_business_date(clock)
                written = sync_fx(conn, client, through, clock.now, batch_id)
        log.info("fx_task_done", run_type=run_type_of(dag_run), rates=written)

    sync()


ingest_fx()
