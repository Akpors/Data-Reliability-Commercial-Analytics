"""dbt transform: runs when new raw data lands, and publishes the dbt results for monitoring.

Scheduled on the raw-table assets, so the chain is landing -> raw -> dbt with no cron: sales and
payments are only transformed together, the other sources on their own. ``publish_dbt_artifacts``
runs even when dbt fails (``all_done``), because the failing test results are what the monitoring
DAG turns into incidents.
"""

from datetime import UTC, datetime
from pathlib import Path

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task

from drct.orchestration.assets import DBT_RESULTS_ASSET, MARTS_ASSET, RAW_ASSETS
from drct.orchestration.callbacks import record_dag_run

DBT_DIR = "/opt/drct/dbt"
DBT_TARGET = Path(DBT_DIR) / "target"

# The stale run_results.json is deleted before anything else (even dbt deps), so a failure in deps or
# a dbt crash before it writes results cannot make publish re-insert the previous run's results
# (publish needs it). manifest.json is kept: the monitoring DAG reads it concurrently for the blast
# radius. dbt deps runs only when the packages are missing (fresh clone).
DBT_BUILD = f"""set -e
rm -f {DBT_DIR}/target/run_results.json
[ -d {DBT_DIR}/dbt_packages ] || $DBT_BIN deps --project-dir {DBT_DIR} --profiles-dir {DBT_DIR}
$DBT_BIN build --project-dir {DBT_DIR} --profiles-dir {DBT_DIR} --target-path target
"""


@dag(
    dag_id="dbt_transform",
    schedule=(RAW_ASSETS["sales"] & RAW_ASSETS["payments"])
    | RAW_ASSETS["customers"]
    | RAW_ASSETS["products"]
    | RAW_ASSETS["fx_rates"],
    start_date=datetime(2025, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    on_success_callback=record_dag_run,
    on_failure_callback=record_dag_run,
    tags=["transform", "event-driven"],
)
def dbt_transform():
    dbt_build = BashOperator(task_id="dbt_build", bash_command=DBT_BUILD)

    @task(task_id="mark_marts", trigger_rule="all_success", outlets=[MARTS_ASSET])
    def mark_marts() -> None:
        """No-op: emitting the marts asset only when the whole build succeeded."""

    @task(
        task_id="publish_dbt_artifacts",
        trigger_rule="all_done",
        outlets=[DBT_RESULTS_ASSET],
    )
    def publish() -> None:
        from drct.common.db import connect
        from drct.common.logging import get_logger
        from drct.common.sim_clock import read_clock
        from drct.monitoring.dbt_artifacts import publish_dbt_artifacts

        log = get_logger("drct.dags.dbt_transform")
        with connect() as conn:
            published = publish_dbt_artifacts(conn, DBT_TARGET, read_clock(conn).now)
        log.info("dbt_artifacts_published", rows=published)

    dbt_build >> [mark_marts(), publish()]


dbt_transform()
