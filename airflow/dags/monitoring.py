"""Monitoring: freshness, volume and SLA detectors, then the incident reconcile.

Wakes on the ``monitoring.dbt_results`` asset (``dbt_transform`` emits it after every build, pass or
fail) and on a 30-minute cron, which catches stale data when nothing upstream fires.
Every task after ``source_freshness`` runs on ``all_done``: one failed detector must not stop the
others from running, and ``incidents`` must still reconcile whatever the detectors did record.
"""

from datetime import UTC, datetime
from pathlib import Path

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import AssetOrTimeSchedule, CronTriggerTimetable, dag, task
from airflow.sdk.exceptions import AirflowException, AirflowSkipException

from drct.orchestration.assets import DBT_RESULTS_ASSET
from drct.orchestration.callbacks import record_dag_run
from drct.orchestration.monitoring_run import failed_tasks, read_manifest

DBT_DIR = "/opt/drct/dbt"
# dbt_transform's build target: the incident engine reads its manifest.json for the blast radius.
DBT_TARGET = Path(DBT_DIR) / "target"
# Freshness gets its own target dir. It runs concurrently with dbt_transform's build and also writes
# manifest.json and partial-parse state, which dbt does not support sharing between invocations.
FRESHNESS_TARGET = Path(DBT_DIR) / "target_freshness"

# The stale sources.json is deleted first, so a dbt that fails before writing it cannot make
# publish_freshness re-insert the previous check's rows with a fresh checked_at. dbt deps runs only
# when the packages are missing (fresh clone). ``exit 0``: a warn/error freshness result (dbt exits
# non-zero) is data for publish_freshness, not a task failure.
SOURCE_FRESHNESS = f"""rm -f {FRESHNESS_TARGET}/sources.json
[ -d {DBT_DIR}/dbt_packages ] || $DBT_BIN deps --project-dir {DBT_DIR} --profiles-dir {DBT_DIR}
$DBT_BIN source freshness --project-dir {DBT_DIR} --profiles-dir {DBT_DIR} --target-path {FRESHNESS_TARGET.name}
exit 0
"""


UPSTREAM_TASKS = ["source_freshness", "publish_freshness", "volume", "sla"]


def _read_clock(conn):
    """The sim clock, or skip the task: before ``make seed`` there is nothing to monitor."""
    from drct.common.sim_clock import ClockNotInitialised, read_clock

    try:
        return read_clock(conn)
    except ClockNotInitialised:
        raise AirflowSkipException("sim clock not initialised: run `make seed` first") from None


@dag(
    dag_id="monitoring",
    schedule=AssetOrTimeSchedule(
        timetable=CronTriggerTimetable("*/30 * * * *", timezone="UTC"),
        assets=DBT_RESULTS_ASSET,
    ),
    start_date=datetime(2025, 1, 1, tzinfo=UTC),
    catchup=False,
    # Load-bearing: two concurrent reconciles would both see "no open incident" for a subject and
    # race on the one-open-incident-per-(type, asset) unique index.
    max_active_runs=1,
    on_success_callback=record_dag_run,
    on_failure_callback=record_dag_run,
    tags=["monitoring"],
)
def monitoring():
    source_freshness = BashOperator(task_id="source_freshness", bash_command=SOURCE_FRESHNESS)

    @task(task_id="publish_freshness", trigger_rule="all_done")
    def publish_freshness() -> None:
        from drct.common.db import connect
        from drct.common.logging import get_logger
        from drct.monitoring.dbt_artifacts import publish_freshness as publish

        log = get_logger("drct.dags.monitoring")
        with connect() as conn:
            rows = publish(conn, FRESHNESS_TARGET)
        log.info("freshness_published", rows=rows)

    @task(task_id="volume", trigger_rule="all_done")
    def volume() -> None:
        from drct.common.db import connect
        from drct.common.logging import get_logger
        from drct.monitoring.volume import compute_volume_metrics, evaluate_volume

        log = get_logger("drct.dags.monitoring")
        with connect() as conn:
            clock = _read_clock(conn)
            metrics = compute_volume_metrics(conn)
            results = evaluate_volume(conn, clock.now)
        anomalies = sum(1 for _, _, result in results if result.is_anomaly)
        log.info("volume_evaluated", metrics=metrics, sources=len(results), anomalies=anomalies)

    @task(task_id="sla", trigger_rule="all_done")
    def sla() -> None:
        from drct.common.db import connect
        from drct.common.logging import get_logger
        from drct.monitoring.sla import evaluate_all, load_sla_rules

        log = get_logger("drct.dags.monitoring")
        with connect() as conn:
            _read_clock(conn)
            results = evaluate_all(conn, load_sla_rules(), datetime.now(UTC))
        breached = sum(1 for r in results if r.status == "breached")
        log.info("sla_evaluated", rules=len(results), breached=breached)

    @task(task_id="incidents", trigger_rule="all_done")
    def incidents(**context) -> None:
        from drct.common.db import connect
        from drct.common.logging import get_logger
        from drct.monitoring.checks import collect_checks
        from drct.monitoring.incidents import reconcile

        log = get_logger("drct.dags.monitoring")
        manifest = read_manifest(DBT_TARGET / "manifest.json")
        with connect() as conn:
            clock = _read_clock(conn)
            changes = reconcile(conn, collect_checks(conn), clock.now, manifest)
        log.info(
            "incidents_reconciled",
            opened=len(changes.to_open),
            updated=len(changes.to_update),
            resolved=len(changes.to_resolve),
        )
        # incidents is the only leaf and runs on all_done, so Airflow would mark the run success even
        # when a detector failed. Fail it here, after reconcile has committed, so a failed detector
        # still never hides the others' incidents but the run, its record and the next cycle's
        # pipeline_failure check do see the failure.
        run_id = context["run_id"]
        states = context["ti"].get_task_states(
            dag_id=context["dag_run"].dag_id, task_ids=UPSTREAM_TASKS, run_ids=[run_id]
        )[run_id]
        failed = failed_tasks(states)
        if failed:
            raise AirflowException(f"monitoring detectors failed: {', '.join(failed)}")

    source_freshness >> publish_freshness() >> volume() >> sla() >> incidents()


monitoring()
