import re
from pathlib import Path

from airflow.sdk.definitions.timetables.assets import AssetOrTimeSchedule

from drct.orchestration.callbacks import record_dag_run

DETECTORS = ["publish_freshness", "volume", "sla", "incidents"]


def test_monitoring_runs_on_dbt_results_or_cron(dagbag):
    dag = dagbag.get_dag("monitoring")
    assert "monitoring.dbt_results" in str(dag.timetable) and "*/30" in str(dag.timetable)
    assert dag.get_task("incidents").trigger_rule == "all_done"
    assert [t.task_id for t in dag.topological_sort()] == [
        "source_freshness",
        "publish_freshness",
        "volume",
        "sla",
        "incidents",
    ]


def test_schedule_is_asset_or_cron_in_utc(dagbag):
    dag = dagbag.get_dag("monitoring")
    assert isinstance(dag.timetable, AssetOrTimeSchedule)
    assert dag.timetable.timetable.expression == "*/30 * * * *"
    assert str(dag.timetable.timetable.timezone) == "UTC"
    assert dag.timetable.asset_condition.name == "monitoring.dbt_results"


def test_one_run_at_a_time_without_catchup_and_recorded(dagbag):
    dag = dagbag.get_dag("monitoring")
    assert dag.max_active_runs == 1  # concurrent reconciles would race on the open-incident index
    assert dag.catchup is False
    assert dag.on_success_callback is record_dag_run
    assert dag.on_failure_callback is record_dag_run


def test_one_failed_detector_never_hides_the_others(dagbag):
    dag = dagbag.get_dag("monitoring")
    for task_id in DETECTORS:
        assert dag.get_task(task_id).trigger_rule == "all_done", task_id


def test_source_freshness_command(dagbag):
    command = dagbag.get_dag("monitoring").get_task("source_freshness").bash_command
    assert (
        "$DBT_BIN source freshness --project-dir /opt/drct/dbt --profiles-dir /opt/drct/dbt"
        in command
    )
    assert command.rstrip().endswith("exit 0")  # a stale source is a result, not a task failure
    # stale results are deleted first, so a failed dbt cannot let publish re-insert old rows
    assert command.index("rm -f /opt/drct/dbt/target_freshness/sources.json") < command.index(
        "source freshness"
    )
    assert "dbt_packages" in command and "deps" in command


def _target_path(command: str) -> Path:
    """The ``--target-path`` of a dbt command, resolved against the dbt project dir."""
    project = re.search(r"--project-dir (\S+)", command).group(1)
    return Path(project) / re.search(r"--target-path (\S+)", command).group(1)


def test_freshness_and_build_never_share_a_dbt_target_dir(dagbag):
    """Review I2: freshness also writes manifest.json and partial-parse state, and it runs
    concurrently with dbt_transform's build, so the two need separate target dirs."""
    monitoring = dagbag.get_dag("monitoring")
    freshness = _target_path(monitoring.get_task("source_freshness").bash_command)
    build = _target_path(dagbag.get_dag("dbt_transform").get_task("dbt_build").bash_command)
    assert freshness == Path("/opt/drct/dbt/target_freshness")
    assert freshness != build
    # publish_freshness reads sources.json where freshness writes it ...
    publish = monitoring.get_task("publish_freshness").python_callable
    assert publish.__globals__["FRESHNESS_TARGET"] == freshness
    # ... and the incident engine's blast radius still comes from the build's manifest.
    incidents = monitoring.get_task("incidents").python_callable
    assert incidents.__globals__["DBT_TARGET"] == build
