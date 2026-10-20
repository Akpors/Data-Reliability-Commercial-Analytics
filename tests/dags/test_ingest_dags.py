"""Ingestion DAG wiring: file DAGs are asset-triggered, FX is cron with retries."""

from datetime import timedelta

# DagBag holds SDK DAGs, whose timetables are the SDK definitions (not airflow.timetables.*).
from airflow.sdk.definitions.timetables.assets import AssetTriggeredTimetable

from drct.orchestration.assets import FILE_SOURCES, LANDING_ASSETS, RAW_ASSETS
from drct.orchestration.callbacks import record_dag_run
from drct.orchestration.triggers import FileArrivalTrigger


def test_file_ingest_dags_are_asset_scheduled(dagbag):
    for s in ("sales", "payments", "customers", "products"):
        dag = dagbag.get_dag(f"ingest_{s}")
        assert isinstance(dag.timetable, AssetTriggeredTimetable)  # asset-triggered, not cron
        assert dag.get_task("load").outlets[0].name == f"raw.{s}"


def test_fx_is_cron_with_retries(dagbag):
    t = dagbag.get_dag("ingest_fx").get_task("sync")
    assert t.retries == 3 and t.retry_exponential_backoff


def test_file_ingest_dags_watch_their_landing_asset_one_run_at_a_time(dagbag):
    for s in FILE_SOURCES:
        dag = dagbag.get_dag(f"ingest_{s}")
        assert dag.max_active_runs == 1
        assert [a.name for a in dag.timetable.asset_condition.objects] == [f"landing.{s}"]
        assert dag.on_success_callback is record_dag_run
        assert dag.on_failure_callback is record_dag_run
        assert dag.get_task("load").retries == 2  # covers files landing before the seed clock


def test_fx_dag_schedule_and_outlet(dagbag):
    dag = dagbag.get_dag("ingest_fx")
    task = dag.get_task("sync")
    assert dag.timetable.expression == "*/10 * * * *"
    assert dag.catchup is False and dag.max_active_runs == 1
    assert task.retry_delay == timedelta(seconds=15)
    assert task.retry_exponential_backoff == 2.0  # 3.3 multiplier: True would mean constant
    assert [a.name for a in task.outlets] == ["raw.fx_rates"]
    assert dag.on_success_callback is record_dag_run
    assert dag.on_failure_callback is record_dag_run


def test_asset_catalogue():
    assert FILE_SOURCES == ("sales", "payments", "customers", "products")
    assert set(RAW_ASSETS) == {*FILE_SOURCES, "fx_rates"}
    assert RAW_ASSETS["fx_rates"].uri == "postgres://warehouse:5432/warehouse/raw/fx_rates"
    patterns = {"sales": "csv", "payments": "parquet", "customers": "json", "products": "json"}
    for s in FILE_SOURCES:
        asset = LANDING_ASSETS[s]
        assert (asset.name, asset.uri) == (f"landing.{s}", f"file:///opt/drct/landing/{s}")
        [watcher] = asset.watchers
        assert watcher.name == f"watch_{s}"
        assert isinstance(watcher.trigger, FileArrivalTrigger)
        assert watcher.trigger.serialize()[1] == {
            "directory": f"/opt/drct/landing/{s}",
            "pattern": f"{s}_*.{patterns[s]}",
            "poll_interval": 5.0,
        }
