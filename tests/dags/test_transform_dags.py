from airflow.sdk.definitions.timetables.assets import AssetTriggeredTimetable


def test_publish_runs_even_when_dbt_fails(dagbag):
    dag = dagbag.get_dag("dbt_transform")
    assert dag.get_task("publish_dbt_artifacts").trigger_rule == "all_done"
    assert dag.get_task("publish_dbt_artifacts").outlets[0].name == "monitoring.dbt_results"
    assert dag.get_task("mark_marts").trigger_rule == "all_success"


def test_marts_and_publish_both_follow_dbt_build(dagbag):
    dag = dagbag.get_dag("dbt_transform")
    build = dag.get_task("dbt_build")
    assert {t.task_id for t in build.downstream_list} == {"mark_marts", "publish_dbt_artifacts"}
    assert [a.name for a in dag.get_task("mark_marts").outlets] == ["marts"]


def test_dbt_build_command_and_stale_artifact_guard(dagbag):
    command = dagbag.get_dag("dbt_transform").get_task("dbt_build").bash_command
    assert (
        "$DBT_BIN build --project-dir /opt/drct/dbt --profiles-dir /opt/drct/dbt"
        " --target-path target" in command
    )
    # Stale artifacts are deleted first of all (before deps and build), so a failure in either
    # cannot let publish re-insert the previous run's results.
    rm = command.index("rm -f")
    assert rm < command.index("deps") and rm < command.index("$DBT_BIN build")
    assert "target/run_results.json" in command
    # The manifest is kept: the incident engine reads it concurrently for the blast radius, and
    # publish already refuses a stale run because run_results.json is gone (review I2).
    assert "manifest.json" not in command
    assert "dbt_packages" in command and "deps" in command


def test_dbt_transform_waits_for_raw_assets_and_runs_one_at_a_time(dagbag):
    dag = dagbag.get_dag("dbt_transform")
    assert dag.max_active_runs == 1
    assert isinstance(dag.timetable, AssetTriggeredTimetable)
    # (sales AND payments) OR customers OR products OR fx_rates
    any_of = dag.timetable.asset_condition
    assert type(any_of).__name__ == "AssetAny"
    both = next(o for o in any_of.objects if type(o).__name__ == "AssetAll")
    assert {a.name for a in both.objects} == {"raw.sales", "raw.payments"}
    alone = {o.name for o in any_of.objects if type(o).__name__ == "Asset"}
    assert alone == {"raw.customers", "raw.products", "raw.fx_rates"}
    assert dag.on_success_callback and dag.on_failure_callback


def test_reprocess_emits_all_file_raw_assets(dagbag):
    names = {a.name for a in dagbag.get_dag("reprocess").get_task("reload").outlets}
    assert names == {"raw.sales", "raw.payments", "raw.customers", "raw.products"}


def test_reprocess_is_manual_with_date_params_and_callbacks(dagbag):
    dag = dagbag.get_dag("reprocess")
    assert type(dag.timetable).__name__ == "NullTimetable"  # schedule=None: manual only
    assert set(dag.params) == {"start", "end"}
    assert dag.on_success_callback and dag.on_failure_callback


def test_reprocess_runs_one_at_a_time(dagbag):
    """A retried trigger (timeout after the run was created) can queue a second reprocess run; two
    overlapping delete-then-insert reloads of the same date could duplicate rows."""
    assert dagbag.get_dag("reprocess").max_active_runs == 1
