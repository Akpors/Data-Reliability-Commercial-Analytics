from drct.orchestration.monitoring_run import failed_tasks, read_manifest


def test_failed_and_upstream_failed_tasks_are_reported_in_order():
    states = {
        "source_freshness": "success",
        "publish_freshness": "failed",
        "volume": "upstream_failed",
        "sla": "success",
    }
    assert failed_tasks(states) == ["publish_freshness", "volume"]


def test_skipped_success_and_unfinished_tasks_are_not_failures():
    states = {"volume": "skipped", "sla": "success", "publish_freshness": None}
    assert failed_tasks(states) == []


def test_read_manifest_returns_parsed_json(tmp_path):
    path = tmp_path / "manifest.json"
    path.write_text('{"nodes": {}}')
    assert read_manifest(path) == {"nodes": {}}


def test_read_manifest_missing_file_is_none(tmp_path):
    assert read_manifest(tmp_path / "manifest.json") is None


def test_read_manifest_half_written_file_is_none(tmp_path):
    path = tmp_path / "manifest.json"
    path.write_text('{"nodes": {"model.x": {"depends_on"')  # dbt is mid-rewrite
    assert read_manifest(path) is None
