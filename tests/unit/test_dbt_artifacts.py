import json
from pathlib import Path

from drct.monitoring.dbt_artifacts import parse_run_results, parse_sources

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "dbt"
RUN_RESULTS = json.loads((FIXTURES / "run_results.json").read_text())
MANIFEST = json.loads((FIXTURES / "manifest.json").read_text())
SOURCES = json.loads((FIXTURES / "sources.json").read_text())


def _by_id():
    return {r.unique_id: r for r in parse_run_results(RUN_RESULTS, MANIFEST)}


def test_parse_run_results_maps_type_status_and_node():
    results = parse_run_results(RUN_RESULTS, MANIFEST)
    failed = next(r for r in results if r.test_type == "unique" and r.status == "fail")
    assert failed.attached_node == "model.drct.stg_payments"
    assert failed.failures > 0 and failed.compiled_sql


def test_skipped_tests_are_kept_as_skipped():
    assert "skipped" in {r.status for r in parse_run_results(RUN_RESULTS, MANIFEST)}


def test_only_tests_are_returned():
    results = parse_run_results(RUN_RESULTS, MANIFEST)
    assert results and all(r.unique_id.startswith("test.") for r in results)


def test_warn_status_and_lowercased_severity():
    warn = next(r for r in parse_run_results(RUN_RESULTS, MANIFEST) if r.status == "warn")
    assert warn.test_type == "expect_column_values_to_be_between"
    assert warn.severity == "warn" and warn.attached_node == "model.drct.stg_sales"
    assert warn.failures == 1


def test_singular_test_type_when_no_test_metadata():
    singular = _by_id()["test.drct.assert_no_missing_sales_dates"]
    assert singular.test_type == "singular" and singular.severity == "warn"
    assert singular.test_name == "assert_no_missing_sales_dates"


def test_attached_node_is_the_tested_model_for_relationships_tests():
    # dbt lists the referenced model first in depends_on.nodes; the tested model is attached_node.
    rel = next(
        r for r in parse_run_results(RUN_RESULTS, MANIFEST) if r.test_type == "relationships"
    )
    assert rel.attached_node == "model.drct.stg_sales"


def test_parse_sources_uses_sim_snapshot():
    r = {f.source: f for f in parse_sources(SOURCES)}["raw.sales"]
    assert r.status == "pass" and r.age_seconds >= 0
    assert r.snapshotted_at.isoformat() == "2026-07-23T06:00:00+00:00"
    assert r.max_loaded_at is not None


def test_parse_sources_keeps_runtime_error_without_timestamps():
    sources = {
        "results": [
            {"unique_id": "source.drct.raw.sales", "status": "runtime error", "error": "boom"}
        ]
    }
    (r,) = parse_sources(sources)
    assert r.source == "raw.sales" and r.status == "runtime error"
    assert r.max_loaded_at is None and r.snapshotted_at is None and r.age_seconds is None
