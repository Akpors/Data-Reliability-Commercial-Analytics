import json
import shutil
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from drct.monitoring.checks import Check, collect_checks
from drct.monitoring.dbt_artifacts import publish_dbt_artifacts, publish_freshness
from drct.monitoring.incidents import reconcile

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "dbt"
MANIFEST = json.loads((FIXTURES / "manifest.json").read_text())
SIM_NOW = datetime(2026, 7, 23, 6, 0, tzinfo=UTC)
LATER = SIM_NOW + timedelta(days=1)


@pytest.fixture
def published_fixture_artifacts(db_conn, tmp_path):
    for name in ("run_results.json", "manifest.json", "sources.json"):
        shutil.copy(FIXTURES / name, tmp_path / name)
    publish_dbt_artifacts(db_conn, tmp_path, SIM_NOW)
    publish_freshness(db_conn, tmp_path)
    return tmp_path


def _incidents(conn):
    return conn.execute(
        "select type, asset, severity, status, first_seen, last_seen, resolved_at, detail,"
        " affected_downstream from monitoring.incidents order by id"
    ).fetchall()


@pytest.mark.db
def test_reconcile_round_trip_with_blast_radius(db_conn):
    fail = Check("stale_data", "raw.payments", False, "critical", "age 48h")
    reconcile(db_conn, [fail], SIM_NOW, MANIFEST)
    row = db_conn.execute("select status, affected_downstream from monitoring.incidents").fetchone()
    assert row[0] == "open" and "exposure.drct.control_tower" in row[1]
    reconcile(db_conn, [replace(fail, passed=True)], SIM_NOW, MANIFEST)
    assert db_conn.execute("select status from monitoring.incidents").fetchone() == ("resolved",)


@pytest.mark.db
def test_reconcile_update_keeps_first_seen_and_never_downgrades_severity(db_conn):
    fail = Check("stale_data", "raw.payments", False, "critical", "age 48h")
    reconcile(db_conn, [fail], SIM_NOW, MANIFEST)
    changes = reconcile(
        db_conn, [replace(fail, severity="warning", detail="age 50h")], LATER, MANIFEST
    )
    assert len(changes.to_update) == 1
    [row] = _incidents(db_conn)
    assert row[2] == "critical" and row[3] == "open"
    assert row[4] == SIM_NOW and row[5] == LATER and row[7] == "age 50h"


@pytest.mark.db
def test_reconcile_escalates_warning_to_critical(db_conn):
    warn = Check("stale_data", "raw.payments", False, "warning", "age 21h")
    reconcile(db_conn, [warn], SIM_NOW, MANIFEST)
    reconcile(db_conn, [replace(warn, severity="critical")], LATER, MANIFEST)
    assert _incidents(db_conn)[0][2] == "critical"


@pytest.mark.db
def test_reconcile_resolve_stamps_resolved_at_and_reopen_makes_new_incident(db_conn):
    fail = Check("pipeline_failure", "ingest_files", False, "critical", "failed")
    reconcile(db_conn, [fail], SIM_NOW, MANIFEST)
    reconcile(db_conn, [replace(fail, passed=True)], LATER, MANIFEST)
    assert _incidents(db_conn)[0][6] == LATER
    reconcile(db_conn, [fail], LATER, MANIFEST)
    assert [r[3] for r in _incidents(db_conn)] == ["resolved", "open"]


@pytest.mark.db
def test_reconcile_survives_missing_manifest_and_unknown_node(db_conn):
    reconcile(db_conn, [Check("sla_breach", "marts_lag", False, "critical", "late")], SIM_NOW, None)
    reconcile(
        db_conn,
        [Check("pipeline_failure", "no_such_dag", False, "critical", "x")],
        SIM_NOW,
        MANIFEST,
    )
    assert [r[8] for r in _incidents(db_conn)] == [[], []]


@pytest.mark.db
def test_reconcile_uses_lineage_node_for_blast_radius(db_conn):
    check = Check(
        "duplicate_records",
        "test.drct.some_test",
        False,
        "critical",
        "dupes",
        "model.drct.stg_payments",
    )
    reconcile(db_conn, [check], SIM_NOW, MANIFEST)
    assert "model.drct.fct_payments" in _incidents(db_conn)[0][8]


@pytest.mark.db
def test_reconcile_is_atomic(db_conn):
    good = Check("stale_data", "raw.sales", False, "critical", "ok")
    bad = Check("stale_data", "raw.bad\x00", False, "critical", "nul byte")
    with pytest.raises(Exception, match="NUL|nul|0x00"):
        reconcile(db_conn, [good, bad], SIM_NOW, MANIFEST)
    assert _incidents(db_conn) == []


@pytest.mark.db
def test_collect_checks_maps_skipped_test_to_no_signal(db_conn, published_fixture_artifacts):
    by_asset = {c.asset: c for c in collect_checks(db_conn)}
    assert any(c.passed is None for c in by_asset.values())


@pytest.mark.db
def test_collect_checks_from_dbt_artifacts(db_conn, published_fixture_artifacts):
    checks = collect_checks(db_conn)
    tests = [c for c in checks if c.asset.startswith("test.")]
    failing = [c for c in tests if c.passed is False]
    assert sorted((c.type, c.severity, c.lineage_node) for c in failing) == [
        ("duplicate_records", "critical", "model.drct.stg_payments"),
        ("out_of_range", "warning", "model.drct.stg_sales"),
    ]
    assert sum(c.passed is None for c in tests) == 2  # the two skipped fct_payments tests
    fresh = {c.asset: c for c in checks if c.type == "stale_data"}
    assert set(fresh) == {
        "raw.customers",
        "raw.fx_rates",
        "raw.payments",
        "raw.products",
        "raw.sales",
    }
    assert all(c.passed is True for c in fresh.values())
    assert fresh["raw.sales"].lineage_node == "source.drct.raw.sales"


@pytest.mark.db
def test_collect_checks_only_uses_latest_invocation(db_conn, published_fixture_artifacts):
    db_conn.execute(
        "insert into monitoring.test_results (invocation_id, unique_id, test_name, test_type,"
        " status, severity, failures, generated_at) values"
        " ('newer', 'test.drct.only_new', 'only_new', 'not_null', 'pass', 'error', 0,"
        " now() + interval '1 day')"
    )
    tests = [c for c in collect_checks(db_conn) if c.asset.startswith("test.")]
    assert [(c.asset, c.passed) for c in tests] == [("test.drct.only_new", True)]


@pytest.mark.db
def test_collect_checks_freshness_statuses_and_latest_row(db_conn):
    db_conn.execute(
        "insert into monitoring.freshness (source, age_seconds, status, checked_at) values"
        " ('raw.sales', 0, 'pass', now() - interval '2 hour'),"
        " ('raw.sales', 90000, 'warn', now() - interval '1 hour'),"
        " ('raw.payments', 200000, 'error', now()),"
        " ('raw.products', null, 'runtime error', now())"
    )
    by_asset = {c.asset: c for c in collect_checks(db_conn)}
    assert (by_asset["raw.sales"].passed, by_asset["raw.sales"].severity) == (False, "warning")
    assert (by_asset["raw.payments"].passed, by_asset["raw.payments"].severity) == (
        False,
        "critical",
    )
    assert by_asset["raw.products"].passed is None


@pytest.mark.db
def test_collect_checks_test_statuses_and_severity(db_conn):
    db_conn.execute(
        "insert into monitoring.test_results (invocation_id, unique_id, test_name, test_type,"
        " attached_node, status, severity, failures, generated_at) values"
        " ('i', 'test.drct.a', 'a', 'unique', 'model.drct.m', 'fail', 'error', 3, now()),"
        " ('i', 'test.drct.b', 'b', 'singular', 'model.drct.m', 'warn', 'warn', 2, now()),"
        " ('i', 'test.drct.c', 'c', 'accepted_values', 'model.drct.m', 'fail', 'warn', 1, now()),"
        " ('i', 'test.drct.d', 'd', 'relationships', 'model.drct.m', 'error', 'error', null, now()),"
        " ('i', 'test.drct.e', 'e', 'not_null', 'model.drct.m', 'skipped', 'error', null, now())"
    )
    by_asset = {c.asset: c for c in collect_checks(db_conn)}
    got = {
        k.removeprefix("test.drct."): (c.type, c.passed, c.severity) for k, c in by_asset.items()
    }
    assert got == {
        "a": ("duplicate_records", False, "critical"),
        "b": ("missing_dates", False, "warning"),
        "c": ("test_failure", False, "warning"),
        "d": ("broken_reference", False, "critical"),
        "e": ("missing_values", None, "critical"),
    }
    assert "3 failing rows" in by_asset["test.drct.a"].detail
    assert by_asset["test.drct.a"].lineage_node == "model.drct.m"


@pytest.mark.db
def test_collect_checks_volume_schema_sla_and_pipeline(db_conn):
    db_conn.execute(
        "insert into monitoring.volume_anomalies (source, business_date, row_count, robust_z,"
        " baseline_median, is_anomaly, reason, evaluated_at) values"
        " ('sales', '2026-07-22', 100, 1, 90, false, 'normal', now() - interval '1 hour'),"
        " ('sales', '2026-07-22', 900, 20, 90, true, 'spike', now()),"
        " ('payments', '2026-07-22', 50, null, null, false, 'insufficient_history', now())"
    )
    db_conn.execute(
        "insert into monitoring.schema_events (source, file_name, added, missing, is_breaking,"
        " recorded_at) values"
        " ('sales', 'old.csv', '{}', '{}', true, now() - interval '1 hour'),"
        " ('sales', 'sales_2026-07-22.csv', '{promo_code}', '{}', false, now()),"
        " ('payments', 'p.parquet', '{}', '{amount}', true, now()),"
        " ('customers', 'c.json', '{}', '{}', false, now())"
    )
    db_conn.execute(
        "insert into monitoring.sla_results (rule, status, detail, evaluated_at) values"
        " ('r_met', 'met', 'ok', now()), ('r_bad', 'breached', 'late', now()),"
        " ('r_old', 'breached', 'x', now() - interval '1 hour'), ('r_old', 'met', 'y', now()),"
        " ('r_pend', 'pending', 'p', now()), ('r_nd', 'no_data', 'n', now())"
    )
    db_conn.execute(
        "insert into monitoring.pipeline_runs (dag_id, run_id, task_id, status, started_at) values"
        " ('ingest_files', 'r1', null, 'failed', now() - interval '3 hour'),"
        " ('ingest_files', 'r2', null, 'success', now() - interval '2 hour'),"
        " ('ingest_files', 'r3', null, 'running', now() - interval '1 hour'),"
        " ('ingest_files', 'r2', 'load', 'failed', now()),"
        " ('dbt_transform', 'r1', null, 'failed', now())"
    )
    got = {(c.type, c.asset): (c.passed, c.severity) for c in collect_checks(db_conn)}
    assert got == {
        ("volume_anomaly", "raw.sales"): (False, "warning"),
        ("volume_anomaly", "raw.payments"): (None, "warning"),
        ("schema_change", "raw.sales"): (True, "critical"),
        ("schema_drift", "raw.sales"): (False, "warning"),
        ("schema_change", "raw.payments"): (False, "critical"),
        ("schema_drift", "raw.payments"): (None, "warning"),
        ("schema_change", "raw.customers"): (True, "critical"),
        ("schema_drift", "raw.customers"): (True, "warning"),
        ("sla_breach", "r_met"): (True, "critical"),
        ("sla_breach", "r_bad"): (False, "critical"),
        ("sla_breach", "r_old"): (True, "critical"),
        ("sla_breach", "r_pend"): (None, "critical"),
        ("sla_breach", "r_nd"): (None, "critical"),
        ("pipeline_failure", "ingest_files"): (True, "critical"),
        ("pipeline_failure", "dbt_transform"): (False, "critical"),
    }


@pytest.mark.db
def test_collect_checks_on_empty_warehouse(db_conn):
    assert collect_checks(db_conn) == []


@pytest.mark.db
def test_publishing_the_same_artifacts_twice_does_not_poison_reconcile(
    db_conn, published_fixture_artifacts
):
    publish_dbt_artifacts(db_conn, published_fixture_artifacts, SIM_NOW)  # second publish
    checks = collect_checks(db_conn)
    identities = [(c.type, c.asset) for c in checks]
    assert len(identities) == len(set(identities))
    for when in (SIM_NOW, LATER):
        reconcile(db_conn, collect_checks(db_conn), when, MANIFEST)
    open_rows = db_conn.execute(
        "select type, asset from monitoring.incidents where status = 'open'"
    ).fetchall()
    assert len(open_rows) == len(set(open_rows)) == 2  # failing unique + warning range test
