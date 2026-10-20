import json
import shutil
from datetime import UTC, datetime
from pathlib import Path

import psycopg
import pytest

from drct.monitoring.dbt_artifacts import publish_dbt_artifacts, publish_freshness

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "dbt"
SIM_NOW = datetime(2026, 7, 23, 6, 0, tzinfo=UTC)


@pytest.fixture
def target_dir(tmp_path):
    for name in ("run_results.json", "manifest.json", "sources.json"):
        shutil.copy(FIXTURES / name, tmp_path / name)
    return tmp_path


@pytest.mark.db
def test_publish_inserts_every_test_result(db_conn, target_dir):
    run_results = json.loads((target_dir / "run_results.json").read_text())
    expected = sum(1 for r in run_results["results"] if r["unique_id"].startswith("test."))
    assert publish_dbt_artifacts(db_conn, target_dir, SIM_NOW) == expected
    rows = db_conn.execute(
        "select invocation_id, status, sim_now, generated_at, test_type, attached_node, failures "
        "from monitoring.test_results"
    ).fetchall()
    assert len(rows) == expected
    assert {r[0] for r in rows} == {run_results["metadata"]["invocation_id"]}
    assert {r[1] for r in rows} == {"pass", "fail", "warn", "skipped"}
    assert {r[2] for r in rows} == {SIM_NOW}
    assert {r[3] for r in rows} == {datetime(2026, 10, 20, 0, 38, 51, 884655, tzinfo=UTC)}
    failing = [r for r in rows if r[1] == "fail"]
    assert [(r[4], r[5], r[6]) for r in failing] == [("unique", "model.drct.stg_payments", 3)]


@pytest.mark.db
def test_publish_is_atomic(db_conn, target_dir):
    run_results = json.loads((target_dir / "run_results.json").read_text())
    # A result whose status cannot be stored as text forces a mid-insert failure.
    bad = next(r for r in run_results["results"] if r["status"] == "pass")
    bad["status"] = "pass\x00"
    (target_dir / "run_results.json").write_text(json.dumps(run_results))
    with pytest.raises(psycopg.Error):
        publish_dbt_artifacts(db_conn, target_dir, SIM_NOW)
    assert db_conn.execute("select count(*) from monitoring.test_results").fetchone() == (0,)


@pytest.mark.db
def test_publish_with_missing_files_returns_zero(db_conn, tmp_path):
    assert publish_dbt_artifacts(db_conn, tmp_path, SIM_NOW) == 0
    (tmp_path / "run_results.json").write_text((FIXTURES / "run_results.json").read_text())
    assert publish_dbt_artifacts(db_conn, tmp_path, SIM_NOW) == 0  # manifest still missing
    assert db_conn.execute("select count(*) from monitoring.test_results").fetchone() == (0,)


@pytest.mark.db
def test_publish_freshness_inserts_one_row_per_source(db_conn, target_dir):
    assert publish_freshness(db_conn, target_dir) == 5
    rows = db_conn.execute(
        "select source, status, age_seconds, snapshotted_at, checked_at is not null "
        "from monitoring.freshness order by source"
    ).fetchall()
    assert [r[0] for r in rows] == [
        "raw.customers",
        "raw.fx_rates",
        "raw.payments",
        "raw.products",
        "raw.sales",
    ]
    assert all(r[1] == "pass" and r[2] == 0 and r[3] == SIM_NOW and r[4] for r in rows)


@pytest.mark.db
def test_publish_freshness_with_missing_file_returns_zero(db_conn, tmp_path):
    assert publish_freshness(db_conn, tmp_path) == 0
    assert db_conn.execute("select count(*) from monitoring.freshness").fetchone() == (0,)


@pytest.mark.db
@pytest.mark.parametrize("half_written", ["run_results.json", "manifest.json"])
def test_publish_with_a_half_written_artifact_returns_zero(db_conn, target_dir, half_written):
    """Review I2 / ledger L159: a concurrent dbt rewriting an artifact must not fail publish."""
    path = target_dir / half_written
    path.write_text(path.read_text()[:200])
    assert publish_dbt_artifacts(db_conn, target_dir, SIM_NOW) == 0
    assert db_conn.execute("select count(*) from monitoring.test_results").fetchone() == (0,)


@pytest.mark.db
def test_publish_freshness_with_a_half_written_file_returns_zero(db_conn, target_dir):
    path = target_dir / "sources.json"
    path.write_text(path.read_text()[:200])
    assert publish_freshness(db_conn, target_dir) == 0
    assert db_conn.execute("select count(*) from monitoring.freshness").fetchone() == (0,)
