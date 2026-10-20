"""Control Tower panel queries, run as the read-only ``control_tower_ro`` role the app uses."""

from contextlib import contextmanager
from datetime import UTC, date, datetime, timedelta

import pytest

from drct.control_tower.queries import (
    failed_tests,
    freshness_status,
    health_summary,
    incidents,
    quality_trend,
    sla_timeline,
    volume_series,
)

SIM_NOW = datetime(2026, 8, 3, 6, 0, tzinfo=UTC)
REAL = datetime(2026, 10, 20, 7, 0, tzinfo=UTC)


@contextmanager
def _as_owner(conn):
    """Seed rows as the warehouse owner, then drop back to the read-only role."""
    conn.execute("reset role")
    try:
        yield conn
    finally:
        conn.execute("set role control_tower_ro")


@pytest.fixture(autouse=True)
def read_only(db_conn):
    """Every query in this module runs with only the grants the Control Tower has."""
    db_conn.execute("set role control_tower_ro")
    return db_conn


def _insert_incident(conn, type_, asset, severity, status="open", downstream=()):
    conn.execute(
        "insert into monitoring.incidents (type, asset, severity, status, first_seen, last_seen,"
        " resolved_at, detail, affected_downstream) values (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
        (
            type_,
            asset,
            severity,
            status,
            SIM_NOW,
            SIM_NOW,
            SIM_NOW if status == "resolved" else None,
            f"{type_} on {asset}",
            list(downstream),
        ),
    )


def _insert_test(conn, invocation, unique_id, status, generated_at, test_type="unique"):
    conn.execute(
        "insert into monitoring.test_results (invocation_id, unique_id, test_name, test_type,"
        " attached_node, status, severity, failures, message, compiled_sql, sim_now, generated_at)"
        " values (%s, %s, %s, %s, 'model.drct.stg_sales', %s, 'ERROR', %s, %s, %s, %s, %s)",
        (
            invocation,
            unique_id,
            unique_id.split(".")[-1],
            test_type,
            status,
            0 if status == "pass" else 3,
            None if status == "pass" else "Got 3 results, configured to fail if != 0",
            "select 1",
            SIM_NOW,
            generated_at,
        ),
    )


@pytest.fixture
def seeded_incidents(db_conn):
    with _as_owner(db_conn):
        _insert_incident(
            db_conn,
            "duplicate_records",
            "test.drct.unique_stg_sales_sale_line_id",
            "critical",
            downstream=("model.drct.fct_payments", "exposure.drct.control_tower"),
        )
        _insert_incident(db_conn, "volume_anomaly", "raw.sales", "warning")
        _insert_incident(db_conn, "stale_data", "raw.payments", "critical", status="resolved")


@pytest.fixture
def two_invocations_one_failure(db_conn):
    tests = [f"test.drct.t{i}" for i in range(4)]
    with _as_owner(db_conn):
        for uid in tests:
            _insert_test(db_conn, "inv-1", uid, "pass", REAL)
        for uid in tests:
            _insert_test(
                db_conn,
                "inv-2",
                uid,
                "fail" if uid.endswith("t0") else "pass",
                REAL + timedelta(minutes=10),
            )
        # The same artifacts published twice must not count twice.
        _insert_test(db_conn, "inv-2", tests[0], "fail", REAL + timedelta(minutes=10))


@pytest.mark.db
def test_empty_install_reports_no_data(db_conn):  # Review Focus 4
    assert health_summary(db_conn).status == "no_data"
    for fn in (
        freshness_status,
        quality_trend,
        volume_series,
        failed_tests,
        sla_timeline,
        incidents,
    ):
        assert fn(db_conn).empty


@pytest.mark.db
def test_empty_install_has_no_sim_clock(db_conn):
    summary = health_summary(db_conn)
    assert summary.sim_now is None
    assert (summary.open_critical, summary.open_warning) == (0, 0)


@pytest.mark.db
def test_open_critical_incident_makes_status_critical(db_conn, seeded_incidents):
    s = health_summary(db_conn)
    assert s.status == "critical" and s.open_critical == 1


@pytest.mark.db
def test_only_open_warnings_make_status_degraded(db_conn, seeded_incidents):
    with _as_owner(db_conn):
        db_conn.execute(
            "update monitoring.incidents set status = 'resolved' where severity = 'critical'"
        )
    s = health_summary(db_conn)
    assert (s.status, s.open_critical, s.open_warning) == ("degraded", 0, 1)


@pytest.mark.db
def test_health_reads_sim_clock_through_security_definer(db_conn, seeded_incidents):
    with _as_owner(db_conn):
        db_conn.execute(
            "insert into control.sim_clock (id, day0, sim_now, advanced_at) values (1, %s, %s, now())",
            (date(2026, 5, 5), SIM_NOW),
        )
    assert health_summary(db_conn).sim_now == SIM_NOW


@pytest.mark.db
def test_quality_trend_pass_rate(db_conn, two_invocations_one_failure):
    assert list(quality_trend(db_conn)["pass_rate"]) == [1.0, 0.75]


@pytest.mark.db
def test_failed_tests_lists_latest_invocation_failures_once(db_conn, two_invocations_one_failure):
    frame = failed_tests(db_conn)
    assert list(frame["test_name"]) == ["t0"]
    assert list(frame.columns) == [
        "test_name",
        "attached_node",
        "status",
        "failures",
        "message",
        "compiled_sql",
    ]


@pytest.mark.db
def test_freshness_takes_latest_row_per_source(db_conn):
    with _as_owner(db_conn):
        for source, age_h, status, checked in [
            ("raw.sales", 48, "error", REAL),
            ("raw.sales", 2, "pass", REAL + timedelta(minutes=5)),
            ("raw.payments", 22, "warn", REAL),
        ]:
            db_conn.execute(
                "insert into monitoring.freshness (source, age_seconds, status, checked_at)"
                " values (%s, %s, %s, %s)",
                (source, age_h * 3600, status, checked),
            )
    frame = freshness_status(db_conn).set_index("source")
    assert frame.loc["raw.sales", "status"] == "pass"
    assert frame.loc["raw.sales", "age_hours"] == 2
    assert frame.loc["raw.payments", "status"] == "warn"
    assert set(frame["warn_hours"]) == {20} and set(frame["error_hours"]) == {44}


@pytest.mark.db
def test_volume_series_flags_dates_by_latest_evaluation(db_conn):
    start = date(2026, 7, 1)
    with _as_owner(db_conn):
        for i in range(40):
            db_conn.execute(
                "insert into monitoring.volume_metrics (source, business_date, row_count)"
                " values ('sales', %s, %s)",
                (start + timedelta(days=i), 900),
            )
        last = start + timedelta(days=39)
        for is_anomaly, evaluated in [(False, REAL), (True, REAL + timedelta(minutes=1))]:
            db_conn.execute(
                "insert into monitoring.volume_anomalies (source, business_date, row_count,"
                " is_anomaly, evaluated_at) values ('sales', %s, 900, %s, %s)",
                (last, is_anomaly, evaluated),
            )
    frame = volume_series(db_conn, days=30)
    assert len(frame) == 30 and frame["business_date"].max() == last
    assert list(frame.loc[frame["is_anomaly"], "business_date"]) == [last]


@pytest.mark.db
def test_sla_timeline_marks_a_day_breached_if_any_evaluation_breached(db_conn):
    with _as_owner(db_conn):
        for status, minutes in [("breached", 0), ("met", 5)]:
            db_conn.execute(
                "insert into monitoring.sla_results (rule, status, sim_now, evaluated_at)"
                " values ('fx_ingest_duration', %s, %s, %s)",
                (status, SIM_NOW, REAL + timedelta(minutes=minutes)),
            )
    frame = sla_timeline(db_conn)
    assert list(frame["status"]) == ["breached"]
    assert list(frame["latest_status"]) == ["met"]
    assert list(frame.columns[:3]) == ["rule", "evaluated_at", "status"]


@pytest.mark.db
def test_incidents_filter_by_status_and_list_open_critical_first(db_conn, seeded_incidents):
    assert list(incidents(db_conn, status="open")["type"]) == [
        "duplicate_records",
        "volume_anomaly",
    ]
    everything = incidents(db_conn)
    assert len(everything) == 3
    assert everything.loc[0, "affected_downstream"] == [
        "model.drct.fct_payments",
        "exposure.drct.control_tower",
    ]
