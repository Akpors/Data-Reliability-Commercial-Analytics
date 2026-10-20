from datetime import UTC, date, datetime, timedelta

import pytest

from drct.common.run_log import RunRecord, record_run
from drct.common.sim_clock import init_clock
from drct.monitoring.sla import SlaRule, evaluate_all
from drct.monitoring.volume import compute_volume_metrics, detect_anomaly, evaluate_volume

DAY0 = date(2026, 4, 1)
SIM_NOW = datetime(2026, 7, 1, 6, tzinfo=UTC)


def _load(conn, table: str, counts: dict[date, int]) -> None:
    for d, n in counts.items():
        conn.execute(
            f"insert into raw.{table} (business_date) select %s from generate_series(1, %s)",
            (d, n),
        )


def _days(n: int, start: date = DAY0) -> list[date]:
    return [start + timedelta(days=i) for i in range(n)]


@pytest.mark.db
def test_compute_volume_metrics_counts_and_recomputes_old_days(db_conn):
    _load(db_conn, "sales", {DAY0: 5, DAY0 + timedelta(days=1): 7})
    _load(db_conn, "payments", {DAY0: 3})

    assert compute_volume_metrics(db_conn) == 3
    assert compute_volume_metrics(db_conn) == 3  # idempotent

    _load(db_conn, "sales", {DAY0: 2})  # a reprocess adds rows to an old day
    compute_volume_metrics(db_conn)
    rows = db_conn.execute(
        "select source, business_date, row_count from monitoring.volume_metrics order by 1, 2"
    ).fetchall()
    assert rows == [
        ("payments", DAY0, 3),
        ("sales", DAY0, 7),
        ("sales", DAY0 + timedelta(days=1), 7),
    ]


# Strong weekday pattern (Mon=0 .. Sun=6); Saturday runs ~1.3x the weekday level.
WEEKDAY_FACTOR = {0: 1.0, 1: 1.0, 2: 1.0, 3: 1.0, 4: 1.1, 5: 1.3, 6: 0.8}
SATURDAY = date(2026, 6, 27)  # ten weeks of daily history end on this Saturday


def _seasonal(base: int, end: date = SATURDAY, n: int = 70) -> dict[date, int]:
    days = [end - timedelta(days=n - 1 - i) for i in range(n)]
    return {d: round(base * WEEKDAY_FACTOR[d.weekday()]) + (i * 7) % 5 for i, d in enumerate(days)}


@pytest.mark.db
def test_evaluate_volume_flags_x8_saturday_but_not_a_normal_one(db_conn):
    assert SATURDAY.weekday() == 5
    sales = _seasonal(100)
    payments = _seasonal(50)
    sales[SATURDAY] = sales[SATURDAY] * 8
    _load(db_conn, "sales", sales)
    _load(db_conn, "payments", payments)
    compute_volume_metrics(db_conn)

    results = evaluate_volume(db_conn, SIM_NOW)

    by_source = {s: (d, r) for s, d, r in results}
    assert set(by_source) == {"sales", "payments"}
    assert by_source["sales"][0] == SATURDAY
    assert by_source["sales"][1].is_anomaly and by_source["sales"][1].reason == "spike"
    assert not by_source["payments"][1].is_anomaly  # a normal (1.3x) Saturday
    stored = db_conn.execute(
        "select source, business_date, row_count, is_anomaly, reason, sim_now "
        "from monitoring.volume_anomalies order by source"
    ).fetchall()
    assert stored == [
        ("payments", SATURDAY, payments[SATURDAY], False, "normal", SIM_NOW),
        ("sales", SATURDAY, sales[SATURDAY], True, "spike", SIM_NOW),
    ]


@pytest.mark.db
def test_pooled_window_would_have_flagged_the_normal_saturday(db_conn):
    """Guards the rationale for R23: the seasonality really does fool a mixed-weekday window."""
    counts = _seasonal(100)
    pooled = [counts[SATURDAY - timedelta(days=i)] for i in range(1, 15)]
    assert detect_anomaly(pooled, counts[SATURDAY]).is_anomaly


@pytest.mark.db
def test_evaluate_volume_uses_only_same_weekday_dates_within_window(db_conn):
    saturdays = [SATURDAY - timedelta(weeks=w) for w in range(8, -1, -1)]  # oldest first
    counts = {d: 130 + i % 3 for i, d in enumerate(saturdays)}
    counts[saturdays[0]] = 5000  # outlier: outside window=6, tolerated inside window=8
    for d in saturdays:  # a wildly different weekday between every pair of Saturdays
        counts[d - timedelta(days=3)] = 1
    _load(db_conn, "sales", counts)
    compute_volume_metrics(db_conn)

    (_, _, short), *_ = evaluate_volume(db_conn, SIM_NOW, window=6)
    assert short.reason == "insufficient_history"  # 6 prior Saturdays < min_history 7
    (_, _, full), *_ = evaluate_volume(db_conn, SIM_NOW)
    assert full.reason == "normal"  # 8 prior Saturdays; the lone outlier is robustly ignored
    assert full.baseline_median is not None and 130 <= full.baseline_median <= 132


@pytest.mark.db
def test_evaluate_volume_with_no_metrics_returns_nothing(db_conn):
    assert evaluate_volume(db_conn, SIM_NOW) == []


def _sla_clock(conn, advanced_ago: timedelta) -> datetime:
    init_clock(conn, DAY0, SIM_NOW)
    conn.execute(
        "update control.sim_clock set advanced_at = %s", (datetime.now(UTC) - advanced_ago,)
    )
    return datetime.now(UTC)


RULES = [
    SlaRule("marts_daily_refresh", "lag", max_lag_days=1, grace_real_minutes=10),
    SlaRule("fx_ingest_duration", "duration", dag_id="ingest_fx", max_duration_s=30),
]


@pytest.mark.db
def test_evaluate_all_without_marts_or_runs_is_no_data(db_conn):
    now_real = _sla_clock(db_conn, timedelta(hours=1))
    results = evaluate_all(db_conn, RULES, now_real)
    assert [(r.rule, r.status) for r in results] == [
        ("marts_daily_refresh", "no_data"),
        ("fx_ingest_duration", "no_data"),
    ]
    assert db_conn.execute("select count(*) from monitoring.sla_results").fetchone() == (2,)


@pytest.mark.db
def test_evaluate_all_breaches_and_records_sim_now(db_conn):
    now_real = _sla_clock(db_conn, timedelta(hours=1))
    db_conn.execute("create table marts.fct_sales (business_date date)")
    db_conn.execute("insert into marts.fct_sales values (%s)", (date(2026, 6, 27),))
    start = datetime(2026, 10, 16, 12, tzinfo=UTC)
    # Task-level rows are ignored; the latest DAG-level success/failed run decides.
    record_run(
        db_conn,
        RunRecord("ingest_fx", "r1", "scheduled", "success", start, start + timedelta(seconds=5)),
    )
    record_run(
        db_conn,
        RunRecord(
            "ingest_fx", "r2", "scheduled", "success",
            start + timedelta(hours=1), start + timedelta(hours=1, seconds=48),
        ),
    )  # fmt: skip
    record_run(
        db_conn,
        RunRecord(
            "ingest_fx", "r2", "scheduled", "failed",
            start + timedelta(hours=2), start + timedelta(hours=2, seconds=1), task_id="fetch",
        ),
    )  # fmt: skip
    record_run(
        db_conn,
        RunRecord("ingest_fx", "r3", "scheduled", "running", start + timedelta(hours=3), None),
    )

    results = {r.rule: r for r in evaluate_all(db_conn, RULES, now_real)}

    assert results["marts_daily_refresh"].status == "breached"
    assert results["marts_daily_refresh"].actual == "lag 4 days"
    assert results["fx_ingest_duration"].status == "breached"
    stored = db_conn.execute(
        "select rule, status, sim_now, evaluated_at from monitoring.sla_results order by rule"
    ).fetchall()
    assert [(r[0], r[1], r[2]) for r in stored] == [
        ("fx_ingest_duration", "breached", SIM_NOW),
        ("marts_daily_refresh", "breached", SIM_NOW),
    ]
    assert all(r[3] == now_real for r in stored)


@pytest.mark.db
def test_evaluate_all_lag_is_pending_within_grace_then_met(db_conn):
    now_real = _sla_clock(db_conn, timedelta(minutes=1))
    db_conn.execute("create table marts.fct_sales (business_date date)")
    db_conn.execute("insert into marts.fct_sales values (%s)", (date(2026, 6, 27),))
    assert evaluate_all(db_conn, RULES[:1], now_real)[0].status == "pending"

    db_conn.execute("insert into marts.fct_sales values (%s)", (date(2026, 6, 30),))
    now_real = _sla_clock(db_conn, timedelta(hours=1))
    assert evaluate_all(db_conn, RULES[:1], now_real)[0].status == "met"


@pytest.mark.db
def test_evaluate_volume_tolerates_an_ordinary_wobble_on_tight_history(db_conn):
    """Regression (Task 17 live run): a clean +12% Friday must not open a volume_anomaly."""
    fridays = [date(2026, 7, 24) - timedelta(weeks=w) for w in range(8, -1, -1)]  # oldest first
    assert all(d.weekday() == 4 for d in fridays)
    counts = dict(zip(fridays, [753, 773, 784, 870, 738, 795, 762, 769, 864], strict=True))
    _load(db_conn, "sales", counts)
    compute_volume_metrics(db_conn)

    ((_, _, result),) = evaluate_volume(db_conn, SIM_NOW)

    assert result.reason == "normal"
    assert result.mad == pytest.approx(0.05 * 771)
