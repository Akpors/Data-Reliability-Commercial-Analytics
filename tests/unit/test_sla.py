from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

from drct.common.run_log import RunRecord
from drct.monitoring.sla import (
    DEFAULT_SLA_PATH,
    SlaRule,
    evaluate_duration,
    evaluate_lag,
    load_sla_rules,
)

MARTS_RULE = SlaRule(name="marts_daily_refresh", kind="lag", max_lag_days=1, grace_real_minutes=10)
FX_RULE = SlaRule(name="fx_ingest_duration", kind="duration", dag_id="ingest_fx", max_duration_s=30)

T0 = datetime(2026, 10, 16, 12, tzinfo=UTC)


def run(duration_s: int = 10, status: str = "success") -> RunRecord:
    return RunRecord(
        dag_id="ingest_fx",
        run_id="r1",
        run_type="scheduled",
        status=status,
        started_at=T0,
        ended_at=T0 + timedelta(seconds=duration_s),
    )


def test_lag_breached_after_grace():
    r = evaluate_lag(
        MARTS_RULE,
        date(2026, 7, 16),
        datetime(2026, 7, 19, 6, tzinfo=UTC),
        advanced_at := datetime(2026, 10, 16, 12, tzinfo=UTC),
        advanced_at + timedelta(minutes=11),
    )
    assert r.status == "breached"
    assert r.expected == "lag <= 1 day" and r.actual == "lag 3 days"


def test_lag_pending_within_grace():
    r = evaluate_lag(
        MARTS_RULE,
        date(2026, 7, 16),
        datetime(2026, 7, 19, 6, tzinfo=UTC),
        advanced_at := datetime(2026, 10, 16, 12, tzinfo=UTC),
        advanced_at + timedelta(minutes=2),
    )
    assert r.status == "pending"


def test_lag_met_when_within_max_lag():
    r = evaluate_lag(
        MARTS_RULE,
        date(2026, 7, 18),
        datetime(2026, 7, 19, 6, tzinfo=UTC),
        T0,
        T0 + timedelta(minutes=30),
    )
    assert r.status == "met" and r.actual == "lag 1 day"


def test_lag_no_data_without_marts():
    r = evaluate_lag(MARTS_RULE, None, datetime(2026, 7, 19, 6, tzinfo=UTC), T0, T0)
    assert r.status == "no_data"


def test_duration_breach():
    assert evaluate_duration(FX_RULE, run(duration_s=48)).status == "breached"
    assert evaluate_duration(FX_RULE, None).status == "no_data"


def test_duration_met_at_limit():
    assert evaluate_duration(FX_RULE, run(duration_s=30)).status == "met"


def test_failed_final_run_is_breached_even_if_fast():
    r = evaluate_duration(FX_RULE, run(duration_s=2, status="failed"))
    assert r.status == "breached" and "failed" in r.detail


def test_default_sla_yml_has_the_four_rules():
    rules = {r.name: r for r in load_sla_rules()}
    assert DEFAULT_SLA_PATH.name == "sla.yml"
    assert rules == {
        "marts_daily_refresh": SlaRule(
            "marts_daily_refresh", "lag", max_lag_days=1, grace_real_minutes=10
        ),
        "fx_ingest_duration": SlaRule(
            "fx_ingest_duration", "duration", dag_id="ingest_fx", max_duration_s=30
        ),
        "sales_ingest_duration": SlaRule(
            "sales_ingest_duration", "duration", dag_id="ingest_sales", max_duration_s=600
        ),
        "transform_duration": SlaRule(
            "transform_duration", "duration", dag_id="dbt_transform", max_duration_s=900
        ),
    }


@pytest.mark.parametrize(
    "body",
    [
        "- {name: x, kind: bogus}",
        "- {name: x, kind: lag, max_lag_days: 1}",  # missing grace
        "- {name: x, kind: duration, dag_id: d}",  # missing max_duration_s
        "- {name: x, kind: duration, max_duration_s: 5}",  # missing dag_id
        "- {kind: lag, max_lag_days: 1, grace_real_minutes: 1}",  # missing name
    ],
)
def test_load_sla_rules_validates(tmp_path: Path, body: str):
    path = tmp_path / "sla.yml"
    path.write_text(body)
    with pytest.raises(ValueError):
        load_sla_rules(path)
