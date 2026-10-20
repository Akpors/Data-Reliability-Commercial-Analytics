"""The Control Tower's populated rendering: failed-test counts and the single-run quality chart."""

import json
import uuid
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import pytest

from drct.control_tower import queries
from drct.control_tower.queries import HealthSummary

APP = str(Path(__file__).resolve().parents[2] / "src" / "drct" / "control_tower" / "app.py")
RUN_AT = datetime(2026, 10, 20, 8, 0, tzinfo=UTC)


def _empty(*columns: str) -> pd.DataFrame:
    return pd.DataFrame(columns=list(columns))


def _failed_tests() -> pd.DataFrame:
    """One blocking failure and the two tests it skipped downstream (as `dbt build` reports)."""
    rows = [
        ("unique_stg_payments_payment_id", "model.drct.stg_payments", "fail", 18, None, "select 1"),
        ("not_null_fct_payments_id", "model.drct.fct_payments", "skipped", None, None, None),
        ("unique_fct_payments_id", "model.drct.fct_payments", "skipped", None, None, None),
    ]
    columns = ["test_name", "attached_node", "status", "failures", "message", "compiled_sql"]
    return pd.DataFrame(rows, columns=columns)


@pytest.fixture
def populated(monkeypatch):
    """Every query answers as a seeded stack with one failing and two skipped tests would."""
    # A fresh DSN per test, so st.cache_data never serves another test's data.
    monkeypatch.setenv("CONTROL_TOWER_DSN", f"postgresql://unused/{uuid.uuid4().hex}")
    monkeypatch.setattr(queries, "connect", lambda dsn: _NullConnection())
    answers = {
        "health_summary": HealthSummary("critical", 1, 0, RUN_AT),
        "freshness_status": pd.DataFrame(
            [("raw.sales", 2.0, "pass", 20, 44)],
            columns=["source", "age_hours", "status", "warn_hours", "error_hours"],
        ),
        "quality_trend": pd.DataFrame(
            [(RUN_AT, 0.95, 1, 0)], columns=["generated_at", "pass_rate", "failed", "warned"]
        ),
        "volume_series": _empty("source", "business_date", "row_count", "is_anomaly"),
        "failed_tests": _failed_tests(),
        "sla_timeline": _empty("rule", "evaluated_at", "status", "sim_date", "latest_status"),
        "incidents": _empty(
            "id",
            "type",
            "asset",
            "severity",
            "status",
            "first_seen",
            "last_seen",
            "resolved_at",
            "detail",
            "affected_downstream",
        ),
    }
    for name, answer in answers.items():
        monkeypatch.setattr(queries, name, lambda conn, answer=answer: answer)


class _NullConnection:
    def __enter__(self):
        return self

    def __exit__(self, *exc) -> None:
        pass


def _run():
    from streamlit.testing.v1 import AppTest

    at = AppTest.from_file(APP).run()
    assert not at.exception
    return at


def _html(at) -> str:
    return "\n".join(m.value for m in at.markdown)


def test_failed_tests_chip_counts_only_failures_like_the_kpi_tile(populated):
    """Review I4: the chip said "3 not passing" next to the KPI tile's "1 not passing"."""
    html = _html(_run())
    assert "latest dbt run · 1 not passing" in html  # KPI tile
    assert "1 not passing · 2 skipped" in html  # Failed-tests chip
    assert "3 not passing" not in html


def test_skipped_tests_are_shown_apart_as_blocked_upstream(populated):
    html = _html(_run())
    assert html.count("skipped (blocked upstream)") == 2
    assert html.count('class="ct-row skipped"') == 2
    # Failures are listed before the tests they blocked.
    assert html.index("unique_stg_payments_payment_id") < html.index("not_null_fct_payments_id")


def test_quality_chart_shows_a_single_dbt_run(populated):
    """Review M1: a lines-only trace draws nothing for one point."""
    at = _run()
    specs = [json.loads(chart.proto.spec) for chart in at.get("plotly_chart")]
    quality = next(s for s in specs if "passing" in s["data"][0].get("hovertemplate", ""))
    assert "markers" in quality["data"][0]["mode"]
