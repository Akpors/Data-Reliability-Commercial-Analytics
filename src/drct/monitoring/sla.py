"""SLA rules (YAML) and their evaluators: marts freshness lag and DAG run duration.

``evaluate_all`` reads marts, the sim clock and ``monitoring.pipeline_runs`` and appends one
``monitoring.sla_results`` row per rule. Nothing here reads ``control.chaos_state``.
"""

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Literal

import psycopg
import yaml

from drct.common.logging import get_logger
from drct.common.run_log import RunRecord
from drct.common.sim_clock import read_clock

__all__ = [
    "DEFAULT_SLA_PATH",
    "SlaResult",
    "SlaRule",
    "evaluate_all",
    "evaluate_duration",
    "evaluate_lag",
    "load_sla_rules",
]

log = get_logger("drct.monitoring.sla")

DEFAULT_SLA_PATH = Path(__file__).resolve().parent / "sla.yml"

SlaStatus = Literal["met", "breached", "pending", "no_data"]


@dataclass(frozen=True)
class SlaRule:
    name: str
    kind: Literal["lag", "duration"]
    dag_id: str | None = None
    max_lag_days: int | None = None
    grace_real_minutes: int | None = None
    max_duration_s: int | None = None


@dataclass(frozen=True)
class SlaResult:
    rule: str
    status: SlaStatus
    expected: str
    actual: str
    detail: str


_REQUIRED = {
    "lag": ("max_lag_days", "grace_real_minutes"),
    "duration": ("dag_id", "max_duration_s"),
}
_FIELDS = ("name", "kind", "dag_id", "max_lag_days", "grace_real_minutes", "max_duration_s")


def load_sla_rules(path: Path = DEFAULT_SLA_PATH) -> list[SlaRule]:
    """Parse and validate the rules file; raises ``ValueError`` on any malformed rule."""
    raw = yaml.safe_load(path.read_text())
    if not isinstance(raw, list):
        raise ValueError(f"{path}: expected a list of rules")  # noqa: TRY004
    rules = []
    for i, item in enumerate(raw):
        if not isinstance(item, dict):
            raise ValueError(f"{path}: rule #{i} is not a mapping")  # noqa: TRY004
        unknown = set(item) - set(_FIELDS)
        if unknown:
            raise ValueError(f"{path}: rule #{i} has unknown fields {sorted(unknown)}")
        if not item.get("name"):
            raise ValueError(f"{path}: rule #{i} has no name")
        kind = item.get("kind")
        if kind not in _REQUIRED:
            raise ValueError(f"{path}: rule {item['name']!r} has invalid kind {kind!r}")
        missing = [f for f in _REQUIRED[kind] if item.get(f) is None]
        if missing:
            raise ValueError(f"{path}: rule {item['name']!r} ({kind}) is missing {missing}")
        rules.append(SlaRule(**item))
    return rules


def _days(n: int) -> str:
    return f"{n} day{'' if n == 1 else 's'}"


def evaluate_lag(
    rule: SlaRule,
    marts_max_date: date | None,
    sim_now: datetime,
    clock_advanced_at: datetime,
    now_real: datetime,
) -> SlaResult:
    """Marts must be within ``max_lag_days`` of the sim date, once the real-time grace is over."""
    expected = f"lag <= {_days(rule.max_lag_days)}"
    if marts_max_date is None:
        return SlaResult(rule.name, "no_data", expected, "no marts data", "marts has no rows yet")
    lag = (sim_now.date() - marts_max_date).days
    actual = f"lag {_days(lag)}"
    grace = timedelta(minutes=rule.grace_real_minutes)
    if now_real - clock_advanced_at < grace:
        return SlaResult(
            rule.name,
            "pending",
            expected,
            actual,
            f"within {rule.grace_real_minutes} min grace after the clock advanced",
        )
    detail = f"marts max business_date {marts_max_date}, sim date {sim_now.date()}"
    return SlaResult(
        rule.name, "breached" if lag > rule.max_lag_days else "met", expected, actual, detail
    )


def evaluate_duration(rule: SlaRule, latest: RunRecord | None) -> SlaResult:
    """The latest finished DAG-level run must succeed within ``max_duration_s``."""
    expected = f"duration <= {rule.max_duration_s}s"
    if latest is None or latest.status not in ("success", "failed"):
        return SlaResult(
            rule.name, "no_data", expected, "no finished run", f"no run of {rule.dag_id}"
        )
    if latest.ended_at is None:
        return SlaResult(rule.name, "no_data", expected, "run has no end time", latest.run_id)
    duration = (latest.ended_at - latest.started_at).total_seconds()
    actual = f"{duration:g}s"
    if latest.status == "failed":
        return SlaResult(
            rule.name, "breached", expected, actual, f"latest run {latest.run_id} failed"
        )
    status = "breached" if duration > rule.max_duration_s else "met"
    return SlaResult(rule.name, status, expected, actual, f"latest run {latest.run_id}")


def _marts_max_date(conn: psycopg.Connection) -> date | None:
    # to_regclass avoids an aborted transaction when the marts table hasn't been built yet.
    if conn.execute("select to_regclass('marts.fct_sales')").fetchone()[0] is None:
        return None
    return conn.execute("select max(business_date) from marts.fct_sales").fetchone()[0]


def _latest_run(conn: psycopg.Connection, dag_id: str) -> RunRecord | None:
    row = conn.execute(
        """
        select dag_id, run_id, run_type, status, started_at, ended_at, try_number,
               business_date, rows_in, rows_rejected, sim_now
          from monitoring.pipeline_runs
         where dag_id = %s and task_id is null and status in ('success', 'failed')
         order by started_at desc
         limit 1
        """,
        (dag_id,),
    ).fetchone()
    if row is None:
        return None
    (dag, run_id, run_type, status, started, ended, tries, bdate, rows_in, rejected, sim) = row
    return RunRecord(
        dag_id=dag,
        run_id=run_id,
        run_type=run_type,
        status=status,
        started_at=started,
        ended_at=ended,
        try_number=tries,
        business_date=bdate,
        rows_in=rows_in,
        rows_rejected=rejected,
        sim_now=sim,
    )


def evaluate_all(
    conn: psycopg.Connection, rules: list[SlaRule], now_real: datetime
) -> list[SlaResult]:
    """Evaluate every rule and append the results to ``monitoring.sla_results``."""
    clock = read_clock(conn)
    marts_max = _marts_max_date(conn)
    results = []
    for rule in rules:
        if rule.kind == "lag":
            results.append(evaluate_lag(rule, marts_max, clock.now, clock.advanced_at, now_real))
        else:
            results.append(evaluate_duration(rule, _latest_run(conn, rule.dag_id)))
    with conn.transaction():
        for r in results:
            conn.execute(
                """
                insert into monitoring.sla_results
                    (rule, status, expected, actual, detail, sim_now, evaluated_at)
                values (%s, %s, %s, %s, %s, %s, %s)
                """,
                (r.rule, r.status, r.expected, r.actual, r.detail, clock.now, now_real),
            )
    return results
