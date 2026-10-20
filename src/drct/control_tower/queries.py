"""Read-only queries behind the Control Tower panels.

Every function takes an open connection (the app uses ``CONTROL_TOWER_DSN``, role
``control_tower_ro``) and returns a ``pd.DataFrame`` with fixed columns, empty on a fresh install.
Detector tables are append-only, so each query takes the latest row per subject.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Literal

import pandas as pd
import psycopg

from drct.common.db import connect

__all__ = [
    "ERROR_HOURS",
    "WARN_HOURS",
    "HealthSummary",
    "connect",
    "failed_tests",
    "freshness_status",
    "health_summary",
    "incidents",
    "quality_trend",
    "readable_node",
    "sla_timeline",
    "volume_series",
]

WARN_HOURS = 20
ERROR_HOURS = 44


@dataclass(frozen=True)
class HealthSummary:
    status: Literal["healthy", "degraded", "critical", "no_data"]
    open_critical: int
    open_warning: int
    sim_now: datetime | None


def _frame(
    conn: psycopg.Connection, sql: str, columns: list[str], params: tuple | dict = ()
) -> pd.DataFrame:
    return pd.DataFrame(conn.execute(sql, params).fetchall(), columns=columns)


def _sim_now(conn: psycopg.Connection) -> datetime | None:
    """The simulated clock, or None while it is uninitialised (the function raises then)."""
    try:
        with conn.transaction():
            row = conn.execute("select control.sim_now()").fetchone()
    except psycopg.errors.RaiseException:
        return None
    return row[0] if row else None


def health_summary(conn: psycopg.Connection) -> HealthSummary:
    """Overall status: critical if any open critical incident, degraded if any open warning."""
    counts = dict(
        conn.execute(
            "select severity, count(*) from monitoring.incidents"
            " where status = 'open' group by severity"
        ).fetchall()
    )
    has_data = conn.execute(
        "select 1 where exists (select 1 from monitoring.freshness)"
        " or exists (select 1 from monitoring.incidents)"
    ).fetchall()
    open_critical = int(counts.get("critical", 0))
    open_warning = int(counts.get("warning", 0))
    if not has_data:
        status = "no_data"
    elif open_critical:
        status = "critical"
    elif open_warning:
        status = "degraded"
    else:
        status = "healthy"
    return HealthSummary(status, open_critical, open_warning, _sim_now(conn))


def freshness_status(conn: psycopg.Connection) -> pd.DataFrame:
    """Latest freshness check per source, with its age in sim hours and the thresholds."""
    frame = _frame(
        conn,
        """
        select distinct on (source) source, age_seconds / 3600.0, status
          from monitoring.freshness
         order by source, checked_at desc nulls last
        """,
        ["source", "age_hours", "status"],
    )
    frame["warn_hours"] = WARN_HOURS
    frame["error_hours"] = ERROR_HOURS
    return frame


# One row per test per invocation: the same artifacts published twice must not count twice, and
# a failing duplicate wins over a passing one (as the incident engine reads them).
_DEDUPED_TESTS = """
    select distinct on (invocation_id, unique_id) *
      from monitoring.test_results
     order by invocation_id, unique_id, status in ('pass', 'skipped')
"""


def quality_trend(conn: psycopg.Connection, invocations: int = 20) -> pd.DataFrame:
    """Pass rate (passed / non-skipped) of the last ``invocations`` dbt test runs, oldest first."""
    return _frame(
        conn,
        f"""
        select generated_at, pass_rate, failed, warned from (
            select max(generated_at) as generated_at,
                   count(*) filter (where status = 'pass')::float
                       / nullif(count(*) filter (where status <> 'skipped'), 0) as pass_rate,
                   count(*) filter (where status in ('fail', 'error')) as failed,
                   count(*) filter (where status = 'warn') as warned
              from ({_DEDUPED_TESTS}) t
             group by invocation_id
             order by max(generated_at) desc nulls last
             limit %s
        ) recent
        order by generated_at
        """,
        ["generated_at", "pass_rate", "failed", "warned"],
        (invocations,),
    )


def volume_series(conn: psycopg.Connection, days: int = 30) -> pd.DataFrame:
    """Daily row counts for the last ``days`` business dates, flagged by the latest evaluation."""
    return _frame(
        conn,
        """
        with latest as (
            select distinct on (source, business_date) source, business_date, is_anomaly
              from monitoring.volume_anomalies
             order by source, business_date, evaluated_at desc nulls last
        )
        select m.source, m.business_date, m.row_count, coalesce(l.is_anomaly, false)
          from monitoring.volume_metrics m
          left join latest l using (source, business_date)
         where m.business_date > (select max(business_date) from monitoring.volume_metrics) - %s
         order by m.source, m.business_date
        """,
        ["source", "business_date", "row_count", "is_anomaly"],
        (days,),
    )


def failed_tests(conn: psycopg.Connection) -> pd.DataFrame:
    """Every non-passing test of the latest dbt invocation, errors before warnings."""
    return _frame(
        conn,
        f"""
        select test_name, attached_node, status, failures, message, compiled_sql
          from ({_DEDUPED_TESTS}) t
         where invocation_id = (
               select invocation_id from monitoring.test_results
                order by generated_at desc nulls last limit 1)
           and status <> 'pass'
         order by array_position(array['error', 'fail', 'warn'], status) nulls last, test_name
        """,
        ["test_name", "attached_node", "status", "failures", "message", "compiled_sql"],
    )


def sla_timeline(conn: psycopg.Connection, days: int = 14) -> pd.DataFrame:
    """One status per SLA rule per sim day over the last ``days`` sim days.

    A day counts as ``breached`` if any evaluation that day breached; otherwise it takes the
    day's latest status. ``evaluated_at`` is the day's latest evaluation (real time) and
    ``latest_status`` its result, so the newest day also says where each rule stands now.
    """
    return _frame(
        conn,
        """
        with evals as (
            select rule, status, evaluated_at,
                   (coalesce(sim_now, evaluated_at) at time zone 'UTC')::date as sim_date
              from monitoring.sla_results
        ), daily as (
            select distinct on (rule, sim_date) rule, sim_date, evaluated_at, status,
                   bool_or(status = 'breached') over (partition by rule, sim_date) as any_breach
              from evals
             order by rule, sim_date, evaluated_at desc nulls last
        )
        select rule, evaluated_at, case when any_breach then 'breached' else status end,
               sim_date, status
          from daily
         where sim_date > (select max(sim_date) from evals) - %s
         order by rule, sim_date
        """,
        ["rule", "evaluated_at", "status", "sim_date", "latest_status"],
        (days,),
    )


def incidents(conn: psycopg.Connection, status: str | None = None) -> pd.DataFrame:
    """Incidents, open before resolved, critical before warning, most recently seen first."""
    return _frame(
        conn,
        """
        select id, type, asset, severity, status, first_seen, last_seen, resolved_at, detail,
               coalesce(affected_downstream, '{}')
          from monitoring.incidents
         where %(status)s::text is null or status = %(status)s::text
         order by status = 'open' desc, severity = 'critical' desc, last_seen desc, id desc
        """,
        [
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
        ],
        {"status": status},
    )


def readable_node(unique_id: str) -> str:
    """Name a dbt node for people.

    ``model.drct.fct_payments`` -> ``fct_payments``, ``exposure.drct.control_tower`` ->
    ``Control Tower (dashboard)``, ``source.drct.raw.sales`` -> ``raw.sales``; test ids lose their
    hash suffix. Anything else (``raw.sales``, SLA rules, DAG ids) is already readable.
    """
    parts = unique_id.split(".")
    kind = parts[0]
    if len(parts) < 3 or kind not in ("model", "test", "seed", "snapshot", "source", "exposure"):
        return unique_id
    if kind == "source":
        return ".".join(parts[2:])
    if kind == "exposure":
        return f"{parts[2].replace('_', ' ').title()} (dashboard)"
    return parts[2]
