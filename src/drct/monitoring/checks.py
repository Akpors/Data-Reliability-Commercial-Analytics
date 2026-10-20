"""Turn every detector's latest output into generic ``Check``s for the incident engine.

Detectors write append-only rows into the ``monitoring`` schema; ``collect_checks`` reads the
latest row(s) per subject and maps them to one uniform shape. Nothing here reads
``control.chaos_state``: a check knows what failed, never why.
"""

from dataclasses import dataclass
from typing import Literal

import psycopg

from drct.monitoring.lineage import node_for_asset

__all__ = ["TEST_TYPE_TO_INCIDENT", "Check", "collect_checks"]

Severity = Literal["warning", "critical"]

TEST_TYPE_TO_INCIDENT = {
    "unique": "duplicate_records",
    "not_null": "missing_values",
    "relationships": "broken_reference",
    "expect_column_values_to_be_between": "out_of_range",
    "singular": "missing_dates",
}


@dataclass(frozen=True)
class Check:
    """One pass/fail signal about one asset. ``passed=None`` means no signal (never resolves)."""

    type: str
    asset: str
    passed: bool | None
    severity: Severity
    detail: str
    lineage_node: str | None = None


def _raw_asset(source: str) -> str:
    """Name a source like freshness does (``raw.sales``); detectors store the bare ``sales``."""
    return source if "." in source else f"raw.{source}"


def _test_checks(conn: psycopg.Connection) -> list[Check]:
    rows = conn.execute(
        """
        select distinct on (unique_id) unique_id, test_name, test_type, attached_node, status,
               severity, failures
          from monitoring.test_results
         where invocation_id = (
               select invocation_id from monitoring.test_results
                order by generated_at desc nulls last limit 1)
         order by unique_id, status in ('pass', 'skipped')
        """
    ).fetchall()
    checks = []
    for unique_id, test_name, test_type, attached_node, status, severity, failures in rows:
        incident_type = TEST_TYPE_TO_INCIDENT.get(test_type, "test_failure")
        passed: bool | None
        if status == "pass":
            passed = True
        elif status == "skipped":
            passed = None
        else:
            passed = False
        level: Severity = "warning" if status == "warn" or severity == "warn" else "critical"
        detail = f"{test_name}: {status}" + (f", {failures} failing rows" if failures else "")
        checks.append(Check(incident_type, unique_id, passed, level, detail, attached_node))
    return checks


def _freshness_checks(conn: psycopg.Connection) -> list[Check]:

    rows = conn.execute(
        """
        select distinct on (source) source, status, age_seconds
          from monitoring.freshness
         order by source, checked_at desc
        """
    ).fetchall()
    checks = []
    for source, status, age_seconds in rows:
        passed: bool | None = {"pass": True, "warn": False, "error": False}.get(status)
        level: Severity = "warning" if status == "warn" else "critical"
        age = "unknown age" if age_seconds is None else f"age {age_seconds / 3600:.1f}h"
        checks.append(
            Check(
                "stale_data",
                source,
                passed,
                level,
                f"{source} {status}: {age}",
                node_for_asset(source),
            )
        )
    return checks


def _volume_checks(conn: psycopg.Connection) -> list[Check]:

    rows = conn.execute(
        """
        select distinct on (source) source, business_date, row_count, baseline_median,
               robust_z, is_anomaly, reason
          from monitoring.volume_anomalies
         order by source, evaluated_at desc
        """
    ).fetchall()
    checks = []
    for source, business_date, row_count, median, z, is_anomaly, reason in rows:
        asset = _raw_asset(source)
        passed: bool | None = None if reason == "insufficient_history" else not is_anomaly
        detail = f"{asset} {business_date}: {row_count} rows ({reason}"
        if z is not None:
            detail += f", z={z:.1f}, median={median:.0f}"
        checks.append(
            Check("volume_anomaly", asset, passed, "warning", detail + ")", node_for_asset(asset))
        )
    return checks


def _schema_checks(conn: psycopg.Connection) -> list[Check]:

    rows = conn.execute(
        """
        select distinct on (source) source, file_name, added, missing, type_mismatches,
               is_breaking
          from monitoring.schema_events
         order by source, recorded_at desc, id desc
        """
    ).fetchall()
    checks = []
    for source, file_name, added, missing, mismatches, is_breaking in rows:
        asset = _raw_asset(source)
        node = node_for_asset(asset)
        added = added or []
        breaking = (
            f"{file_name}: missing {missing or []}, type mismatches {sorted(mismatches or {})}"
        )
        drift = f"{file_name}: added columns {added}"
        checks.append(
            Check(
                "schema_change",
                asset,
                not is_breaking,
                "critical",
                breaking if is_breaking else f"{file_name}: clean",
                node,
            )
        )
        # A breaking file is already reported (and quarantined) as schema_change, so drift has no signal.
        drift_passed: bool | None = None if is_breaking else not added
        checks.append(Check("schema_drift", asset, drift_passed, "warning", drift, node))
    return checks


def _sla_checks(conn: psycopg.Connection) -> list[Check]:
    rows = conn.execute(
        """
        select distinct on (rule) rule, status, detail
          from monitoring.sla_results
         order by rule, evaluated_at desc
        """
    ).fetchall()
    checks = []
    for rule, status, detail in rows:
        passed: bool | None = {"met": True, "breached": False}.get(status)
        checks.append(Check("sla_breach", rule, passed, "critical", detail or f"{rule}: {status}"))
    return checks


def _pipeline_checks(conn: psycopg.Connection) -> list[Check]:
    rows = conn.execute(
        """
        select distinct on (dag_id) dag_id, run_id, status
          from monitoring.pipeline_runs
         where task_id is null and status is distinct from 'running'
         order by dag_id, started_at desc
        """
    ).fetchall()
    return [
        Check(
            "pipeline_failure",
            dag_id,
            status == "success",
            "critical",
            f"{dag_id} run {run_id}: {status}",
        )
        for dag_id, run_id, status in rows
    ]


def collect_checks(conn: psycopg.Connection) -> list[Check]:
    """Return one check per subject from the latest output of every detector."""
    return [
        *_test_checks(conn),
        *_freshness_checks(conn),
        *_volume_checks(conn),
        *_schema_checks(conn),
        *_sla_checks(conn),
        *_pipeline_checks(conn),
    ]
