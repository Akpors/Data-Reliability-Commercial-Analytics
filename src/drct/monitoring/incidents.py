"""Incident lifecycle: open on a failed check, update while it persists, auto-resolve on pass.

An incident's identity is ``(type, asset)``; at most one is ``open`` at a time (enforced by a
partial unique index). ``plan_incident_changes`` is pure; ``reconcile`` applies its plan.
"""

from dataclasses import dataclass, field
from datetime import datetime

import psycopg

from drct.common.logging import get_logger
from drct.monitoring.checks import Check
from drct.monitoring.lineage import downstream_of, node_for_asset

__all__ = ["Incident", "IncidentChanges", "plan_incident_changes", "reconcile"]

log = get_logger("drct.monitoring.incidents")


@dataclass
class Incident:
    """Mirrors a ``monitoring.incidents`` row."""

    id: int
    type: str
    asset: str
    severity: str
    status: str
    first_seen: datetime
    last_seen: datetime
    resolved_at: datetime | None = None
    detail: str | None = None
    affected_downstream: list[str] | None = None


@dataclass
class IncidentChanges:
    to_open: list[Check] = field(default_factory=list)
    to_update: list[tuple[Incident, Check]] = field(default_factory=list)
    to_resolve: list[Incident] = field(default_factory=list)


def _collapse(checks: list[Check]) -> list[Check]:
    """One check per ``(type, asset)``: failed beats passed, critical beats warning, no signal is dropped.

    Duplicates (e.g. the same dbt artifacts published twice) must not open two incidents.
    """
    best: dict[tuple[str, str], Check] = {}
    for check in checks:
        if check.passed is None:
            continue
        key = (check.type, check.asset)
        current = best.get(key)
        if current is None or _rank(check) > _rank(current):
            best[key] = check
    return list(best.values())


def _rank(check: Check) -> tuple[bool, bool]:
    return (check.passed is False, check.severity == "critical")


def plan_incident_changes(open_incidents: list[Incident], checks: list[Check]) -> IncidentChanges:
    """Match checks to open incidents by ``(type, asset)``; ``passed=None`` is left untouched."""
    open_by_key = {(i.type, i.asset): i for i in open_incidents}
    changes = IncidentChanges()
    for check in _collapse(checks):
        match = open_by_key.get((check.type, check.asset))
        if not check.passed:
            if match is None:
                changes.to_open.append(check)
            else:
                changes.to_update.append((match, check))
        elif match is not None:
            changes.to_resolve.append(match)
    return changes


_COLUMNS = (
    "id, type, asset, severity, status, first_seen, last_seen, resolved_at, detail,"
    " affected_downstream"
)


def _blast_radius(manifest: dict | None, check: Check) -> list[str]:
    if manifest is None:
        return []
    return downstream_of(manifest, node_for_asset(check.lineage_node or check.asset))


def reconcile(
    conn: psycopg.Connection, checks: list[Check], sim_now: datetime, manifest: dict | None
) -> IncidentChanges:
    """Apply the incident plan for ``checks`` in one transaction and return it.

    New incidents get their blast radius from ``manifest``; an update refreshes ``last_seen`` and
    ``detail`` and never lowers severity.
    """
    with conn.transaction():
        rows = conn.execute(
            f"select {_COLUMNS} from monitoring.incidents where status = 'open'"
        ).fetchall()
        changes = plan_incident_changes([Incident(*row) for row in rows], checks)
        for check in changes.to_open:
            conn.execute(
                "insert into monitoring.incidents (type, asset, severity, status, first_seen,"
                " last_seen, detail, affected_downstream)"
                " values (%s, %s, %s, 'open', %s, %s, %s, %s)",
                (
                    check.type,
                    check.asset,
                    check.severity,
                    sim_now,
                    sim_now,
                    check.detail,
                    _blast_radius(manifest, check),
                ),
            )
        for incident, check in changes.to_update:
            severity = (
                "critical" if "critical" in (incident.severity, check.severity) else check.severity
            )
            conn.execute(
                "update monitoring.incidents set last_seen = %s, detail = %s, severity = %s"
                " where id = %s",
                (sim_now, check.detail, severity, incident.id),
            )
        for incident in changes.to_resolve:
            conn.execute(
                "update monitoring.incidents set status = 'resolved', resolved_at = %s"
                " where id = %s",
                (sim_now, incident.id),
            )
    log.info(
        "incidents_reconciled",
        opened=len(changes.to_open),
        updated=len(changes.to_update),
        resolved=len(changes.to_resolve),
    )
    return changes
