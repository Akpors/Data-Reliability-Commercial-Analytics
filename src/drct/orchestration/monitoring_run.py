"""Helpers for the monitoring DAG, kept free of Airflow imports so they unit-test in the app image."""

import json
from collections.abc import Mapping
from pathlib import Path

from drct.common.logging import get_logger

log = get_logger("drct.orchestration.monitoring_run")

_FAILED_STATES = frozenset({"failed", "upstream_failed"})


def failed_tasks(states: Mapping[str, str | None]) -> list[str]:
    """Task ids (in mapping order) whose state is ``failed`` or ``upstream_failed``."""
    return [task_id for task_id, state in states.items() if state in _FAILED_STATES]


def read_manifest(path: Path) -> dict | None:
    """The dbt manifest, or None when it is missing or half-written (dbt is rewriting it)."""
    try:
        return json.loads(path.read_text())
    except (FileNotFoundError, json.JSONDecodeError) as exc:
        log.warning("manifest_unavailable", path=str(path), reason=type(exc).__name__)
        return None
