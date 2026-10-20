"""Chaos switches (``control.chaos_state``) and the business dates they touched
(``control.chaos_affected``).

Read only by the generator path and the FX task, never by detectors.
"""

from collections.abc import Iterable
from datetime import date

import psycopg
from psycopg.types.json import Jsonb

from drct.chaos.scenarios import DEFAULT_PARAMS, SCENARIOS, SCHEMA_ADD_DEFAULTS, SCHEMA_MODES
from drct.ingestion.loader import SOURCES


def _resolve_params(scenario: str, params: dict | None) -> dict:
    """The scenario's defaults overridden by ``params``; raises ValueError for unusable values."""
    params = params or {}
    defaults = DEFAULT_PARAMS[scenario]
    if scenario == "schema_change" and params.get("mode") == "add":
        defaults = SCHEMA_ADD_DEFAULTS
    resolved = {**defaults, **params}
    if scenario == "schema_change" and resolved["mode"] not in SCHEMA_MODES:
        raise ValueError(
            f"schema_change mode must be one of {SCHEMA_MODES}, got {resolved['mode']!r}"
        )
    if scenario == "stale_data" and resolved["source"] not in SOURCES:
        raise ValueError(f"stale_data source must be one of {SOURCES}, got {resolved['source']!r}")
    return resolved


def enable(conn: psycopg.Connection, scenario: str, params: dict | None = None) -> None:
    """Switch ``scenario`` on (or replace its params); it applies from the next simulated day."""
    if scenario not in SCENARIOS:
        raise ValueError(f"unknown chaos scenario {scenario!r}; expected one of {SCENARIOS}")
    conn.execute(
        """
        insert into control.chaos_state (scenario, params, enabled_at)
        values (%s, %s, now())
        on conflict (scenario) do update
            set params = excluded.params, enabled_at = excluded.enabled_at
        """,
        (scenario, Jsonb(_resolve_params(scenario, params))),
    )


def active(conn: psycopg.Connection) -> dict[str, dict]:
    """Every enabled scenario with its params."""
    rows = conn.execute("select scenario, params from control.chaos_state order by scenario")
    return {scenario: params or {} for scenario, params in rows.fetchall()}


def record_affected(
    conn: psycopg.Connection, scenarios: Iterable[str], business_date: date
) -> None:
    """Remember that ``scenarios`` shaped ``business_date``'s files, so a reset can re-send them."""
    with conn.transaction():
        for scenario in scenarios:
            conn.execute(
                """
                insert into control.chaos_affected (scenario, business_date) values (%s, %s)
                on conflict do nothing
                """,
                (scenario, business_date),
            )


def reset(conn: psycopg.Connection) -> list[date]:
    """Switch every scenario off; returns the sorted distinct business dates chaos affected."""
    with conn.transaction():
        rows = conn.execute(
            "select distinct business_date from control.chaos_affected order by business_date"
        ).fetchall()
        conn.execute("delete from control.chaos_affected")
        conn.execute("delete from control.chaos_state")
    return [business_date for (business_date,) in rows]
