"""End-to-end scenario verifier: inject -> detect -> fix -> resolved, against the live stack.

For each scenario: no incident may be open beforehand; the scenario is switched on and its day (or
FX run) produced; monitoring must then open the expected incident type; after the realistic fix
(resend + reprocess, next delivery, or next run) monitoring must leave no incident open.

Detection is asynchronous (asset-driven DAG runs), so every check is a bounded settle loop of
wait-until-idle + a monitoring run rather than a single shot.
"""

import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Literal

import httpx
import psycopg

from drct.chaos.state import enable
from drct.common.airflow_api import AirflowClient
from drct.common.config import Settings, get_settings
from drct.generator.writer import simulate_day
from drct.ops.commands import IDLE_TIMEOUT_S, chaos_reset

Fix = Literal["resend", "replay_accept", "next_delivery", "next_run"]

SETTLE_CYCLES = 4
RUN_TIMEOUT_S = 900


@dataclass(frozen=True)
class ScenarioExpectation:
    scenario: str
    incident_type: str
    fix: Fix
    extra_days: int = 0


EXPECTATIONS: tuple[ScenarioExpectation, ...] = (
    ScenarioExpectation("duplicates", "duplicate_records", "resend"),
    ScenarioExpectation("missing_dates", "missing_values", "resend"),
    ScenarioExpectation("broken_fk", "broken_reference", "resend"),
    ScenarioExpectation("schema_change", "schema_change", "resend"),
    ScenarioExpectation("negative_revenue", "out_of_range", "resend"),
    # Two missed deliveries push the source past the 44h error threshold.
    ScenarioExpectation("stale_data", "stale_data", "next_delivery", extra_days=1),
    ScenarioExpectation("volume_spike", "volume_anomaly", "resend"),
    ScenarioExpectation("api_flaky", "sla_breach", "next_run"),
)


@dataclass(frozen=True)
class ScenarioOutcome:
    scenario: str
    detected: bool
    resolved: bool
    seconds: float
    detail: str

    @property
    def passed(self) -> bool:
        return self.detected and self.resolved


class ScenarioFailed(Exception):
    """A step of the scenario could not be carried out (failed DAG run, nothing to resend...)."""


def open_incident_types(conn: psycopg.Connection) -> list[str]:
    """Distinct types of the currently open incidents, sorted."""
    rows = conn.execute(
        "select distinct type from monitoring.incidents where status = 'open' order by type"
    ).fetchall()
    return [incident_type for (incident_type,) in rows]


def _run_dag(client: AirflowClient, dag_id: str, conf: dict | None = None) -> None:
    """Trigger ``dag_id`` and require that this very run succeeds (the CLI paths only wait)."""
    run_id = client.trigger_dag(dag_id, conf)
    state = client.wait_for_run(dag_id, run_id, timeout_s=RUN_TIMEOUT_S)
    if state != "success":
        raise ScenarioFailed(f"{dag_id} run {run_id} ended {state}")


def _settle(
    client: AirflowClient,
    conn: psycopg.Connection,
    settings: Settings,
    done: Callable[[list[str]], bool],
) -> list[str]:
    """Up to ``SETTLE_CYCLES`` of idle + monitoring run until ``done(open types)``; returns them."""
    open_types: list[str] = []
    for _ in range(SETTLE_CYCLES):
        client.wait_until_idle(timeout_s=IDLE_TIMEOUT_S, landing_dir=settings.landing_dir)
        _run_dag(client, "monitoring")
        open_types = open_incident_types(conn)
        if done(open_types):
            break
    return open_types


def _inject(
    exp: ScenarioExpectation, client: AirflowClient, conn: psycopg.Connection, settings: Settings
) -> None:
    if exp.scenario == "api_flaky":
        # The flaky FX run itself is the injection: its retries (~80s) must still end in success.
        _run_dag(client, "ingest_fx")
        return
    for _ in range(1 + exp.extra_days):
        simulate_day(conn, settings)
    _run_dag(client, "ingest_fx")  # as the simulate-day CLI does: FX never goes stale


def _apply_fix(
    exp: ScenarioExpectation, client: AirflowClient, conn: psycopg.Connection, settings: Settings
) -> None:
    dates = chaos_reset(conn, settings)
    if exp.fix == "resend":
        if not dates:
            raise ScenarioFailed("chaos reset returned no affected dates to reprocess")
        _run_dag(client, "reprocess", {"start": dates[0].isoformat(), "end": dates[-1].isoformat()})
    elif exp.fix == "next_delivery":
        simulate_day(conn, settings)
        _run_dag(client, "ingest_fx")
    elif exp.fix == "next_run":
        _run_dag(client, "ingest_fx")
    else:
        raise ScenarioFailed(f"fix {exp.fix!r} is not automated by the verifier")


def run_scenario(
    exp: ScenarioExpectation, client: AirflowClient, conn: psycopg.Connection
) -> ScenarioOutcome:
    """Inject ``exp.scenario``, check it is detected, fix it, check every incident resolves."""
    settings = get_settings()
    started = time.monotonic()
    detected = resolved = False
    notes = [f"expected {exp.incident_type}"]
    chaos_on = False
    try:
        open_types = open_incident_types(conn)
        if open_types:
            open_types = _settle(client, conn, settings, lambda types: not types)
        if open_types:
            raise ScenarioFailed(f"precondition: incidents already open: {', '.join(open_types)}")

        enable(conn, exp.scenario)
        chaos_on = True
        _inject(exp, client, conn, settings)
        open_types = _settle(client, conn, settings, lambda types: exp.incident_type in types)
        detected = exp.incident_type in open_types
        companions = [t for t in open_types if t != exp.incident_type]
        notes.append(f"companions: {', '.join(companions) or 'none'}")
        if not detected:
            notes.append(f"not detected; open: {', '.join(open_types) or 'none'}")

        # Fix even when undetected, so the next scenario starts from clean data.
        _apply_fix(exp, client, conn, settings)
        chaos_on = False
        open_types = _settle(client, conn, settings, lambda types: not types)
        resolved = not open_types
        if not resolved:
            notes.append(f"still open: {', '.join(open_types)}")
    except (ScenarioFailed, TimeoutError, httpx.HTTPError) as exc:
        notes.append(f"error: {exc}")
    finally:
        if chaos_on:
            chaos_reset(conn, settings)  # never leave a scenario switched on
    return ScenarioOutcome(
        exp.scenario, detected, resolved, time.monotonic() - started, "; ".join(notes)
    )


def format_table(outcomes: Iterable[ScenarioOutcome]) -> str:
    outcomes = list(outcomes)
    lines = [f"{'scenario':<17} {'result':<30} {'seconds':>7}  detail"]
    for o in outcomes:
        flags = f"detected={o.detected} resolved={o.resolved}"
        lines.append(f"{o.scenario:<17} {flags:<30} {o.seconds:>7.0f}  {o.detail}")
    passed = sum(o.passed for o in outcomes)
    lines.append(f"{passed}/{len(outcomes)} scenarios detected and resolved")
    return "\n".join(lines)


def verify_all(
    client: AirflowClient,
    conn: psycopg.Connection,
    expectations: Iterable[ScenarioExpectation] = EXPECTATIONS,
) -> list[ScenarioOutcome]:
    """Run every scenario in order, printing each outcome as it lands, then the summary table."""
    outcomes = []
    for exp in expectations:
        print(f"-> {exp.scenario}", flush=True)
        outcome = run_scenario(exp, client, conn)
        print(f"   detected={outcome.detected} resolved={outcome.resolved} ({outcome.detail})")
        outcomes.append(outcome)
    print(format_table(outcomes))
    return outcomes
