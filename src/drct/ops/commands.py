"""Command-line entry points for the operator."""

import argparse
from collections.abc import Sequence
from datetime import UTC, date, datetime, timedelta

import psycopg

from drct.chaos.state import enable, reset
from drct.common.airflow_api import AirflowClient
from drct.common.config import Settings, get_settings
from drct.common.db import connect
from drct.common.sim_clock import ClockNotInitialised, current_business_date, read_clock
from drct.generator.writer import (
    HISTORY_DAYS,
    AlreadySeeded,
    build_clean_day,
    seed,
    simulate_day,
    write_day,
)
from drct.ingestion.fx import make_fx_client, sync_fx
from drct.ingestion.loader import RESEND_DIR, SOURCES, load_pending
from drct.ingestion.quarantine import replay_quarantine

IDLE_TIMEOUT_S = 900
LOAD_ALL_BATCH_ID = "ci-load-all"


def _positive_int(value: str) -> int:
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError("must be at least 1")
    return number


def _iso_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f"not a YYYY-MM-DD date: {value!r}") from None


def _param(value: str) -> tuple[str, int | float | str]:
    """``k=v``, with numeric values cast to int or float."""
    key, sep, raw = value.partition("=")
    if not sep or not key:
        raise argparse.ArgumentTypeError(f"expected k=v, got {value!r}")
    for cast in (int, float):
        try:
            return key, cast(raw)
        except ValueError:
            continue
    return key, raw


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m drct.ops", description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    seed_cmd = commands.add_parser("seed", help="write 90 days of history and start the sim clock")
    seed_mode = seed_cmd.add_mutually_exclusive_group()
    seed_mode.add_argument("--force", action="store_true", help="re-seed even if already seeded")
    seed_mode.add_argument(
        "--if-needed",
        action="store_true",
        help="skip (exit 0) if already seeded, but still trigger ingest_fx (used by make demo)",
    )
    seed_cmd.add_argument(
        "--no-trigger",
        action="store_true",
        help="do not trigger the ingest_fx DAG afterwards (for runs without Airflow, e.g. CI)",
    )
    sim_cmd = commands.add_parser("simulate-day", help="advance the sim clock and land new files")
    sim_cmd.add_argument("--days", type=_positive_int, default=1, help="days to simulate")
    reprocess_cmd = commands.add_parser(
        "reprocess", help="re-ingest a date range via the reprocess DAG, then wait for the pipeline"
    )
    reprocess_cmd.add_argument("--from", dest="start", type=_iso_date, required=True)
    reprocess_cmd.add_argument("--to", dest="end", type=_iso_date, required=True)
    commands.add_parser("monitor", help="run a monitoring cycle now and wait for it to finish")
    chaos_cmd = commands.add_parser("chaos", help="inject failures into the next simulated day")
    chaos_actions = chaos_cmd.add_subparsers(dest="chaos_action", required=True)
    enable_cmd = chaos_actions.add_parser("enable", help="switch a scenario on")
    enable_cmd.add_argument("scenario")
    enable_cmd.add_argument(
        "--param", dest="params", type=_param, action="append", default=[], metavar="K=V"
    )
    chaos_actions.add_parser(
        "reset", help="switch chaos off and re-send clean files for the affected dates"
    )
    replay_cmd = commands.add_parser(
        "replay-quarantine", help="move quarantined files back to landing to be ingested again"
    )
    replay_cmd.add_argument("--source", required=True, choices=SOURCES)
    commands.add_parser(
        "verify-scenarios", help="inject, detect, fix and resolve every chaos scenario end to end"
    )
    commands.add_parser(
        "load-all",
        help="load every pending landing file and sync FX directly, without Airflow (CI)",
    )
    return parser


def _trigger_and_wait(
    client: AirflowClient, settings: Settings, dag_id: str, conf: dict | None, what: str
) -> int:
    """Trigger ``dag_id``, require that this very run succeeds, then wait for the pipeline it
    sets off (dbt, monitoring) to go idle. Returns the exit code: 1 unless the run succeeded."""
    run_id = client.trigger_dag(dag_id, conf)
    print(f"Triggered {what} (run {run_id}); waiting for it to finish")
    state = client.wait_for_run(dag_id, run_id, timeout_s=IDLE_TIMEOUT_S)
    if state != "success":
        print(
            f"error: {dag_id} run {run_id} ended {state}: see the run's task logs in the Airflow UI"
            " (http://localhost:8080)"
        )
        return 1
    print(f"{dag_id} run succeeded; waiting for the pipeline to idle")
    client.wait_until_idle(timeout_s=IDLE_TIMEOUT_S, landing_dir=settings.landing_dir)
    print("Pipeline idle")
    return 0


def _reprocess(settings: Settings, start: date, end: date) -> int:
    client = AirflowClient(settings.airflow_api_url)
    conf = {"start": start.isoformat(), "end": end.isoformat()}
    return _trigger_and_wait(client, settings, "reprocess", conf, f"reprocess {start} .. {end}")


def _monitor(settings: Settings) -> int:
    client = AirflowClient(settings.airflow_api_url)
    # Let pending ingestion and dbt runs finish first, so the cycle sees the landed data
    # (straight after seed, freshness would otherwise read empty raw tables).
    client.wait_until_idle(timeout_s=IDLE_TIMEOUT_S, landing_dir=settings.landing_dir)
    return _trigger_and_wait(client, settings, "monitoring", None, "monitoring")


def _trigger_fx(settings: Settings) -> None:
    """Sync FX now rather than at the next 10-minute tick, so FX never goes transiently stale."""
    run_id = AirflowClient(settings.airflow_api_url).trigger_dag("ingest_fx")
    print(f"Triggered ingest_fx (run {run_id})")


def chaos_reset(conn: psycopg.Connection, settings: Settings) -> list[date]:
    """Switch chaos off and have the sources re-send clean files for every affected date.

    The clean transactions and snapshots go to ``landing/<source>/_resend/``, where the reprocess
    DAG prefers them over ``_processed/``. Returns the affected dates, oldest first.
    """
    # One transaction: if a re-send fails, the chaos state and affected dates survive for a retry.
    with conn.transaction():
        dates = reset(conn)
        for day in dates:
            write_day(build_clean_day(settings, day), settings.landing_dir, subdir=RESEND_DIR)
    return dates


def _seed(
    conn: psycopg.Connection,
    settings: Settings,
    force: bool,
    trigger_fx: bool,
    if_needed: bool = False,
) -> int:
    """Seed history. With ``if_needed`` an already-seeded stack is skipped (exit 0) but FX is still
    triggered, so ``make demo`` can run again and recovers a first run whose FX trigger failed."""
    try:
        clock = seed(conn, settings, real_today=datetime.now(UTC).date(), force=force)
    except AlreadySeeded as exc:
        if not if_needed:
            print(f"error: {exc}: run `make nuke` to start again from scratch (deletes all data)")
            return 1
        print(f"Warehouse {exc}; skipping (sim clock at {read_clock(conn).now.isoformat()})")
    else:
        last_day = clock.day0 + timedelta(days=HISTORY_DAYS - 1)
        print(
            f"Seeded business dates {clock.day0} .. {last_day}"
            f" into {settings.landing_dir}; sim clock at {clock.now.isoformat()}"
        )
    if trigger_fx:
        _trigger_fx(settings)
    return 0


def _load_all(conn: psycopg.Connection, settings: Settings) -> int:
    """Ingest everything pending plus FX in-process, the way the DAGs would. Non-zero on quarantine."""
    try:
        clock = read_clock(conn)
    except ClockNotInitialised:
        print("error: the sim clock is not initialised: run `make seed` first")
        return 1
    quarantined = 0
    for source in SOURCES:
        results = load_pending(conn, source, settings.landing_dir, clock.now, LOAD_ALL_BATCH_ID)
        rejected = [r for r in results if r.status == "quarantined"]
        quarantined += len(rejected)
        print(f"{source}: {len(results) - len(rejected)} loaded, {len(rejected)} quarantined")
        for result in rejected:
            print(f"  quarantined {result.path.name}")
    through = current_business_date(clock)
    rates = sync_fx(conn, make_fx_client(settings), through, clock.now, LOAD_ALL_BATCH_ID)
    print(f"fx: {rates} rates synced through {through}")
    return 1 if quarantined else 0


def _simulate(conn: psycopg.Connection, settings: Settings, days: int) -> int:
    try:
        for _ in range(days):
            business_date = simulate_day(conn, settings)
            print(f"Simulated business date {business_date}")
    except ClockNotInitialised:
        print("error: the sim clock is not initialised: run `make seed` first")
        return 1
    _trigger_fx(settings)
    return 0


def _chaos(conn: psycopg.Connection, settings: Settings, args: argparse.Namespace) -> int:
    if args.chaos_action == "enable":
        try:
            enable(conn, args.scenario, dict(args.params))
        except ValueError as exc:
            print(f"error: {exc}")
            return 1
        when = "the next FX sync" if args.scenario == "api_flaky" else "the next simulated day"
        print(f"Chaos scenario {args.scenario} enabled; it takes effect from {when}")
        return 0
    dates = chaos_reset(conn, settings)
    if not dates:
        print("Chaos off; no business date was affected, so nothing was re-sent")
        return 0
    print(f"Chaos off; clean files re-sent to landing/<source>/{RESEND_DIR}/ for:")
    for day in dates:
        print(f"  {day}")
    print(f"Next: make reprocess FROM={dates[0]} TO={dates[-1]}")
    return 0


def _replay(conn: psycopg.Connection, settings: Settings, source: str) -> int:
    replay = replay_quarantine(conn, source, settings.landing_dir)
    if not replay.moved and not replay.untracked:
        print(f"No quarantined {source} files")
        return 0
    for path in replay.moved:
        print(f"Replayed {path.name}; ingest_{source} picks it up from {path.parent}")
    for path in replay.untracked:
        print(
            f"Left {path.name} in {path.parent}: no quarantine record awaits its replay"
            " (left over from an earlier stack, or already replayed)"
        )
    return 0


def _verify(conn: psycopg.Connection, settings: Settings) -> int:
    # Imported here: verify builds on this module's commands (chaos_reset), so a top-level
    # import would be circular.
    from drct.ops.verify import verify_all

    outcomes = verify_all(AirflowClient(settings.airflow_api_url), conn)
    return 0 if all(o.passed for o in outcomes) else 1


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    settings = get_settings()
    if args.command == "reprocess":
        if args.start > args.end:
            parser.error("--from must not be after --to")
        return _reprocess(settings, args.start, args.end)
    if args.command == "monitor":
        return _monitor(settings)
    with connect(settings.warehouse_dsn) as conn:
        if args.command == "seed":
            return _seed(
                conn,
                settings,
                args.force,
                trigger_fx=not args.no_trigger,
                if_needed=args.if_needed,
            )
        if args.command == "load-all":
            return _load_all(conn, settings)
        if args.command == "simulate-day":
            return _simulate(conn, settings, args.days)
        if args.command == "chaos":
            return _chaos(conn, settings, args)
        if args.command == "verify-scenarios":
            return _verify(conn, settings)
        return _replay(conn, settings, args.source)
