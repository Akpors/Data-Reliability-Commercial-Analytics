"""Write generator frames to the landing zone, and the seed / simulate-day operations."""

import dataclasses
import os
from collections.abc import Callable
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Literal

import pandas as pd
import psycopg

from drct.chaos.scenarios import apply_scenarios, suppressed_sources
from drct.chaos.state import active, record_affected
from drct.common.config import Settings
from drct.common.sim_clock import (
    ClockNotInitialised,
    SimClock,
    advance_day,
    current_business_date,
    init_clock,
    read_clock,
)
from drct.generator.daily import DayBatch, build_day
from drct.generator.entities import build_customers, build_products

HISTORY_DAYS = 90
# Real date of first seed minus this many days is business date day0.
DAY0_OFFSET_DAYS = 180
CLOCK_HOUR_UTC = 6

_EXTENSIONS = {"sales": "csv", "payments": "parquet", "customers": "json", "products": "json"}


@dataclasses.dataclass(frozen=True)
class DayFiles:
    """Everything one business date delivers: the transactions and both snapshots."""

    batch: DayBatch
    customers: pd.DataFrame
    products: pd.DataFrame


def file_name(source: str, business_date: date) -> str:
    return f"{source}_{business_date.isoformat()}.{_EXTENSIONS[source]}"


def _atomic_write(target: Path, write: Callable[[Path], None]) -> Path:
    """Write via a hidden temp file in the same directory, then rename into place.

    The rename is atomic, so a file trigger never sees a half-written landing file.
    """
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_name(f".{target.name}.tmp")
    write(tmp)
    os.replace(tmp, target)
    return target


def _target(landing_dir: Path, source: str, business_date: date, subdir: str | None = None) -> Path:
    folder = landing_dir / source
    if subdir is not None:
        folder = folder / subdir
    return folder / file_name(source, business_date)


def write_transactions(
    batch: DayBatch,
    landing_dir: Path,
    suppress: set[str] | frozenset[str] = frozenset(),
    subdir: str | None = None,
) -> list[Path]:
    """Write the day's sales (csv) and payments (parquet); returns the paths written.

    With ``subdir`` (e.g. ``_resend``) the files go to ``landing/<source>/<subdir>/`` instead.
    """
    paths: list[Path] = []
    if "sales" not in suppress:
        target = _target(landing_dir, "sales", batch.business_date, subdir)
        paths.append(_atomic_write(target, lambda p: batch.sales.to_csv(p, index=False)))
    if "payments" not in suppress:
        target = _target(landing_dir, "payments", batch.business_date, subdir)
        paths.append(_atomic_write(target, lambda p: batch.payments.to_parquet(p, index=False)))
    return paths


def write_snapshot(
    source: Literal["customers", "products"],
    df: pd.DataFrame,
    business_date: date,
    landing_dir: Path,
    suppress: set[str] | frozenset[str] = frozenset(),
    subdir: str | None = None,
) -> Path | None:
    """Write a full snapshot as a JSON array of records; None if the source is suppressed.

    With ``subdir`` the file goes to ``landing/<source>/<subdir>/`` instead.
    """
    if source in suppress:
        return None
    payload = df.to_json(orient="records", force_ascii=False)
    return _atomic_write(
        _target(landing_dir, source, business_date, subdir),
        lambda p: p.write_text(payload, encoding="utf-8"),
    )


class AlreadySeeded(RuntimeError):
    """The sim clock already exists, so the warehouse has been seeded."""


def seed(
    conn: psycopg.Connection, settings: Settings, real_today: date, force: bool = False
) -> SimClock:
    """Write 90 days of history and start the sim clock at day0+90 06:00 UTC.

    Raises ``AlreadySeeded("already seeded")`` if a clock exists, unless ``force``.
    """
    if not force:
        try:
            read_clock(conn)
        except ClockNotInitialised:
            pass
        else:
            raise AlreadySeeded("already seeded")

    day0 = real_today - timedelta(days=DAY0_OFFSET_DAYS)
    last_day = day0 + timedelta(days=HISTORY_DAYS - 1)
    products = build_products(settings.seed)
    customers = build_customers(settings.seed, as_of=last_day)
    for offset in range(HISTORY_DAYS):
        # build_day only uses customers who had signed up by that day, so one list serves all.
        batch = build_day(day0 + timedelta(days=offset), settings.seed, customers, products)
        write_transactions(batch, settings.landing_dir)
    write_snapshot("customers", customers, last_day, settings.landing_dir)
    write_snapshot("products", products, last_day, settings.landing_dir)

    start = datetime.combine(day0 + timedelta(days=HISTORY_DAYS), time(CLOCK_HOUR_UTC), tzinfo=UTC)
    with conn.transaction():
        clock = init_clock(conn, day0, start)
        conn.execute(
            """
            insert into control.fx_sync (id, watermark) values (1, %s)
            on conflict (id) do update set watermark = excluded.watermark
            """,
            (day0 - timedelta(days=1),),
        )
    return clock


def build_clean_day(settings: Settings, business_date: date) -> DayFiles:
    """The chaos-free files of ``business_date``: transactions plus both snapshots."""
    products = build_products(settings.seed)
    customers = build_customers(settings.seed, as_of=business_date)
    batch = build_day(business_date, settings.seed, customers, products)
    return DayFiles(batch, customers, products)


def write_day(
    day: DayFiles,
    landing_dir: Path,
    suppress: set[str] | frozenset[str] = frozenset(),
    subdir: str | None = None,
) -> None:
    business_date = day.batch.business_date
    write_transactions(day.batch, landing_dir, suppress, subdir)
    write_snapshot("customers", day.customers, business_date, landing_dir, suppress, subdir)
    write_snapshot("products", day.products, business_date, landing_dir, suppress, subdir)


def simulate_day(conn: psycopg.Connection, settings: Settings) -> date:
    """Advance the clock one day and write the landing files for the new business date.

    Active chaos scenarios shape (or suppress) the files and the date is recorded as affected.
    The day is built first, so a build failure leaves the clock where it was.
    """
    business_date = current_business_date(read_clock(conn)) + timedelta(days=1)
    chaos = active(conn)
    day = build_clean_day(settings, business_date)
    day = dataclasses.replace(day, batch=apply_scenarios(day.batch, chaos, settings.seed))
    with conn.transaction():
        advanced = current_business_date(advance_day(conn))
        if advanced != business_date:  # the clock moved under us: roll the advance back
            raise RuntimeError(f"sim clock moved: built {business_date}, clock at {advanced}")
        record_affected(conn, [s for s in chaos if s != "api_flaky"], business_date)
    write_day(day, settings.landing_dir, suppress=suppressed_sources(chaos))
    return business_date
