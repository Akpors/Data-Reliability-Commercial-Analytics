"""The simulated clock stored in ``control.sim_clock`` (single row, id = 1)."""

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta

import psycopg


class ClockNotInitialised(RuntimeError):
    """control.sim_clock has no row yet."""


@dataclass(frozen=True)
class SimClock:
    day0: date
    now: datetime
    advanced_at: datetime


def _to_clock(row: tuple) -> SimClock:
    day0, now, advanced_at = row
    # Normalise to UTC so .date() never depends on the session time zone.
    return SimClock(day0=day0, now=now.astimezone(UTC), advanced_at=advanced_at.astimezone(UTC))


def init_clock(conn: psycopg.Connection, day0: date, now: datetime) -> SimClock:
    """Create or reset the clock (upsert, so a forced re-seed can re-initialise it)."""
    row = conn.execute(
        """
        insert into control.sim_clock (id, day0, sim_now, advanced_at)
        values (1, %s, %s, now())
        on conflict (id) do update
            set day0 = excluded.day0,
                sim_now = excluded.sim_now,
                advanced_at = excluded.advanced_at
        returning day0, sim_now, advanced_at
        """,
        (day0, now),
    ).fetchone()
    return _to_clock(row)


def read_clock(conn: psycopg.Connection) -> SimClock:
    row = conn.execute(
        "select day0, sim_now, advanced_at from control.sim_clock where id = 1"
    ).fetchone()
    if row is None:
        raise ClockNotInitialised("control.sim_clock is not initialised")
    return _to_clock(row)


def advance_day(conn: psycopg.Connection) -> SimClock:
    """Add one day to the clock and stamp advanced_at with real time."""
    # Arithmetic is done in UTC so the step is always 24h, whatever the session time zone.
    row = conn.execute(
        """
        update control.sim_clock
           set sim_now = ((sim_now at time zone 'UTC') + interval '1 day') at time zone 'UTC',
               advanced_at = now()
         where id = 1
        returning day0, sim_now, advanced_at
        """
    ).fetchone()
    if row is None:
        raise ClockNotInitialised("control.sim_clock is not initialised")
    return _to_clock(row)


def current_business_date(clock: SimClock) -> date:
    """A clock at D 06:00 means files for business date D-1 have been delivered."""
    return clock.now.date() - timedelta(days=1)
