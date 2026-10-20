from datetime import UTC, date, datetime

import pytest

from drct.common.sim_clock import (
    ClockNotInitialised,
    SimClock,
    advance_day,
    current_business_date,
    init_clock,
    read_clock,
)

DAY0 = date(2026, 4, 19)
NOW = datetime(2026, 7, 18, 6, tzinfo=UTC)


@pytest.mark.db
def test_read_clock_raises_until_initialised(db_conn):
    with pytest.raises(ClockNotInitialised):
        read_clock(db_conn)


@pytest.mark.db
def test_init_clock_then_read_clock_round_trips(db_conn):
    created = init_clock(db_conn, DAY0, NOW)
    assert created.day0 == DAY0
    assert created.now == NOW
    assert read_clock(db_conn) == created
    assert db_conn.execute("select control.sim_now()").fetchone()[0] == NOW


@pytest.mark.db
def test_init_clock_upserts_on_reseed(db_conn):
    init_clock(db_conn, DAY0, NOW)
    later = datetime(2026, 8, 1, 6, tzinfo=UTC)
    init_clock(db_conn, date(2026, 5, 2), later)
    clock = read_clock(db_conn)
    assert (clock.day0, clock.now) == (date(2026, 5, 2), later)
    assert db_conn.execute("select count(*) from control.sim_clock").fetchone()[0] == 1


@pytest.mark.db
def test_advance_day_moves_clock_and_sim_now(db_conn):
    init_clock(db_conn, date(2026, 4, 19), datetime(2026, 7, 18, 6, tzinfo=UTC))
    clock = advance_day(db_conn)
    assert clock.now == datetime(2026, 7, 19, 6, tzinfo=UTC)
    assert current_business_date(clock) == date(2026, 7, 18)
    assert db_conn.execute("select control.sim_now()").fetchone()[0] == clock.now


@pytest.mark.db
def test_advance_day_stamps_real_time_and_keeps_day0(db_conn):
    init_clock(db_conn, DAY0, NOW)
    before = datetime.now(UTC)
    clock = advance_day(db_conn)
    assert clock.day0 == DAY0
    assert before <= clock.advanced_at <= datetime.now(UTC)
    assert read_clock(db_conn) == clock


@pytest.mark.db
def test_advance_day_raises_when_not_initialised(db_conn):
    with pytest.raises(ClockNotInitialised):
        advance_day(db_conn)


def test_current_business_date_is_the_day_before_now():
    clock = SimClock(day0=DAY0, now=datetime(2026, 7, 18, 6, tzinfo=UTC), advanced_at=NOW)
    assert current_business_date(clock) == date(2026, 7, 17)
