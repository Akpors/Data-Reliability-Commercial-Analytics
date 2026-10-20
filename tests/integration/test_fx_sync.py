from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from drct.ingestion.fx import (
    FxRate,
    StubFxClient,
    get_watermark,
    set_watermark,
    sync_fx,
    sync_fx_range,
    upsert_rates,
)

SIM_NOW = datetime(2026, 5, 8, 6, tzinfo=UTC)


class RecordingClient:
    def __init__(self) -> None:
        self.calls: list[tuple[date, date]] = []

    def rates(self, start: date, end: date) -> list[FxRate]:
        self.calls.append((start, end))
        return StubFxClient().rates(start, end)


def fx_count(conn) -> int:
    return conn.execute("select count(*) from raw.fx_rates").fetchone()[0]


@pytest.mark.db
def test_sync_fx_advances_watermark_and_is_idempotent(db_conn):
    set_watermark(db_conn, date(2026, 4, 30))
    assert sync_fx(db_conn, StubFxClient(), date(2026, 5, 7), SIM_NOW, "b") > 0
    assert get_watermark(db_conn) == date(2026, 5, 7)
    assert sync_fx(db_conn, StubFxClient(), date(2026, 5, 7), SIM_NOW, "b") == 0


@pytest.mark.db
def test_sync_fx_fetches_only_the_gap_after_the_watermark(db_conn):
    client = RecordingClient()
    set_watermark(db_conn, date(2026, 4, 30))
    # 2026-05-01 Fri .. 2026-05-07 Thu = 5 weekdays x 2 quotes
    assert sync_fx(db_conn, client, date(2026, 5, 7), SIM_NOW, "b") == 10
    assert client.calls == [(date(2026, 5, 1), date(2026, 5, 7))]
    assert fx_count(db_conn) == 10
    # watermark already at/after through: client is not called
    assert sync_fx(db_conn, client, date(2026, 5, 5), SIM_NOW, "b") == 0
    assert len(client.calls) == 1


@pytest.mark.db
def test_sync_fx_requires_a_watermark(db_conn):
    assert get_watermark(db_conn) is None
    with pytest.raises(RuntimeError):
        sync_fx(db_conn, StubFxClient(), date(2026, 5, 7), SIM_NOW, "b")


@pytest.mark.db
def test_set_watermark_upserts_the_single_row(db_conn):
    set_watermark(db_conn, date(2026, 4, 30))
    set_watermark(db_conn, date(2026, 5, 1))
    assert get_watermark(db_conn) == date(2026, 5, 1)
    assert db_conn.execute("select count(*) from control.fx_sync").fetchone()[0] == 1


@pytest.mark.db
def test_upsert_rates_updates_in_place(db_conn):
    d = date(2026, 5, 1)
    assert upsert_rates(db_conn, [FxRate(d, "GBP", "EUR", Decimal("1.17"))], SIM_NOW, "b1") == 1
    assert upsert_rates(db_conn, [FxRate(d, "GBP", "EUR", Decimal("1.18"))], SIM_NOW, "b2") == 1
    rate, batch = db_conn.execute("select rate, _batch_id from raw.fx_rates").fetchone()
    assert (rate, batch) == (Decimal("1.18000000"), "b2")
    assert fx_count(db_conn) == 1


@pytest.mark.db
def test_sync_fx_range_backfills_without_moving_watermark(db_conn):
    set_watermark(db_conn, date(2026, 4, 30))
    assert (
        sync_fx_range(db_conn, StubFxClient(), date(2026, 3, 2), date(2026, 3, 6), SIM_NOW, "b")
        == 10
    )
    assert fx_count(db_conn) == 10
    assert get_watermark(db_conn) == date(2026, 4, 30)
