"""GBP->EUR/USD exchange rates: Frankfurter (ECB) client, offline stub and watermark sync.

Frankfurter only publishes ECB business days, so weekend gaps are normal; dbt joins each order to
the latest rate on or before its date. ``sync_fx`` is the incremental path (advances the
``control.fx_sync`` watermark); ``sync_fx_range`` is the watermark-neutral backfill path.
"""

import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import ClassVar, Protocol

import httpx
import psycopg

from drct.common.config import Settings
from drct.common.logging import get_logger

__all__ = [
    "BASE_CURRENCY",
    "QUOTE_CURRENCIES",
    "FrankfurterClient",
    "FxClient",
    "FxRate",
    "StubFxClient",
    "get_watermark",
    "make_fx_client",
    "set_watermark",
    "sync_fx",
    "sync_fx_range",
    "upsert_rates",
]

BASE_CURRENCY = "GBP"
QUOTE_CURRENCIES = ("EUR", "USD")

log = get_logger("drct.ingestion.fx")


@dataclass(frozen=True)
class FxRate:
    rate_date: date
    base: str
    quote: str
    rate: Decimal


class FxClient(Protocol):
    def rates(self, start: date, end: date) -> list[FxRate]: ...


class FrankfurterClient:
    """Reads a GBP time series from the free Frankfurter API."""

    def __init__(
        self,
        base_url: str,
        timeout: float = 10.0,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._http = httpx.Client(timeout=timeout, transport=transport)

    def rates(self, start: date, end: date) -> list[FxRate]:
        response = self._http.get(
            f"{self._base_url}/{start.isoformat()}..{end.isoformat()}",
            params={"base": BASE_CURRENCY, "symbols": ",".join(QUOTE_CURRENCIES)},
        )
        response.raise_for_status()
        # parse_float=Decimal keeps the published rates exact (no float drift into numeric(18,8)).
        payload = json.loads(response.text, parse_float=Decimal)
        return [
            FxRate(date.fromisoformat(day), BASE_CURRENCY, quote, rate)
            for day, quotes in payload["rates"].items()
            for quote, rate in quotes.items()
        ]


class StubFxClient:
    """Deterministic offline rates: constant per quote, weekdays only (no weekend rows)."""

    _RATES: ClassVar[dict[str, Decimal]] = {"EUR": Decimal("1.17"), "USD": Decimal("1.27")}

    def rates(self, start: date, end: date) -> list[FxRate]:
        days = (start + timedelta(days=n) for n in range((end - start).days + 1))
        return [
            FxRate(day, BASE_CURRENCY, quote, rate)
            for day in days
            if day.weekday() < 5
            for quote, rate in self._RATES.items()
        ]


def make_fx_client(settings: Settings) -> FxClient:
    if settings.fx_source == "stub":
        return StubFxClient()
    return FrankfurterClient(settings.fx_api_url)


def get_watermark(conn: psycopg.Connection) -> date | None:
    """Last date through which rates have been synced, or None if never initialised."""
    row = conn.execute("select watermark from control.fx_sync where id = 1").fetchone()
    return row[0] if row else None


def set_watermark(conn: psycopg.Connection, d: date) -> None:
    conn.execute(
        """
        insert into control.fx_sync (id, watermark) values (1, %s)
        on conflict (id) do update set watermark = excluded.watermark
        """,
        (d,),
    )


def upsert_rates(
    conn: psycopg.Connection, rates: list[FxRate], sim_now: datetime, batch_id: str
) -> int:
    """Insert or refresh rates keyed on (rate_date, base, quote); returns the count written."""
    if not rates:
        return 0
    with conn.transaction(), conn.cursor() as cur:
        cur.executemany(
            """
            insert into raw.fx_rates (rate_date, base, quote, rate, _sim_loaded_at, _batch_id)
            values (%s, %s, %s, %s, %s, %s)
            on conflict (rate_date, base, quote) do update set
                rate = excluded.rate,
                _loaded_at = now(),
                _sim_loaded_at = excluded._sim_loaded_at,
                _batch_id = excluded._batch_id
            """,
            [(r.rate_date, r.base, r.quote, r.rate, sim_now, batch_id) for r in rates],
        )
    return len(rates)


def sync_fx(
    conn: psycopg.Connection,
    client: FxClient,
    through: date,
    sim_now: datetime,
    batch_id: str,
) -> int:
    """Fetch rates for ``watermark+1 .. through``, upsert them and advance the watermark.

    Returns the number of rates written; 0 (without calling the client) if already caught up.
    """
    watermark = get_watermark(conn)
    if watermark is None:
        raise RuntimeError("control.fx_sync watermark is unset; run seed first")
    if watermark >= through:
        return 0
    rates = client.rates(watermark + timedelta(days=1), through)
    with conn.transaction():
        written = upsert_rates(conn, rates, sim_now, batch_id)
        set_watermark(conn, through)
    log.info("fx_synced", through=through.isoformat(), rates=written, batch_id=batch_id)
    return written


def sync_fx_range(
    conn: psycopg.Connection,
    client: FxClient,
    start: date,
    end: date,
    sim_now: datetime,
    batch_id: str,
) -> int:
    """Backfill ``start .. end`` inclusive; does not move the watermark."""
    return upsert_rates(conn, client.rates(start, end), sim_now, batch_id)
