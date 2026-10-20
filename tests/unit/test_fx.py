from datetime import date
from decimal import Decimal
from pathlib import Path

import httpx
import pytest

from drct.common.config import Settings
from drct.ingestion.fx import (
    FrankfurterClient,
    FxRate,
    StubFxClient,
    make_fx_client,
)

D = date(2026, 5, 1)


def _settings(fx_source: str) -> Settings:
    return Settings(
        warehouse_dsn="postgresql://x",
        landing_dir=Path("/tmp/landing"),
        seed=42,
        fx_source=fx_source,  # type: ignore[arg-type]
        fx_api_url="https://fx.example/v1",
        airflow_api_url="http://airflow.invalid",
        dbt_project_dir=Path("/tmp/dbt"),
    )


def test_frankfurter_parses_timeseries():
    transport = httpx.MockTransport(
        lambda req: httpx.Response(
            200,
            json={"base": "GBP", "rates": {"2026-05-01": {"EUR": 1.17, "USD": 1.27}}},
        )
    )
    rates = FrankfurterClient("https://x", transport=transport).rates(D, D)
    assert FxRate(D, "GBP", "EUR", Decimal("1.17")) in rates
    assert FxRate(D, "GBP", "USD", Decimal("1.27")) in rates
    assert len(rates) == 2


def test_frankfurter_rates_are_exact_decimals():
    body = '{"base":"GBP","rates":{"2026-05-01":{"EUR":1.1700000001,"USD":1.27}}}'
    transport = httpx.MockTransport(lambda req: httpx.Response(200, text=body))
    rates = FrankfurterClient("https://x", transport=transport).rates(D, D)
    assert all(isinstance(r.rate, Decimal) for r in rates)
    assert FxRate(D, "GBP", "EUR", Decimal("1.1700000001")) in rates


def test_frankfurter_request_shape():
    seen: list[httpx.Request] = []

    def handler(req: httpx.Request) -> httpx.Response:
        seen.append(req)
        return httpx.Response(200, json={"base": "GBP", "rates": {}})

    client = FrankfurterClient("https://x/v1/", transport=httpx.MockTransport(handler))
    assert client.rates(date(2026, 5, 1), date(2026, 5, 7)) == []
    (req,) = seen
    assert req.method == "GET"
    assert req.url.path == "/v1/2026-05-01..2026-05-07"
    assert req.url.params["base"] == "GBP"
    assert req.url.params["symbols"] == "EUR,USD"


def test_frankfurter_raises_on_500():
    transport = httpx.MockTransport(lambda req: httpx.Response(500))
    with pytest.raises(httpx.HTTPStatusError):
        FrankfurterClient("https://x", transport=transport).rates(D, D)


def test_stub_has_weekend_gaps():
    rates = StubFxClient().rates(date(2026, 5, 1), date(2026, 5, 7))
    assert {r.rate_date.weekday() for r in rates} <= {0, 1, 2, 3, 4}


def test_stub_rates_and_base():
    rates = StubFxClient().rates(date(2026, 5, 1), date(2026, 5, 1))  # a Friday
    assert set(rates) == {
        FxRate(D, "GBP", "EUR", Decimal("1.17")),
        FxRate(D, "GBP", "USD", Decimal("1.27")),
    }


def test_make_fx_client_selects_by_source():
    assert isinstance(make_fx_client(_settings("stub")), StubFxClient)
    client = make_fx_client(_settings("api"))
    assert isinstance(client, FrankfurterClient)
