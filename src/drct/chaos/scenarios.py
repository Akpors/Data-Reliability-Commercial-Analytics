"""Chaos scenarios: realistic corruption of one simulated day's source data.

Only the generator (``simulate_day``) and the FX task use this module. Detectors never do: they
must catch the injected failures generically.
"""

import zlib
from collections.abc import Callable

import numpy as np
import pandas as pd

from drct.generator.daily import DayBatch

SCENARIOS = (
    "duplicates",
    "missing_dates",
    "broken_fk",
    "schema_change",
    "negative_revenue",
    "stale_data",
    "volume_spike",
    "api_flaky",
)

DEFAULT_PARAMS: dict[str, dict] = {
    "duplicates": {"rate": 0.05},
    "missing_dates": {"rate": 0.03},
    "broken_fk": {"rate": 0.02},
    "schema_change": {"mode": "rename", "column": "unit_price", "to": "price"},
    "negative_revenue": {"rate": 0.02},
    "stale_data": {"source": "payments"},
    "volume_spike": {"multiplier": 8},
    "api_flaky": {"fail_attempts": 2},
}
# ``schema_change`` in add mode: a new, unannounced column with a constant value.
SCHEMA_ADD_DEFAULTS = {"mode": "add", "column": "discount_code", "value": "WELCOME10"}
SCHEMA_MODES = ("rename", "add")

Mutation = Callable[[DayBatch, dict, np.random.Generator], DayBatch]


def _count(n_rows: int, rate: float) -> int:
    """Rows to corrupt: ``rate`` of ``n_rows``, at least one so a positive rate always shows."""
    if n_rows == 0 or rate <= 0:
        return 0
    return min(n_rows, max(1, round(n_rows * rate)))


def _pick(rng: np.random.Generator, index: pd.Index, rate: float) -> pd.Index:
    return pd.Index(rng.choice(index, size=_count(len(index), rate), replace=False))


def _volume_spike(batch: DayBatch, params: dict, rng: np.random.Generator) -> DayBatch:
    """Multiply the day's orders (with new ids, payments still matched), or sample them."""
    multiplier = float(params["multiplier"])
    sales, payments = batch.sales, batch.payments
    if multiplier < 1:
        orders = sales["order_id"].drop_duplicates().to_numpy()
        keep = set(rng.choice(orders, size=round(len(orders) * multiplier), replace=False))
        return DayBatch(
            batch.business_date,
            sales[sales["order_id"].isin(keep)].reset_index(drop=True),
            payments[payments["order_id"].isin(keep)].reset_index(drop=True),
        )
    sales_copies, payment_copies = [sales], [payments]
    # order_line_id is "<order_id>-L<n>": copies keep the "-L<n>" tail under the new order id.
    line_tails = sales["order_line_id"].str.extract(r"(-L\d+)$", expand=False)
    for k in range(1, round(multiplier)):
        suffix = f"-R{k}"
        s = sales.copy()
        s["order_id"] = s["order_id"] + suffix
        s["order_line_id"] = s["order_id"] + line_tails
        p = payments.copy()
        p["order_id"] = p["order_id"] + suffix
        p["payment_id"] = p["payment_id"] + suffix
        sales_copies.append(s)
        payment_copies.append(p)
    return DayBatch(
        batch.business_date,
        pd.concat(sales_copies, ignore_index=True),
        pd.concat(payment_copies, ignore_index=True),
    )


def _duplicates(batch: DayBatch, params: dict, rng: np.random.Generator) -> DayBatch:
    """Re-emit some payment rows as exact copies, as a source retrying a send would."""
    payments = batch.payments
    copies = payments.loc[_pick(rng, payments.index, float(params["rate"]))]
    resent = pd.concat([payments, copies]).sort_values("payment_ts", kind="stable")
    return DayBatch(batch.business_date, batch.sales, resent.reset_index(drop=True))


def _missing_dates(batch: DayBatch, params: dict, rng: np.random.Generator) -> DayBatch:
    sales = batch.sales.copy()
    sales.loc[_pick(rng, sales.index, float(params["rate"])), "order_date"] = None
    return DayBatch(batch.business_date, sales, batch.payments)


def _broken_fk(batch: DayBatch, params: dict, rng: np.random.Generator) -> DayBatch:
    """Point some lines at product ids ``P-9xxxx``, far beyond the catalogue (P-00001..)."""
    sales = batch.sales.copy()
    rows = _pick(rng, sales.index, float(params["rate"]))
    sales.loc[rows, "product_id"] = [f"P-{n}" for n in rng.integers(90_000, 100_000, len(rows))]
    return DayBatch(batch.business_date, sales, batch.payments)


def _negative_revenue(batch: DayBatch, params: dict, rng: np.random.Generator) -> DayBatch:
    """Negate ``unit_price`` on some sale lines (refund lines are negative by quantity already)."""
    sales = batch.sales.copy()
    sale_lines = sales.index[sales["line_type"] == "sale"]
    rows = _pick(rng, sale_lines, float(params["rate"]))
    sales.loc[rows, "unit_price"] = "-" + sales.loc[rows, "unit_price"]
    return DayBatch(batch.business_date, sales, batch.payments)


def _schema_change(batch: DayBatch, params: dict, rng: np.random.Generator) -> DayBatch:
    """Change the sales file's shape: rename a column (breaking) or add one (additive)."""
    mode = params.get("mode")
    if mode == "rename":
        sales = batch.sales.rename(columns={params["column"]: params["to"]})
    elif mode == "add":
        sales = batch.sales.copy()
        sales[params["column"]] = params.get("value", SCHEMA_ADD_DEFAULTS["value"])
    else:
        raise ValueError(f"schema_change mode must be one of {SCHEMA_MODES}, got {mode!r}")
    return DayBatch(batch.business_date, sales, batch.payments)


# Application order: volume first (it changes the row set), the shape change last (it can rename a
# column that the row-level corruptions write to).
_MUTATIONS: dict[str, Mutation] = {
    "volume_spike": _volume_spike,
    "duplicates": _duplicates,
    "missing_dates": _missing_dates,
    "broken_fk": _broken_fk,
    "negative_revenue": _negative_revenue,
    "schema_change": _schema_change,
}


def _rng(seed: int, batch: DayBatch, scenario: str) -> np.random.Generator:
    return np.random.default_rng(
        [seed, batch.business_date.toordinal(), zlib.crc32(scenario.encode())]
    )


def apply_scenarios(batch: DayBatch, active: dict[str, dict], seed: int) -> DayBatch:
    """Apply the active data scenarios to ``batch``; deterministic, and ``batch`` is not modified.

    ``stale_data`` and ``api_flaky`` are not data mutations and are ignored here.
    """
    for scenario, mutate in _MUTATIONS.items():
        if scenario in active:
            batch = mutate(batch, active[scenario], _rng(seed, batch, scenario))
    return batch


def suppressed_sources(active: dict[str, dict]) -> set[str]:
    """Sources whose files are not written at all while ``stale_data`` is active."""
    if "stale_data" not in active:
        return set()
    return {active["stale_data"].get("source", DEFAULT_PARAMS["stale_data"]["source"])}


def fx_should_fail(active: dict, try_number: int) -> bool:
    """True while ``api_flaky`` is active and ``try_number`` is within its ``fail_attempts``."""
    if "api_flaky" not in active:
        return False
    fail_attempts = active["api_flaky"].get(
        "fail_attempts", DEFAULT_PARAMS["api_flaky"]["fail_attempts"]
    )
    return try_number <= int(fail_attempts)
