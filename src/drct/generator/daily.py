"""One business day of sales lines and payments, clean by construction."""

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal

import numpy as np
import pandas as pd

BASE_ORDERS_PER_DAY = 300
WEEKDAY_MULTIPLIERS = [0.9, 0.95, 1.0, 1.0, 1.1, 1.3, 1.2]  # Monday..Sunday
NOISE = 0.10
MAX_LINES_PER_ORDER = 4
CURRENCIES = ["GBP", "EUR", "USD"]
CURRENCY_WEIGHTS = [0.70, 0.20, 0.10]
FX_FROM_GBP = {"GBP": Decimal(1), "EUR": Decimal("1.17"), "USD": Decimal("1.27")}
REFUND_LINE_SHARE = 0.03  # overall share of lines
# The first line of an order is always a sale, so refunds are drawn for later lines only, at a
# higher rate that keeps the overall share of refund lines at REFUND_LINE_SHARE.
_MEAN_LINES = (1 + MAX_LINES_PER_ORDER) / 2
_LATER_LINE_REFUND_PROB = REFUND_LINE_SHARE * _MEAN_LINES / (_MEAN_LINES - 1)
FAILED_THEN_CAPTURED_SHARE = 0.02
CHANNELS = ["web", "app"]
METHODS = ["card", "paypal", "apple_pay"]
# Orders start early enough that their payments still land inside the business date.
_LAST_ORDER_SECOND = 86_400 - 900

SALES_COLUMNS = [
    "order_id",
    "order_line_id",
    "order_date",
    "order_ts",
    "customer_id",
    "product_id",
    "quantity",
    "unit_price",
    "currency",
    "line_type",
    "channel",
]
PAYMENT_COLUMNS = ["payment_id", "order_id", "payment_ts", "amount", "currency", "method", "status"]


@dataclass
class DayBatch:
    business_date: date
    sales: pd.DataFrame
    payments: pd.DataFrame


def _cents(value: Decimal) -> int:
    return int((value * 100).quantize(Decimal(1), rounding=ROUND_HALF_UP))


def _money(cents: int) -> str:
    sign = "-" if cents < 0 else ""
    whole, frac = divmod(abs(cents), 100)
    return f"{sign}{whole}.{frac:02d}"


def build_day(
    business_date: date, seed: int, customers: pd.DataFrame, products: pd.DataFrame
) -> DayBatch:
    """Build the sales lines and payments for ``business_date``; deterministic in its arguments.

    Only customers who had signed up by ``business_date`` and active products are used.

    The first line of every order is a sale, and payment amounts cover the sale lines only
    (refund lines are excluded), so every captured amount is positive.
    """
    rng = np.random.default_rng([seed, business_date.toordinal()])
    day = business_date.isoformat()
    midnight = datetime(business_date.year, business_date.month, business_date.day, tzinfo=UTC)

    def ts(seconds: int) -> str:
        return (midnight + timedelta(seconds=seconds)).isoformat()

    eligible_ids = customers.loc[customers["signup_date"] <= day, "customer_id"].to_numpy()
    active = products[products["is_active"] == "true"]
    product_ids = active["product_id"].to_numpy()
    list_prices = [Decimal(p) for p in active["list_price_gbp"]]

    n_orders = 0
    if len(eligible_ids) > 0 and len(product_ids) > 0:
        multiplier = WEEKDAY_MULTIPLIERS[business_date.weekday()]
        n_orders = round(BASE_ORDERS_PER_DAY * multiplier * rng.uniform(1 - NOISE, 1 + NOISE))

    order_seconds = np.sort(rng.integers(0, _LAST_ORDER_SECOND, size=n_orders))
    sales_rows: list[dict[str, str]] = []
    payment_rows: list[dict[str, str]] = []
    for i, second in enumerate(order_seconds, start=1):
        order_id = f"O-{business_date:%Y%m%d}-{i:05d}"
        order_ts = ts(int(second))
        customer_id = str(rng.choice(eligible_ids))
        currency = str(rng.choice(CURRENCIES, p=CURRENCY_WEIGHTS))
        channel = str(rng.choice(CHANNELS))
        n_lines = int(rng.integers(1, MAX_LINES_PER_ORDER + 1))
        picks = rng.choice(len(product_ids), size=n_lines, replace=False)
        order_cents = 0
        for line_no, pick in enumerate(picks, start=1):
            quantity = int(rng.integers(1, 6))
            line_type = "sale"
            if line_no > 1 and rng.random() < _LATER_LINE_REFUND_PROB:
                line_type, quantity = "refund", -quantity
            unit_cents = _cents(list_prices[pick] * FX_FROM_GBP[currency])
            if line_type == "sale":
                order_cents += quantity * unit_cents
            sales_rows.append(
                {
                    "order_id": order_id,
                    "order_line_id": f"{order_id}-L{line_no}",
                    "order_date": day,
                    "order_ts": order_ts,
                    "customer_id": customer_id,
                    "product_id": str(product_ids[pick]),
                    "quantity": str(quantity),
                    "unit_price": _money(unit_cents),
                    "currency": currency,
                    "line_type": line_type,
                    "channel": channel,
                }
            )

        method = str(rng.choice(METHODS))
        attempt_second = int(second) + int(rng.integers(5, 121))
        if rng.random() < FAILED_THEN_CAPTURED_SHARE:
            payment_rows.append(
                _payment(order_id, ts(attempt_second), order_cents, currency, method, "failed")
            )
            attempt_second += int(rng.integers(30, 301))
        payment_rows.append(
            _payment(order_id, ts(attempt_second), order_cents, currency, method, "captured")
        )

    for n, row in enumerate(payment_rows, start=1):
        row["payment_id"] = f"PAY-{business_date:%Y%m%d}-{n:05d}"
    return DayBatch(
        business_date=business_date,
        sales=pd.DataFrame(sales_rows, columns=SALES_COLUMNS),
        payments=pd.DataFrame(payment_rows, columns=PAYMENT_COLUMNS),
    )


def _payment(
    order_id: str, payment_ts: str, cents: int, currency: str, method: str, status: str
) -> dict[str, str]:
    return {
        "order_id": order_id,
        "payment_ts": payment_ts,
        "amount": _money(cents),
        "currency": currency,
        "method": method,
        "status": status,
    }
