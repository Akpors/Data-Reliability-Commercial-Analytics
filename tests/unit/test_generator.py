from datetime import date

import pandas as pd
import pytest

from drct.generator.daily import DayBatch, build_day
from drct.generator.entities import build_customers, build_products

D = date(2026, 5, 6)
PRODUCTS = build_products(42)
CUSTOMERS = build_customers(42, as_of=D)

PRODUCT_COLUMNS = ["product_id", "sku", "product_name", "category", "list_price_gbp", "is_active"]
CUSTOMER_COLUMNS = ["customer_id", "email", "full_name", "country", "signup_date", "segment"]
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


@pytest.fixture(scope="module")
def batch() -> DayBatch:
    return build_day(D, 42, CUSTOMERS, PRODUCTS)


def test_build_day_is_deterministic():
    a = build_day(D, 42, CUSTOMERS, PRODUCTS)
    b = build_day(D, 42, CUSTOMERS, PRODUCTS)
    pd.testing.assert_frame_equal(a.sales, b.sales)
    pd.testing.assert_frame_equal(a.payments, b.payments)


def test_sales_are_clean_by_construction():
    s = build_day(D, 42, CUSTOMERS, PRODUCTS).sales
    assert s["order_line_id"].is_unique
    assert s["order_date"].notna().all() and (s["order_date"] == D.isoformat()).all()
    assert set(s["product_id"]) <= set(PRODUCTS["product_id"])
    assert set(s["customer_id"]) <= set(CUSTOMERS["customer_id"])
    sale = s[s["line_type"] == "sale"]
    assert (sale["quantity"].astype(int) > 0).all() and (
        sale["unit_price"].astype(float) >= 0
    ).all()
    assert set(s["currency"]) <= {"GBP", "EUR", "USD"}


def test_one_captured_payment_per_order_and_unique_ids():
    batch = build_day(D, 42, CUSTOMERS, PRODUCTS)
    assert batch.payments["payment_id"].is_unique
    captured = batch.payments[batch.payments["status"] == "captured"]
    assert set(captured["order_id"]) == set(batch.sales["order_id"])


def test_weekly_seasonality_saturday_busier_than_monday():
    sat, mon = date(2026, 5, 2), date(2026, 5, 4)
    assert build_day(sat, 42, CUSTOMERS, PRODUCTS).sales["order_id"].nunique() > (
        build_day(mon, 42, CUSTOMERS, PRODUCTS).sales["order_id"].nunique()
    )


def test_products_match_raw_columns_and_id_format():
    assert list(PRODUCTS.columns) == PRODUCT_COLUMNS
    assert len(PRODUCTS) == 150
    assert PRODUCTS["product_id"].iloc[0] == "P-00001"
    assert PRODUCTS["product_id"].is_unique and PRODUCTS["sku"].is_unique
    assert (PRODUCTS["list_price_gbp"].astype(float) > 0).all()
    assert set(PRODUCTS["is_active"]) <= {"true", "false"}
    pd.testing.assert_frame_equal(PRODUCTS, build_products(42))


def test_customers_match_raw_columns_and_are_filtered_by_as_of():
    assert list(CUSTOMERS.columns) == CUSTOMER_COLUMNS
    assert CUSTOMERS["customer_id"].is_unique
    assert CUSTOMERS["customer_id"].str.fullmatch(r"C-\d{6}").all()
    assert (CUSTOMERS["signup_date"] <= D.isoformat()).all()
    assert CUSTOMERS["email"].is_unique


def test_customer_population_depends_on_seed_only_so_earlier_as_of_is_a_subset():
    early = build_customers(42, as_of=date(2026, 1, 1))
    late = build_customers(42, as_of=date(2026, 12, 31))
    assert 0 < len(early) < len(late) <= 3000
    merged = early.merge(late, on="customer_id", suffixes=("_e", "_l"))
    assert len(merged) == len(early)
    for col in CUSTOMER_COLUMNS[1:]:
        assert (merged[f"{col}_e"] == merged[f"{col}_l"]).all()
    assert len(build_customers(42, as_of=date(2027, 12, 31))) == 3000


def test_different_seeds_give_different_data():
    assert not build_products(1).equals(build_products(2))
    a = build_day(D, 1, CUSTOMERS, PRODUCTS).sales
    b = build_day(D, 2, CUSTOMERS, PRODUCTS).sales
    assert not a.equals(b)


def test_frames_have_raw_columns_and_string_values(batch):
    assert list(batch.sales.columns) == SALES_COLUMNS
    assert list(batch.payments.columns) == PAYMENT_COLUMNS
    assert batch.business_date == D
    for frame in (batch.sales, batch.payments, PRODUCTS, CUSTOMERS):
        for col in frame.columns:
            assert frame[col].map(lambda v: isinstance(v, str)).all(), col


def test_order_volume_is_around_the_weekday_base(batch):
    # Wednesday: base 300 x 1.0 x (0.9..1.1)
    assert 270 <= batch.sales["order_id"].nunique() <= 330
    lines_per_order = batch.sales.groupby("order_id").size()
    assert lines_per_order.between(1, 4).all()


def test_timestamps_are_utc_and_within_business_date(batch):
    for col, frame in (("order_ts", batch.sales), ("payment_ts", batch.payments)):
        ts = pd.to_datetime(frame[col], utc=True, format="ISO8601")
        assert (ts.dt.strftime("%Y-%m-%d") == D.isoformat()).all()
        assert frame[col].str.endswith("+00:00").all()


def test_refund_lines_have_negative_quantity_and_positive_price():
    s = pd.concat(build_day(date(2026, 5, d), 42, CUSTOMERS, PRODUCTS).sales for d in range(1, 8))
    refunds = s[s["line_type"] == "refund"]
    assert set(s["line_type"]) == {"sale", "refund"}
    assert (refunds["quantity"].astype(int) < 0).all()
    assert (refunds["unit_price"].astype(float) > 0).all()
    assert 0.01 < len(refunds) / len(s) < 0.06
    assert set(s["channel"]) == {"web", "app"}


def test_non_gbp_prices_apply_fx_multiplier_to_list_price(batch):
    s = batch.sales.merge(PRODUCTS[["product_id", "list_price_gbp"]], on="product_id")
    for currency, fx in (("GBP", 1.0), ("EUR", 1.17), ("USD", 1.27)):
        rows = s[s["currency"] == currency]
        assert len(rows) > 0
        expected = (rows["list_price_gbp"].astype(float) * fx).round(2)
        assert ((rows["unit_price"].astype(float) - expected).abs() < 0.0101).all()


def test_currency_is_constant_within_an_order_and_matches_payment(batch):
    assert (batch.sales.groupby("order_id")["currency"].nunique() == 1).all()
    order_ccy = batch.sales.groupby("order_id")["currency"].first()
    pay_ccy = batch.payments.groupby("order_id")["currency"].first()
    assert (order_ccy == pay_ccy.reindex(order_ccy.index)).all()


def test_every_order_has_a_sale_line_and_first_line_is_a_sale():
    s = pd.concat(build_day(date(2026, 5, d), 42, CUSTOMERS, PRODUCTS).sales for d in range(1, 8))
    assert (s.groupby("order_id")["line_type"].apply(lambda t: (t == "sale").any())).all()
    first = s[s["order_line_id"].str.endswith("-L1")]
    assert (first["line_type"] == "sale").all()


def test_captured_amount_is_positive_and_sums_sale_lines_only():
    sales, payments = [], []
    for d in range(1, 8):
        b = build_day(date(2026, 5, d), 42, CUSTOMERS, PRODUCTS)
        sales.append(b.sales)
        payments.append(b.payments)
    sales, payments = pd.concat(sales), pd.concat(payments)
    assert (sales["line_type"] == "refund").any()
    sale = sales[sales["line_type"] == "sale"]
    total = sale["quantity"].astype(int) * sale["unit_price"].astype(float)
    expected = total.groupby(sale["order_id"]).sum().round(2)
    captured = payments[payments["status"] == "captured"].set_index("order_id")
    assert len(captured) == len(expected)
    assert (captured["amount"].astype(float) > 0).all()
    diff = captured["amount"].astype(float) - expected.reindex(captured.index)
    assert (diff.abs() < 0.005).all()
    assert captured["amount"].str.fullmatch(r"\d+\.\d{2}").all()
    failed = payments[payments["status"] == "failed"].set_index("order_id")
    assert len(failed) > 0
    assert (failed["amount"] == captured["amount"].reindex(failed.index)).all()


def test_failed_payments_precede_a_captured_one_and_are_about_two_percent():
    failed_share = []
    for d in range(1, 8):
        b = build_day(date(2026, 5, d), 42, CUSTOMERS, PRODUCTS)
        p = b.payments
        assert set(p["status"]) <= {"captured", "failed"}
        assert set(p["method"]) <= {"card", "paypal", "apple_pay"}
        assert p.groupby("order_id").size().isin([1, 2]).all()
        failed = p[p["status"] == "failed"]
        for order_id in failed["order_id"]:
            rows = p[p["order_id"] == order_id].sort_values("payment_ts")
            assert list(rows["status"]) == ["failed", "captured"]
        failed_share.append(len(failed) / p["order_id"].nunique())
    assert 0.005 < sum(failed_share) / len(failed_share) < 0.04


def test_no_orders_before_customer_signup():
    early = build_customers(42, as_of=date(2026, 5, 6))
    s = build_day(date(2026, 1, 15), 42, early, PRODUCTS).sales
    signup = early.set_index("customer_id")["signup_date"]
    assert (s["customer_id"].map(signup) <= "2026-01-15").all()
