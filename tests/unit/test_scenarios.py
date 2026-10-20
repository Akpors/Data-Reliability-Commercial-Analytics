from datetime import date

import pandas as pd
import pytest

from drct.chaos.scenarios import (
    DEFAULT_PARAMS,
    SCENARIOS,
    apply_scenarios,
    fx_should_fail,
    suppressed_sources,
)
from drct.generator.daily import build_day
from drct.generator.entities import build_customers, build_products

D = date(2026, 5, 6)
PRODUCTS = build_products(42)
CUSTOMERS = build_customers(42, as_of=D)
BATCH = build_day(D, 42, CUSTOMERS, PRODUCTS)


def test_duplicates_repeat_existing_payment_ids():
    out = apply_scenarios(BATCH, {"duplicates": {"rate": 0.05}}, 42)
    assert out.payments["payment_id"].duplicated().sum() == round(len(BATCH.payments) * 0.05)


def test_duplicates_are_exact_copies_of_existing_rows():
    out = apply_scenarios(BATCH, {"duplicates": {"rate": 0.05}}, 42)
    originals = set(map(tuple, BATCH.payments.to_numpy()))
    assert set(map(tuple, out.payments.to_numpy())) == originals
    assert list(out.payments.columns) == list(BATCH.payments.columns)
    pd.testing.assert_frame_equal(out.sales, BATCH.sales)


def test_broken_fk_ids_not_in_catalogue():
    out = apply_scenarios(BATCH, {"broken_fk": {"rate": 0.02}}, 42)
    assert (~out.sales["product_id"].isin(PRODUCTS["product_id"])).sum() > 0


def test_broken_fk_uses_p9_ids_on_about_rate_of_lines():
    out = apply_scenarios(BATCH, {"broken_fk": {"rate": 0.02}}, 42)
    broken = out.sales.loc[~out.sales["product_id"].isin(PRODUCTS["product_id"]), "product_id"]
    assert len(broken) == round(len(BATCH.sales) * 0.02)
    assert broken.str.fullmatch(r"P-9\d{4}").all()


def test_schema_change_rename():
    out = apply_scenarios(BATCH, {"schema_change": DEFAULT_PARAMS["schema_change"]}, 42)
    assert "price" in out.sales.columns and "unit_price" not in out.sales.columns


def test_schema_change_add_appends_constant_column_to_sales_only():
    params = {"mode": "add", "column": "discount_code"}
    out = apply_scenarios(BATCH, {"schema_change": params}, 42)
    assert list(out.sales.columns) == [*BATCH.sales.columns, "discount_code"]
    assert (out.sales["discount_code"] == "WELCOME10").all()
    pd.testing.assert_frame_equal(out.payments, BATCH.payments)


def test_schema_change_unknown_mode_raises():
    with pytest.raises(ValueError, match="mode"):
        apply_scenarios(BATCH, {"schema_change": {"mode": "drop", "column": "x"}}, 42)


def test_negative_revenue_negates_price_on_sale_lines_only():
    out = apply_scenarios(BATCH, {"negative_revenue": {"rate": 0.02}}, 42)
    negative = out.sales["unit_price"].str.startswith("-")
    assert negative.sum() == round(len(BATCH.sales) * 0.02)
    assert (out.sales.loc[negative, "line_type"] == "sale").all()
    flipped = BATCH.sales.loc[negative, "unit_price"]
    assert (out.sales.loc[negative, "unit_price"] == "-" + flipped).all()


def test_missing_dates_nulls_order_date_on_some_lines():
    out = apply_scenarios(BATCH, {"missing_dates": {"rate": 0.03}}, 42)
    assert out.sales["order_date"].isna().sum() == round(len(BATCH.sales) * 0.03)
    assert len(out.sales) == len(BATCH.sales)


def test_volume_spike_multiplies_rows_with_unique_ids():
    out = apply_scenarios(BATCH, {"volume_spike": {"multiplier": 8}}, 42)
    assert len(out.sales) == 8 * len(BATCH.sales) and out.sales["order_line_id"].is_unique


def test_volume_spike_keeps_orders_and_payments_linked():
    out = apply_scenarios(BATCH, {"volume_spike": {"multiplier": 8}}, 42)
    assert len(out.payments) == 8 * len(BATCH.payments)
    assert out.payments["payment_id"].is_unique
    assert set(out.payments["order_id"]) == set(out.sales["order_id"])
    assert out.sales["order_id"].nunique() == 8 * BATCH.sales["order_id"].nunique()
    # Each line id still starts with its own order id.
    assert all(
        line.startswith(f"{order}-L")
        for order, line in zip(out.sales["order_id"], out.sales["order_line_id"], strict=True)
    )


def test_volume_spike_below_one_samples_whole_orders():
    out = apply_scenarios(BATCH, {"volume_spike": {"multiplier": 0.1}}, 42)
    n_orders = BATCH.sales["order_id"].nunique()
    assert out.sales["order_id"].nunique() == round(n_orders * 0.1)
    assert set(out.payments["order_id"]) == set(out.sales["order_id"])
    assert set(out.sales["order_line_id"]) <= set(BATCH.sales["order_line_id"])


def test_no_scenarios_is_identity():
    pd.testing.assert_frame_equal(apply_scenarios(BATCH, {}, 42).sales, BATCH.sales)


def test_non_data_scenarios_leave_frames_alone():
    out = apply_scenarios(BATCH, {"stale_data": {"source": "payments"}, "api_flaky": {}}, 42)
    pd.testing.assert_frame_equal(out.sales, BATCH.sales)
    pd.testing.assert_frame_equal(out.payments, BATCH.payments)


def test_apply_is_deterministic_and_does_not_mutate_input():
    before = BATCH.sales.copy()
    chaos = {name: DEFAULT_PARAMS[name] for name in ("duplicates", "broken_fk", "missing_dates")}
    first = apply_scenarios(BATCH, chaos, 42)
    second = apply_scenarios(BATCH, chaos, 42)
    pd.testing.assert_frame_equal(first.sales, second.sales)
    pd.testing.assert_frame_equal(first.payments, second.payments)
    pd.testing.assert_frame_equal(BATCH.sales, before)
    other_seed = apply_scenarios(BATCH, chaos, 7)
    assert not other_seed.sales.equals(first.sales)


def test_all_scenarios_have_defaults():
    assert set(DEFAULT_PARAMS) == set(SCENARIOS)


def test_suppressed_sources_from_stale_data():
    assert suppressed_sources({}) == set()
    assert suppressed_sources({"stale_data": {"source": "payments"}}) == {"payments"}
    assert suppressed_sources({"stale_data": {"source": "customers"}}) == {"customers"}


def test_fx_flaky_fails_then_recovers():
    assert fx_should_fail({"api_flaky": {"fail_attempts": 2}}, 2) and not fx_should_fail(
        {"api_flaky": {"fail_attempts": 2}}, 3
    )


def test_fx_not_flaky_without_scenario():
    assert not fx_should_fail({}, 1)
    assert not fx_should_fail({"duplicates": {"rate": 0.05}}, 1)
