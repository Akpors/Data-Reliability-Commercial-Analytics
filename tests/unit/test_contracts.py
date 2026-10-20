import dataclasses
import json
import re
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from drct.generator.daily import build_day
from drct.generator.entities import build_customers, build_products
from drct.ingestion.contracts import (
    DEFAULT_DIR,
    Contract,
    canonicalise,
    diff_schema,
    load_contract,
)

D = date(2026, 5, 6)
PRODUCTS = build_products(42)
CUSTOMERS = build_customers(42, as_of=D)
SALES = load_contract("sales")
RAW_INIT_SQL = Path(__file__).resolve().parents[2] / "warehouse" / "init" / "03_raw.sql"

NON_NULLABLE = {
    "sales": {
        "order_id",
        "order_line_id",
        "product_id",
        "customer_id",
        "unit_price",
        "currency",
        "line_type",
    },
    "payments": {"payment_id", "order_id", "amount"},
    "customers": {"customer_id"},
    "products": {"product_id"},
}


def clean_sales_df() -> pd.DataFrame:
    return build_day(D, 42, CUSTOMERS, PRODUCTS).sales


def clean_frames() -> dict[str, pd.DataFrame]:
    batch = build_day(D, 42, CUSTOMERS, PRODUCTS)
    return {
        "sales": batch.sales,
        "payments": batch.payments,
        "customers": CUSTOMERS,
        "products": PRODUCTS,
    }


def replace_column(contract: Contract, name: str, **changes) -> Contract:
    """A new contract with one column's attributes replaced (e.g. its aliases)."""
    columns = tuple(
        dataclasses.replace(c, **changes) if c.name == name else c for c in contract.columns
    )
    return dataclasses.replace(contract, columns=columns)


def test_clean_frame_has_clean_diff():
    assert diff_schema(SALES, clean_sales_df()).is_clean


def test_renamed_column_is_breaking():
    d = diff_schema(SALES, clean_sales_df().rename(columns={"unit_price": "price"}))
    assert d.missing == ["unit_price"] and d.added == ["price"] and d.is_breaking


def test_added_column_is_not_breaking():
    d = diff_schema(SALES, clean_sales_df().assign(discount_code="X"))
    assert d.added == ["discount_code"] and not d.is_breaking
    assert not d.is_clean


def test_type_change_is_breaking():
    d = diff_schema(SALES, clean_sales_df().assign(unit_price="£12.00"))
    assert "unit_price" in d.type_mismatches and d.is_breaking


def test_alias_accepts_renamed_column_and_canonicalises():
    accepted = replace_column(SALES, "unit_price", aliases=("price",))
    df = clean_sales_df().rename(columns={"unit_price": "price"})
    assert diff_schema(accepted, df).aliased == {"price": "unit_price"}
    assert not diff_schema(accepted, df).is_breaking
    assert "unit_price" in canonicalise(accepted, df).columns


def test_aliased_column_is_neither_missing_nor_added():
    accepted = replace_column(SALES, "unit_price", aliases=("price",))
    d = diff_schema(accepted, clean_sales_df().rename(columns={"unit_price": "price"}))
    assert d.missing == [] and d.added == []
    assert not d.is_clean


def test_aliased_column_is_type_checked_under_its_canonical_name():
    accepted = replace_column(SALES, "unit_price", aliases=("price",))
    df = clean_sales_df().rename(columns={"unit_price": "price"}).assign(price="£12.00")
    d = diff_schema(accepted, df)
    assert d.type_mismatches == {"unit_price": "£12.00"} and d.is_breaking


def test_diff_lists_are_sorted():
    df = clean_sales_df().drop(columns=["quantity", "channel"]).assign(zzz="1", aaa="2")
    d = diff_schema(SALES, df)
    assert d.missing == ["channel", "quantity"]
    assert d.added == ["aaa", "zzz"]


def test_type_mismatch_reports_first_bad_value():
    df = clean_sales_df()
    df["quantity"] = df["quantity"].astype(object)
    df.loc[3, "quantity"] = "two"
    df.loc[5, "quantity"] = "three"
    assert diff_schema(SALES, df).type_mismatches == {"quantity": "two"}


@pytest.mark.parametrize(
    ("column", "bad"),
    [
        ("order_date", "06/05/2026"),
        ("order_ts", "yesterday"),
        ("quantity", "1.5"),
        ("unit_price", "NaN"),
    ],
)
def test_each_dtype_rejects_malformed_values(column, bad):
    d = diff_schema(SALES, clean_sales_df().assign(**{column: bad}))
    assert d.type_mismatches == {column: bad}


def test_bool_dtype_is_case_insensitive_and_rejects_other_words():
    products = load_contract("products")
    ok = PRODUCTS.assign(is_active="TRUE")
    assert diff_schema(products, ok).is_clean
    d = diff_schema(products, PRODUCTS.assign(is_active="yes"))
    assert d.type_mismatches == {"is_active": "yes"}


def test_null_values_are_not_type_checked():
    df = clean_sales_df()
    df["order_date"] = df["order_date"].astype(object)
    df.loc[0, "order_date"] = None
    df.loc[1, "unit_price"] = float("nan")
    assert diff_schema(SALES, df).is_clean


def test_canonicalise_orders_columns_and_packs_extra_as_json():
    df = clean_sales_df()
    df = df[list(reversed(df.columns))].assign(discount_code="X")
    out = canonicalise(SALES, df)
    assert list(out.columns) == [c.name for c in SALES.columns] + ["_extra"]
    assert json.loads(out["_extra"].iloc[0]) == {"discount_code": "X"}
    assert out["order_id"].tolist() == clean_sales_df()["order_id"].tolist()


def test_canonicalise_extra_is_none_without_unknown_columns():
    out = canonicalise(SALES, clean_sales_df())
    assert out["_extra"].isna().all()
    assert all(v is None for v in out["_extra"])


def test_canonicalise_does_not_mutate_its_input():
    df = clean_sales_df().rename(columns={"unit_price": "price"})
    before = df.copy()
    canonicalise(replace_column(SALES, "unit_price", aliases=("price",)), df)
    pd.testing.assert_frame_equal(df, before)


@pytest.mark.parametrize("source", ["sales", "payments", "customers", "products"])
def test_contract_matches_raw_table_columns(source):
    ddl = RAW_INIT_SQL.read_text()
    body = re.search(rf"create table raw\.{source} \((.*?)\n\);", ddl, re.DOTALL).group(1)
    raw_columns = [line.split()[0] for line in body.splitlines() if line.strip()]
    source_columns = raw_columns[: raw_columns.index("business_date")]
    assert [c.name for c in load_contract(source).columns] == source_columns


@pytest.mark.parametrize("source", ["sales", "payments", "customers", "products"])
def test_generator_output_satisfies_its_contract(source):
    frame = clean_frames()[source]
    assert diff_schema(load_contract(source), frame).is_clean


@pytest.mark.parametrize("source", ["sales", "payments", "customers", "products"])
def test_contract_metadata_and_nullability(source):
    contract = load_contract(source)
    assert contract.source == source and contract.version == 1
    assert {c.name for c in contract.columns if not c.nullable} == NON_NULLABLE[source]


def test_file_formats_follow_the_landing_conventions():
    formats = {s: load_contract(s).file_format for s in NON_NULLABLE}
    assert formats == {
        "sales": "csv",
        "payments": "parquet",
        "customers": "json",
        "products": "json",
    }


def test_load_contract_reads_from_a_given_directory(tmp_path):
    (tmp_path / "widgets.yml").write_text(
        "source: widgets\nversion: 3\nfile_format: csv\n"
        "columns:\n  - {name: id, dtype: int, nullable: false, aliases: [widget_id]}\n"
        "  - {name: label, dtype: string}\n"
    )
    contract = load_contract("widgets", tmp_path)
    assert contract.version == 3 and contract.columns[0].aliases == ("widget_id",)
    assert contract.columns[1].nullable is True and contract.columns[1].aliases == ()
    assert DEFAULT_DIR != tmp_path


def test_load_contract_rejects_unknown_dtype(tmp_path):
    (tmp_path / "bad.yml").write_text(
        "source: bad\nversion: 1\nfile_format: csv\ncolumns:\n  - {name: a, dtype: float}\n"
    )
    with pytest.raises(ValueError, match="dtype"):
        load_contract("bad", tmp_path)
