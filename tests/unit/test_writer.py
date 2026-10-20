import json
from datetime import date

import pandas as pd

from drct.generator.daily import build_day
from drct.generator.entities import build_customers, build_products
from drct.generator.writer import file_name, write_snapshot, write_transactions

D = date(2026, 5, 6)
PRODUCTS = build_products(42)
CUSTOMERS = build_customers(42, as_of=D)
BATCH = build_day(D, 42, CUSTOMERS, PRODUCTS)


def test_file_name_formats():
    assert file_name("payments", date(2026, 4, 19)) == "payments_2026-04-19.parquet"
    assert file_name("sales", date(2026, 4, 19)) == "sales_2026-04-19.csv"
    assert file_name("customers", date(2026, 4, 19)) == "customers_2026-04-19.json"
    assert file_name("products", date(2026, 4, 19)) == "products_2026-04-19.json"


def test_write_transactions_round_trips(tmp_path):
    paths = write_transactions(BATCH, tmp_path)
    assert {p.relative_to(tmp_path).as_posix() for p in paths} == {
        f"sales/sales_{D}.csv",
        f"payments/payments_{D}.parquet",
    }
    sales = pd.read_csv(paths[0], dtype=str)
    assert list(sales.columns) == list(BATCH.sales.columns)
    assert len(sales) == len(BATCH.sales)
    payments = pd.read_parquet(paths[1])
    assert len(payments) == len(BATCH.payments)
    assert all(dtype == "string" or dtype == object for dtype in payments.dtypes)


def test_suppressed_source_not_written(tmp_path):
    paths = write_transactions(BATCH, tmp_path, suppress={"payments"})
    assert all("payments" not in p.name for p in paths)
    assert not (tmp_path / "payments").exists()


def test_write_snapshot_json_array_of_records(tmp_path):
    path = write_snapshot("customers", CUSTOMERS, D, tmp_path)
    assert path == tmp_path / "customers" / f"customers_{D}.json"
    records = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(records, list) and len(records) == len(CUSTOMERS)
    assert records[0] == CUSTOMERS.iloc[0].to_dict()


def test_write_snapshot_suppressed_returns_none(tmp_path):
    assert write_snapshot("products", PRODUCTS, D, tmp_path, suppress={"products"}) is None
    assert not (tmp_path / "products").exists()


def test_writes_are_atomic_and_leave_no_temp_files(tmp_path):
    write_transactions(BATCH, tmp_path)
    write_snapshot("products", PRODUCTS, D, tmp_path)
    write_snapshot("products", PRODUCTS, D, tmp_path)  # overwriting the same name is fine
    leftovers = [p for p in tmp_path.rglob("*") if p.is_file() and p.name.endswith(".tmp")]
    assert leftovers == []
    assert not any(p.name.startswith(".") for p in tmp_path.rglob("*"))


def test_subdir_writes_under_each_source_folder(tmp_path):
    paths = write_transactions(BATCH, tmp_path, subdir="_resend")
    snapshot = write_snapshot("customers", CUSTOMERS, D, tmp_path, subdir="_resend")
    assert {p.relative_to(tmp_path).as_posix() for p in [*paths, snapshot]} == {
        f"sales/_resend/sales_{D}.csv",
        f"payments/_resend/payments_{D}.parquet",
        f"customers/_resend/customers_{D}.json",
    }
    assert not any(p.name.endswith(".tmp") for p in tmp_path.rglob("*"))
