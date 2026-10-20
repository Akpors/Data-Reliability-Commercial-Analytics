import dataclasses
import json
import os
from datetime import UTC, date, datetime
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import psycopg
import pytest

from drct.generator.daily import build_day
from drct.generator.entities import build_customers, build_products
from drct.generator.writer import file_name, write_snapshot, write_transactions
from drct.ingestion.contracts import SchemaDiff, canonicalise, load_contract
from drct.ingestion.loader import (
    LoadResult,
    load_file,
    load_pending,
    parse_business_date,
    pending_files,
    reprocess_files,
)
from drct.ingestion.quarantine import quarantine_file, replay_quarantine
from drct.ingestion.readers import read_landing_file

D = date(2026, 5, 6)
SIM_NOW = datetime(2026, 5, 7, 6, tzinfo=UTC)
PRODUCTS = build_products(42)
CUSTOMERS = build_customers(42, as_of=D)
BATCH = build_day(D, 42, CUSTOMERS, PRODUCTS)


def count(conn, table: str) -> int:
    return conn.execute(f"select count(*) from {table}").fetchone()[0]


def dup_count(conn, table: str, key: str) -> int:
    """Number of surplus rows: total rows minus distinct keys."""
    return conn.execute(f"select count({key}) - count(distinct {key}) from {table}").fetchone()[0]


@pytest.fixture
def tmp_landing(tmp_path: Path) -> Path:
    return tmp_path / "landing"


@pytest.fixture
def landed_day(tmp_landing: Path) -> SimpleNamespace:
    sales, payments = write_transactions(BATCH, tmp_landing)
    customers = write_snapshot("customers", CUSTOMERS, D, tmp_landing)
    products = write_snapshot("products", PRODUCTS, D, tmp_landing)
    return SimpleNamespace(sales=sales, payments=payments, customers=customers, products=products)


@pytest.fixture
def renamed_price_file(tmp_landing: Path) -> Path:
    """A sales file whose ``unit_price`` column arrives as ``price``: a breaking drift."""
    drifted = dataclasses.replace(BATCH, sales=BATCH.sales.rename(columns={"unit_price": "price"}))
    return write_transactions(drifted, tmp_landing, suppress={"payments"})[0]


@pytest.fixture
def processed_dirty_and_resend_clean(tmp_landing: Path) -> SimpleNamespace:
    """``_processed/`` holds a payments file with duplicated rows, ``_resend/`` the clean one."""
    name = file_name("payments", D)
    dirty = tmp_landing / "payments" / "_processed" / name
    clean = tmp_landing / "payments" / "_resend" / name
    dirty.parent.mkdir(parents=True)
    clean.parent.mkdir(parents=True)
    pd.concat([BATCH.payments, BATCH.payments.head(5)]).to_parquet(dirty, index=False)
    BATCH.payments.to_parquet(clean, index=False)
    return SimpleNamespace(dirty=dirty, clean=clean)


# --- readers ---------------------------------------------------------------------------------


def test_parse_business_date():
    assert parse_business_date(Path("/x/sales/sales_2026-05-06.csv")) == D
    assert parse_business_date(Path("customers_2026-05-06.json")) == D


def test_parse_business_date_rejects_other_names():
    with pytest.raises(ValueError):
        parse_business_date(Path("sales_latest.csv"))


def assert_only_str_or_none(df: pd.DataFrame) -> None:
    for column in df.columns:
        for value in df[column]:
            assert value is None or type(value) is str, (column, value)


@pytest.mark.parametrize("which", ["sales", "payments", "customers", "products"])
def test_read_landing_file_returns_only_str_or_none(landed_day, which):
    df = read_landing_file(getattr(landed_day, which))
    assert len(df) > 0
    assert_only_str_or_none(df)


def test_read_csv_maps_empty_to_none_and_keeps_text(tmp_path):
    path = tmp_path / "sales_2026-05-06.csv"
    path.write_text("order_id,channel,quantity\n007,,NA\n", encoding="utf-8")
    df = read_landing_file(path)
    assert df.iloc[0].tolist() == ["007", None, "NA"]


def test_read_json_nulls_become_none_and_values_become_str(tmp_path):
    path = tmp_path / "customers_2026-05-06.json"
    path.write_text(
        json.dumps([{"customer_id": "C1", "email": None, "n": 3, "x": 1.5, "ok": True}]),
        encoding="utf-8",
    )
    df = read_landing_file(path)
    assert df.iloc[0].to_dict() == {
        "customer_id": "C1",
        "email": None,
        "n": "3",
        "x": "1.5",
        "ok": "true",
    }
    assert_only_str_or_none(df)


def test_read_json_preserves_number_and_boolean_source_text(tmp_path):
    path = tmp_path / "products_2026-05-06.json"
    path.write_text(
        '[{"n": 3, "price": 1.10, "mix": 2, "ok": true},'
        ' {"n": null, "price": 2.5, "mix": 2.5, "ok": false},'
        ' {"n": 7, "price": 1e3, "mix": 1, "ok": null}]',
        encoding="utf-8",
    )
    df = read_landing_file(path)
    assert df["n"].tolist() == ["3", None, "7"]
    assert df["price"].tolist() == ["1.10", "2.5", "1e3"]
    assert df["mix"].tolist() == ["2", "2.5", "1"]
    assert df["ok"].tolist() == ["true", "false", None]
    assert_only_str_or_none(df)


@pytest.mark.db
def test_json_nullable_int_column_is_not_quarantined(db_conn, tmp_landing, monkeypatch):
    # A valid JSON file with a null beside integers must still pass the contract's int check.
    contract = load_contract("products")
    monkeypatch.setattr(
        "drct.ingestion.loader.load_contract",
        lambda source: dataclasses.replace(
            contract,
            columns=tuple(
                dataclasses.replace(c, dtype="int") if c.name == "sku" else c
                for c in contract.columns
            ),
        ),
    )
    path = tmp_landing / "products" / file_name("products", D)
    path.parent.mkdir(parents=True)
    path.write_text(
        json.dumps(
            [
                {**dict.fromkeys(PRODUCTS.columns), "product_id": "P1", "sku": 3},
                {**dict.fromkeys(PRODUCTS.columns), "product_id": "P2", "sku": None},
            ]
        ),
        encoding="utf-8",
    )
    r = load_file(db_conn, "products", path, SIM_NOW, "b1")
    assert r.status == "loaded" and r.rows_loaded == 2
    assert db_conn.execute("select sku from raw.products order by product_id").fetchall() == [
        ("3",),
        (None,),
    ]


@pytest.mark.db
def test_quarantine_file_failed_insert_leaves_file_in_place(
    db_conn, renamed_price_file, tmp_landing
):
    # contract_version is not null-checked, but a non-int value makes the insert fail.
    with pytest.raises(psycopg.Error):
        quarantine_file(
            db_conn,
            "sales",
            renamed_price_file,
            SchemaDiff(),
            "not-an-int",
            SIM_NOW,
            tmp_landing,  # type: ignore[arg-type]
        )
    assert renamed_price_file.exists()
    assert not (tmp_landing / "sales" / "_quarantine").exists()
    assert count(db_conn, "quarantine.files") == 0


def test_read_parquet_nan_nat_and_numbers(tmp_path):
    path = tmp_path / "payments_2026-05-06.parquet"
    pd.DataFrame(
        {
            "payment_id": ["P1", None],
            "amount": [1.5, float("nan")],
            "qty": [3, 4],
            "ts": [pd.Timestamp("2026-05-06T10:00:00Z"), pd.NaT],
        }
    ).to_parquet(path, index=False)
    df = read_landing_file(path)
    assert df["payment_id"].tolist() == ["P1", None]
    assert df["amount"].tolist() == ["1.5", None]
    assert df["qty"].tolist() == ["3", "4"]
    assert df["ts"][1] is None and isinstance(df["ts"][0], str)
    assert_only_str_or_none(df)


def test_read_landing_file_rejects_unknown_extension(tmp_path):
    path = tmp_path / "sales_2026-05-06.xlsx"
    path.write_text("x")
    with pytest.raises(ValueError):
        read_landing_file(path)


# --- load_file -------------------------------------------------------------------------------


@pytest.mark.db
def test_load_file_adds_lineage_columns(db_conn, landed_day):
    r = load_file(db_conn, "sales", landed_day.sales, SIM_NOW, "b1")
    assert r.status == "loaded" and r.rows_loaded == r.rows_in > 0
    assert r.business_date == D and r.path == landed_day.sales and r.diff.is_clean
    row = db_conn.execute(
        "select business_date, _source_file, _batch_id, _sim_loaded_at from raw.sales limit 1"
    ).fetchone()
    assert row == (D, landed_day.sales.name, "b1", SIM_NOW)
    assert db_conn.execute("select _loaded_at is not null from raw.sales limit 1").fetchone() == (
        True,
    )


@pytest.mark.db
def test_load_file_stores_source_values_as_text(db_conn, landed_day):
    load_file(db_conn, "sales", landed_day.sales, SIM_NOW, "b1")
    first = BATCH.sales.iloc[0]
    row = db_conn.execute(
        "select order_line_id, unit_price, quantity from raw.sales order by ctid limit 1"
    ).fetchone()
    assert row == (first["order_line_id"], first["unit_price"], first["quantity"])


@pytest.mark.db
@pytest.mark.parametrize("which", ["sales", "payments", "customers", "products"])
def test_every_source_loads(db_conn, landed_day, which):
    r = load_file(db_conn, which, getattr(landed_day, which), SIM_NOW, "b1")
    assert r.status == "loaded" and r.rows_loaded == r.rows_in > 0
    assert count(db_conn, f"raw.{which}") == r.rows_in


@pytest.mark.db
def test_reloading_same_date_is_idempotent(db_conn, landed_day):  # Review Focus 1
    load_file(db_conn, "sales", landed_day.sales, SIM_NOW, "b1")
    n1 = count(db_conn, "raw.sales")
    load_file(db_conn, "sales", landed_day.sales, SIM_NOW, "b2")
    assert count(db_conn, "raw.sales") == n1
    assert db_conn.execute("select distinct _batch_id from raw.sales").fetchall() == [("b2",)]


@pytest.mark.db
def test_reload_leaves_other_dates_alone(db_conn, landed_day):
    load_file(db_conn, "sales", landed_day.sales, SIM_NOW, "b1")
    db_conn.execute(
        "insert into raw.sales (order_id, business_date) values ('other', %s)",
        (date(2026, 5, 5),),
    )
    load_file(db_conn, "sales", landed_day.sales, SIM_NOW, "b2")
    assert count(db_conn, "raw.sales where order_id = 'other'") == 1


@pytest.mark.db
def test_failed_copy_rolls_back_the_delete(db_conn, landed_day, monkeypatch):
    load_file(db_conn, "sales", landed_day.sales, SIM_NOW, "b1")
    n1 = count(db_conn, "raw.sales")

    def canonicalise_with_bad_json(contract, df):
        frame = canonicalise(contract, df)
        frame.loc[frame.index[-1], "_extra"] = "{not json"  # the COPY fails on the last row
        return frame

    monkeypatch.setattr("drct.ingestion.loader.canonicalise", canonicalise_with_bad_json)
    with pytest.raises(psycopg.errors.InvalidTextRepresentation):
        load_file(db_conn, "sales", landed_day.sales, SIM_NOW, "b2")
    assert count(db_conn, "raw.sales") == n1
    assert db_conn.execute("select distinct _batch_id from raw.sales").fetchall() == [("b1",)]


@pytest.mark.db
def test_breaking_file_is_not_loaded_and_load_file_does_not_move_it(db_conn, renamed_price_file):
    r = load_file(db_conn, "sales", renamed_price_file, SIM_NOW, "b1")
    assert r.status == "quarantined" and r.rows_loaded == 0 and r.rows_in > 0
    assert r.diff.missing == ["unit_price"] and r.diff.added == ["price"]
    assert count(db_conn, "raw.sales") == 0
    assert renamed_price_file.exists()
    assert count(db_conn, "quarantine.files") == 0


@pytest.mark.db
def test_breaking_file_does_not_delete_existing_rows(db_conn, tmp_landing):
    path = write_transactions(BATCH, tmp_landing, suppress={"payments"})[0]
    load_file(db_conn, "sales", path, SIM_NOW, "b1")
    n1 = count(db_conn, "raw.sales")
    drifted = dataclasses.replace(BATCH, sales=BATCH.sales.rename(columns={"unit_price": "price"}))
    write_transactions(drifted, tmp_landing, suppress={"payments"})  # replaces the same file
    r = load_file(db_conn, "sales", path, SIM_NOW, "b2")
    assert r.status == "quarantined"
    assert count(db_conn, "raw.sales") == n1


@pytest.mark.db
def test_additive_file_loads_with_extra_and_warning_event(db_conn, tmp_landing):
    wider = dataclasses.replace(BATCH, sales=BATCH.sales.assign(promo_code="SPRING"))
    path = write_transactions(wider, tmp_landing, suppress={"payments"})[0]
    r = load_file(db_conn, "sales", path, SIM_NOW, "b1")
    assert r.status == "loaded" and r.rows_loaded == len(BATCH.sales)
    assert db_conn.execute("select distinct _extra from raw.sales").fetchall() == [
        ({"promo_code": "SPRING"},)
    ]
    assert db_conn.execute(
        "select is_breaking, added, missing from monitoring.schema_events"
    ).fetchone() == (False, ["promo_code"], [])


@pytest.mark.db
def test_alias_is_recorded_and_loaded_under_canonical_name(
    db_conn, renamed_price_file, monkeypatch
):
    # No shipped contract declares an alias, so give sales one for this test.
    contract = load_contract("sales")
    columns = tuple(
        dataclasses.replace(c, aliases=("price",)) if c.name == "unit_price" else c
        for c in contract.columns
    )
    monkeypatch.setattr(
        "drct.ingestion.loader.load_contract",
        lambda source: dataclasses.replace(contract, columns=columns),
    )
    r = load_file(db_conn, "sales", renamed_price_file, SIM_NOW, "b1")
    assert r.status == "loaded" and r.rows_loaded == len(BATCH.sales)
    assert db_conn.execute(
        "select aliased, is_breaking from monitoring.schema_events"
    ).fetchone() == (
        {"price": "unit_price"},
        False,
    )
    assert db_conn.execute("select count(unit_price) from raw.sales").fetchone() == (
        len(BATCH.sales),
    )


@pytest.mark.db
def test_clean_load_records_clean_schema_event(db_conn, landed_day):
    load_file(db_conn, "sales", landed_day.sales, SIM_NOW, "b1")
    assert db_conn.execute(
        "select is_breaking, added, missing from monitoring.schema_events"
    ).fetchone() == (False, [], [])
    row = db_conn.execute(
        "select source, file_name, contract_version, type_mismatches, aliased, sim_now"
        " from monitoring.schema_events"
    ).fetchone()
    assert row == ("sales", landed_day.sales.name, 1, {}, {}, SIM_NOW)


@pytest.mark.db
def test_type_mismatch_is_breaking_and_recorded(db_conn, tmp_landing):
    bad = BATCH.sales.copy()
    bad.loc[bad.index[0], "quantity"] = "many"
    path = write_transactions(
        dataclasses.replace(BATCH, sales=bad), tmp_landing, suppress={"payments"}
    )[0]
    r = load_file(db_conn, "sales", path, SIM_NOW, "b1")
    assert r.status == "quarantined"
    assert db_conn.execute(
        "select is_breaking, type_mismatches from monitoring.schema_events"
    ).fetchone() == (True, {"quantity": "many"})


# --- pending_files / load_pending ------------------------------------------------------------


def test_pending_files_top_level_only_sorted_by_date(tmp_landing):
    folder = tmp_landing / "sales"
    for name in [
        "sales_2026-05-08.csv",
        "sales_2026-05-06.csv",
        "sales_2026-05-07.csv",
        ".sales_2026-05-09.csv.tmp",
        "sales_2026-05-09.csv.tmp",
        "sales_2026-05-10.tmp",
        ".sales_2026-05-11.csv",
        "notes.txt",
        "sales_latest.csv",
        "payments_2026-05-06.csv",
    ]:
        folder.mkdir(parents=True, exist_ok=True)
        (folder / name).write_text("x")
    for sub in ("_processed", "_quarantine", "_resend"):
        (folder / sub).mkdir()
        (folder / sub / "sales_2026-05-05.csv").write_text("x")
    (folder / "sales_2026-05-12.csv").mkdir()  # a directory is not a file
    assert [p.name for p in pending_files(tmp_landing, "sales")] == [
        "sales_2026-05-06.csv",
        "sales_2026-05-07.csv",
        "sales_2026-05-08.csv",
    ]


def test_pending_files_missing_folder_is_empty(tmp_landing):
    assert pending_files(tmp_landing, "sales") == []


@pytest.mark.db
def test_load_pending_moves_loaded_files_to_processed(db_conn, landed_day, tmp_landing):
    results = load_pending(db_conn, "sales", tmp_landing, SIM_NOW, "b1")
    assert [r.status for r in results] == ["loaded"]
    assert not landed_day.sales.exists()
    assert (tmp_landing / "sales" / "_processed" / landed_day.sales.name).exists()
    assert pending_files(tmp_landing, "sales") == []
    assert load_pending(db_conn, "sales", tmp_landing, SIM_NOW, "b2") == []


@pytest.mark.db
def test_load_pending_loads_oldest_first(db_conn, tmp_landing):
    for offset in (2, 0, 1):
        day = date(2026, 5, 6 + offset)
        batch = build_day(day, 42, CUSTOMERS, PRODUCTS)
        write_transactions(batch, tmp_landing, suppress={"payments"})
    results = load_pending(db_conn, "sales", tmp_landing, SIM_NOW, "b1")
    assert [r.business_date for r in results] == [date(2026, 5, 6 + i) for i in range(3)]


@pytest.mark.db
def test_breaking_file_is_quarantined_and_not_loaded(db_conn, renamed_price_file, tmp_landing):
    r = load_pending(db_conn, "sales", tmp_landing, SIM_NOW, "b1")[0]
    assert r.status == "quarantined" and count(db_conn, "raw.sales") == 0
    assert (tmp_landing / "sales" / "_quarantine" / renamed_price_file.name).exists()
    assert not renamed_price_file.exists()
    assert db_conn.execute("select is_breaking from monitoring.schema_events").fetchone() == (True,)
    row = db_conn.execute(
        "select source, file_name, stored_path, contract_version, diff, sim_now, replayed_at"
        " from quarantine.files"
    ).fetchone()
    assert row[:4] == (
        "sales",
        renamed_price_file.name,
        str(tmp_landing / "sales" / "_quarantine" / renamed_price_file.name),
        1,
    )
    assert row[4]["missing"] == ["unit_price"] and row[4]["added"] == ["price"]
    assert row[5:] == (SIM_NOW, None)


# --- quarantine ------------------------------------------------------------------------------


@pytest.mark.db
def test_quarantine_file_moves_and_records(db_conn, renamed_price_file, tmp_landing):
    diff = SchemaDiff(missing=["unit_price"], type_mismatches={"quantity": "many"})
    stored = quarantine_file(db_conn, "sales", renamed_price_file, diff, 3, SIM_NOW, tmp_landing)
    assert stored == tmp_landing / "sales" / "_quarantine" / renamed_price_file.name
    assert stored.exists() and not renamed_price_file.exists()
    assert db_conn.execute(
        "select diff, contract_version, stored_path from quarantine.files"
    ).fetchone() == (
        {
            "added": [],
            "missing": ["unit_price"],
            "type_mismatches": {"quantity": "many"},
            "aliased": {},
        },
        3,
        str(stored),
    )


@pytest.mark.db
def test_replay_quarantine_moves_back_and_marks_replayed(db_conn, renamed_price_file, tmp_landing):
    load_pending(db_conn, "sales", tmp_landing, SIM_NOW, "b1")
    assert pending_files(tmp_landing, "sales") == []
    paths = replay_quarantine(db_conn, "sales", tmp_landing).moved
    assert paths == [tmp_landing / "sales" / renamed_price_file.name]
    assert paths[0].exists()
    assert not (tmp_landing / "sales" / "_quarantine" / renamed_price_file.name).exists()
    assert db_conn.execute("select replayed_at is not null from quarantine.files").fetchall() == [
        (True,)
    ]
    assert [p.name for p in pending_files(tmp_landing, "sales")] == [renamed_price_file.name]


@pytest.mark.db
def test_replay_quarantine_leaves_other_sources_and_replayed_rows(
    db_conn, renamed_price_file, tmp_landing
):
    load_pending(db_conn, "sales", tmp_landing, SIM_NOW, "b1")
    assert replay_quarantine(db_conn, "payments", tmp_landing).moved == []
    assert db_conn.execute("select replayed_at from quarantine.files").fetchall() == [(None,)]
    replay_quarantine(db_conn, "sales", tmp_landing)
    first = db_conn.execute("select replayed_at from quarantine.files").fetchone()[0]
    assert replay_quarantine(db_conn, "sales", tmp_landing).moved == []
    assert db_conn.execute("select replayed_at from quarantine.files").fetchone()[0] == first


@pytest.mark.db
def test_replayed_file_loads_once_the_drift_is_fixed(db_conn, renamed_price_file, tmp_landing):
    load_pending(db_conn, "sales", tmp_landing, SIM_NOW, "b1")
    (path,) = replay_quarantine(db_conn, "sales", tmp_landing).moved
    path.write_text(
        path.read_text(encoding="utf-8").replace("price", "unit_price", 1), encoding="utf-8"
    )
    assert [r.status for r in load_pending(db_conn, "sales", tmp_landing, SIM_NOW, "b2")] == [
        "loaded"
    ]
    assert count(db_conn, "raw.sales") == len(BATCH.sales)


@pytest.mark.db
def test_replay_quarantine_leaves_files_without_an_unreplayed_record(
    db_conn, renamed_price_file, tmp_landing
):
    """Review C2: a file left in _quarantine/ by an earlier stack (no row in this warehouse) must
    not be replayed along with the file the operator meant to accept."""
    load_pending(db_conn, "sales", tmp_landing, SIM_NOW, "b1")
    held = tmp_landing / "sales" / "_quarantine"
    stale = held / "sales_2026-07-28.csv"
    stale.write_text("order_id,price\n1,2\n")
    replay = replay_quarantine(db_conn, "sales", tmp_landing)
    assert replay.moved == [tmp_landing / "sales" / renamed_price_file.name]
    assert replay.untracked == [stale]
    assert stale.exists()
    assert [p.name for p in pending_files(tmp_landing, "sales")] == [renamed_price_file.name]


@pytest.mark.db
def test_replay_quarantine_skips_a_file_whose_record_was_already_replayed(
    db_conn, renamed_price_file, tmp_landing
):
    load_pending(db_conn, "sales", tmp_landing, SIM_NOW, "b1")
    (path,) = replay_quarantine(db_conn, "sales", tmp_landing).moved
    # The same name lands in _quarantine/ again without a new record (e.g. copied back by hand).
    os.replace(path, tmp_landing / "sales" / "_quarantine" / path.name)
    replay = replay_quarantine(db_conn, "sales", tmp_landing)
    assert replay.moved == []
    assert [p.name for p in replay.untracked] == [path.name]


# --- reprocess -------------------------------------------------------------------------------


@pytest.mark.db
def test_reprocess_prefers_resend(db_conn, tmp_landing, processed_dirty_and_resend_clean):
    load_file(db_conn, "payments", processed_dirty_and_resend_clean.dirty, SIM_NOW, "b1")
    assert dup_count(db_conn, "raw.payments", "payment_id") == 5
    results = reprocess_files(db_conn, tmp_landing, D, D, SIM_NOW, "rp")
    assert [(r.source, r.status) for r in results] == [("payments", "loaded")]
    assert dup_count(db_conn, "raw.payments", "payment_id") == 0
    assert count(db_conn, "raw.payments") == len(BATCH.payments)
    assert db_conn.execute("select distinct _batch_id from raw.payments").fetchall() == [("rp",)]
    # The used resend file replaces the processed one.
    assert not processed_dirty_and_resend_clean.clean.exists()
    assert len(pd.read_parquet(processed_dirty_and_resend_clean.dirty)) == len(BATCH.payments)


@pytest.mark.db
def test_reprocess_falls_back_to_processed(db_conn, landed_day, tmp_landing):
    load_pending(db_conn, "sales", tmp_landing, SIM_NOW, "b1")
    n1 = count(db_conn, "raw.sales")
    results = reprocess_files(db_conn, tmp_landing, D, D, SIM_NOW, "rp")
    assert [(r.source, r.status) for r in results] == [("sales", "loaded")]
    assert count(db_conn, "raw.sales") == n1
    assert db_conn.execute("select distinct _batch_id from raw.sales").fetchall() == [("rp",)]
    assert (tmp_landing / "sales" / "_processed" / landed_day.sales.name).exists()


@pytest.mark.db
def test_reprocess_covers_the_date_range_and_source_filter(db_conn, tmp_landing):
    for offset in range(3):
        write_transactions(
            build_day(date(2026, 5, 6 + offset), 42, CUSTOMERS, PRODUCTS), tmp_landing
        )
    load_pending(db_conn, "sales", tmp_landing, SIM_NOW, "b1")
    load_pending(db_conn, "payments", tmp_landing, SIM_NOW, "b1")
    results = reprocess_files(
        db_conn, tmp_landing, date(2026, 5, 7), date(2026, 5, 8), SIM_NOW, "rp", sources=("sales",)
    )
    assert [(r.source, r.business_date) for r in results] == [
        ("sales", date(2026, 5, 7)),
        ("sales", date(2026, 5, 8)),
    ]
    assert isinstance(results[0], LoadResult)
    assert db_conn.execute(
        "select business_date, _batch_id from raw.sales group by 1, 2 order by 1"
    ).fetchall() == [(date(2026, 5, 6), "b1"), (date(2026, 5, 7), "rp"), (date(2026, 5, 8), "rp")]


@pytest.mark.db
def test_reprocess_skips_dates_with_no_file(db_conn, tmp_landing):
    assert reprocess_files(db_conn, tmp_landing, D, D, SIM_NOW, "rp") == []


@pytest.mark.db
def test_reprocess_quarantines_a_breaking_resend(db_conn, tmp_landing, landed_day):
    load_pending(db_conn, "sales", tmp_landing, SIM_NOW, "b1")
    n1 = count(db_conn, "raw.sales")
    resend = tmp_landing / "sales" / "_resend" / file_name("sales", D)
    resend.parent.mkdir()
    BATCH.sales.rename(columns={"unit_price": "price"}).to_csv(resend, index=False)
    results = reprocess_files(db_conn, tmp_landing, D, D, SIM_NOW, "rp", sources=("sales",))
    assert [r.status for r in results] == ["quarantined"]
    assert count(db_conn, "raw.sales") == n1
    assert not resend.exists()
    assert (tmp_landing / "sales" / "_quarantine" / resend.name).exists()
    assert (tmp_landing / "sales" / "_processed" / resend.name).exists()
    assert count(db_conn, "quarantine.files") == 1
