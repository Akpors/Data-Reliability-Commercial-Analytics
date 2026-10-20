from datetime import UTC, date, datetime

import pytest

from drct.common.sim_clock import read_clock
from drct.generator.writer import seed, simulate_day
from drct.ops import commands

REAL_TODAY = date(2026, 10, 16)


@pytest.mark.db
def test_seed_anchors_calendar_and_refuses_reseed(db_conn, settings_tmp):
    clock = seed(db_conn, settings_tmp, real_today=REAL_TODAY)
    assert clock.day0 == date(2026, 4, 19)
    assert clock.now == datetime(2026, 7, 18, 6, tzinfo=UTC)
    landing = settings_tmp.landing_dir
    assert len(list((landing / "sales").glob("sales_*.csv"))) == 90
    assert len(list((landing / "payments").glob("payments_*.parquet"))) == 90
    assert [p.name for p in (landing / "customers").glob("*.json")] == ["customers_2026-07-17.json"]
    assert [p.name for p in (landing / "products").glob("*.json")] == ["products_2026-07-17.json"]
    assert (landing / "sales" / "sales_2026-04-19.csv").exists()
    with pytest.raises(RuntimeError, match="already seeded"):
        seed(db_conn, settings_tmp, real_today=REAL_TODAY)


@pytest.mark.db
def test_seed_sets_fx_watermark_to_day_before_day0(db_conn, settings_tmp):
    seed(db_conn, settings_tmp, real_today=REAL_TODAY)
    rows = db_conn.execute("select id, watermark from control.fx_sync").fetchall()
    assert rows == [(1, date(2026, 4, 18))]


@pytest.mark.db
def test_seed_force_reinitialises(db_conn, settings_tmp):
    seed(db_conn, settings_tmp, real_today=REAL_TODAY)
    clock = seed(db_conn, settings_tmp, real_today=date(2026, 10, 17), force=True)
    assert clock.day0 == date(2026, 4, 20)
    assert read_clock(db_conn) == clock
    assert db_conn.execute("select watermark from control.fx_sync").fetchall() == [
        (date(2026, 4, 19),)
    ]


@pytest.mark.db
def test_simulate_day_writes_yesterday(db_conn, settings_tmp):
    seed(db_conn, settings_tmp, real_today=REAL_TODAY)
    assert simulate_day(db_conn, settings_tmp) == date(2026, 7, 18)
    landing = settings_tmp.landing_dir
    assert read_clock(db_conn).now == datetime(2026, 7, 19, 6, tzinfo=UTC)
    for name in (
        "sales/sales_2026-07-18.csv",
        "payments/payments_2026-07-18.parquet",
        "customers/customers_2026-07-18.json",
        "products/products_2026-07-18.json",
    ):
        assert (landing / name).exists(), name


@pytest.mark.db
def test_cli_simulate_day_before_seed_says_seed_first(db_conn, cli_env, capsys):
    assert commands.main(["simulate-day"]) == 1
    assert "run `make seed` first" in capsys.readouterr().out
    assert cli_env.triggered_dags() == []


@pytest.mark.db
def test_cli_seed_then_simulate_day_trigger_fx_sync(db_conn, cli_env, capsys):
    assert commands.main(["seed"]) == 0
    assert cli_env.triggered_dags() == ["ingest_fx"]
    assert commands.main(["simulate-day", "--days", "2"]) == 0
    # One FX sync after the last simulated day covers every new business date.
    assert cli_env.triggered_dags() == ["ingest_fx", "ingest_fx"]
    assert "ingest_fx" in capsys.readouterr().out


@pytest.mark.db
def test_cli_seed_no_trigger_never_calls_airflow(db_conn, cli_env, capsys):
    assert commands.main(["seed", "--no-trigger"]) == 0
    assert cli_env.instances == []
    assert "ingest_fx" not in capsys.readouterr().out


@pytest.mark.db
def test_cli_seed_if_needed_seeds_a_fresh_stack(db_conn, cli_env, capsys):
    assert commands.main(["seed", "--if-needed"]) == 0
    assert read_clock(db_conn).day0 is not None
    assert "Seeded business dates" in capsys.readouterr().out
    assert cli_env.triggered_dags() == ["ingest_fx"]


@pytest.mark.db
def test_cli_seed_if_needed_skips_a_seeded_stack_but_still_syncs_fx(
    db_conn, cli_env, settings_tmp, capsys
):
    """R29 / review C1: `make demo` on an already-seeded stack must not stop at seed."""
    assert commands.main(["seed"]) == 0
    clock = read_clock(db_conn)
    first_file = settings_tmp.landing_dir / "sales" / f"sales_{clock.day0}.csv"
    written = first_file.stat().st_mtime_ns
    capsys.readouterr()
    assert commands.main(["seed", "--if-needed"]) == 0
    assert "already seeded; skipping" in capsys.readouterr().out
    assert read_clock(db_conn) == clock
    assert first_file.stat().st_mtime_ns == written  # no history re-written
    # A first run whose FX trigger failed is recovered by the next demo.
    assert cli_env.triggered_dags() == ["ingest_fx", "ingest_fx"]


@pytest.mark.db
def test_cli_plain_seed_stays_strict_and_points_at_make_nuke(db_conn, cli_env, capsys):
    assert commands.main(["seed"]) == 0
    capsys.readouterr()
    assert commands.main(["seed"]) == 1
    out = capsys.readouterr().out
    assert "already seeded" in out and "make nuke" in out
    assert "--force" not in out  # unreachable through make, and unsafe on a live stack
    assert cli_env.triggered_dags() == ["ingest_fx"]


def test_cli_seed_if_needed_and_force_are_exclusive():
    with pytest.raises(SystemExit):
        commands.build_parser().parse_args(["seed", "--if-needed", "--force"])
