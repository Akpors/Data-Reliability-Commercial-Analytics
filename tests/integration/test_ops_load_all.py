import pytest

from drct.common.sim_clock import current_business_date, read_clock
from drct.ops import commands


def count(conn, table: str) -> int:
    return conn.execute(f"select count(*) from {table}").fetchone()[0]


@pytest.fixture
def seeded(db_conn, cli_env, monkeypatch):
    monkeypatch.setenv("FX_SOURCE", "stub")
    assert commands.main(["seed", "--no-trigger"]) == 0
    return cli_env


@pytest.mark.db
def test_load_all_loads_every_source_and_fx_without_airflow(db_conn, seeded, settings_tmp, capsys):
    assert commands.main(["load-all"]) == 0
    assert seeded.instances == []  # no Airflow involved
    assert count(db_conn, "raw.sales") > 0
    assert count(db_conn, "raw.payments") > 0
    assert count(db_conn, "raw.customers") > 0
    assert count(db_conn, "raw.products") > 0
    assert count(db_conn, "quarantine.files") == 0
    watermark = db_conn.execute("select watermark from control.fx_sync").fetchone()[0]
    assert watermark == current_business_date(read_clock(db_conn))
    assert count(db_conn, "raw.fx_rates") > 0
    assert not list((settings_tmp.landing_dir / "sales").glob("sales_*.csv"))  # all moved away
    out = capsys.readouterr().out
    assert "sales: 90 loaded, 0 quarantined" in out
    assert "fx:" in out


@pytest.mark.db
def test_load_all_exits_non_zero_when_a_file_is_quarantined(db_conn, seeded, settings_tmp, capsys):
    sales = min((settings_tmp.landing_dir / "sales").glob("sales_*.csv"))
    sales.write_text(sales.read_text().replace("unit_price", "price", 1))
    assert commands.main(["load-all"]) == 1
    assert count(db_conn, "quarantine.files") == 1
    assert "sales: 89 loaded, 1 quarantined" in capsys.readouterr().out


@pytest.mark.db
def test_load_all_before_seed_says_seed_first(db_conn, cli_env, capsys):
    assert commands.main(["load-all"]) == 1
    assert "run `make seed` first" in capsys.readouterr().out
