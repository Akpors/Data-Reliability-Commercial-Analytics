from datetime import date, timedelta

import pandas as pd
import pytest

from drct.chaos.state import active, enable, record_affected, reset
from drct.common.sim_clock import read_clock
from drct.generator.daily import build_day
from drct.generator.entities import build_customers, build_products
from drct.generator.writer import seed, simulate_day
from drct.ops import commands
from drct.ops.commands import chaos_reset

D = date(2026, 7, 18)
REAL_TODAY = date(2026, 10, 16)  # seed puts the next simulated business date at D


def clean_batch(day: date):
    return build_day(day, 42, build_customers(42, as_of=day), build_products(42))


@pytest.mark.db
def test_reset_returns_affected_dates_and_clears(db_conn):
    enable(db_conn, "duplicates")
    record_affected(db_conn, ["duplicates"], D)
    assert reset(db_conn) == [D] and active(db_conn) == {}


@pytest.mark.db
def test_reset_returns_sorted_distinct_dates_and_clears_affected(db_conn):
    later = D + timedelta(days=1)
    record_affected(db_conn, ["duplicates", "broken_fk"], later)
    record_affected(db_conn, ["duplicates"], D)
    record_affected(db_conn, ["duplicates"], D)  # recording twice is harmless
    assert reset(db_conn) == [D, later]
    assert reset(db_conn) == []


@pytest.mark.db
def test_enable_unknown_scenario_raises(db_conn):
    with pytest.raises(ValueError, match="unknown chaos scenario"):
        enable(db_conn, "meteor_strike")
    assert active(db_conn) == {}


@pytest.mark.db
def test_enable_merges_defaults_and_replaces_on_reenable(db_conn):
    enable(db_conn, "duplicates")
    enable(db_conn, "volume_spike", {"multiplier": 0.1})
    assert active(db_conn) == {"duplicates": {"rate": 0.05}, "volume_spike": {"multiplier": 0.1}}
    enable(db_conn, "duplicates", {"rate": 0.2})
    assert active(db_conn)["duplicates"] == {"rate": 0.2}


@pytest.mark.db
def test_enable_schema_change_add_mode_defaults_to_discount_code(db_conn):
    enable(db_conn, "schema_change", {"mode": "add"})
    assert active(db_conn)["schema_change"] == {
        "mode": "add",
        "column": "discount_code",
        "value": "WELCOME10",
    }


@pytest.mark.db
def test_enable_rejects_bad_schema_mode_and_stale_source(db_conn):
    with pytest.raises(ValueError, match="mode"):
        enable(db_conn, "schema_change", {"mode": "drop"})
    with pytest.raises(ValueError, match="source"):
        enable(db_conn, "stale_data", {"source": "fx_rates"})


@pytest.mark.db
def test_simulate_day_applies_chaos_and_records_affected(db_conn, settings_tmp):
    seed(db_conn, settings_tmp, real_today=REAL_TODAY)
    enable(db_conn, "duplicates")
    enable(db_conn, "api_flaky")
    assert simulate_day(db_conn, settings_tmp) == D
    payments = pd.read_parquet(settings_tmp.landing_dir / "payments" / f"payments_{D}.parquet")
    assert payments["payment_id"].duplicated().sum() > 0
    affected = db_conn.execute("select scenario, business_date from control.chaos_affected")
    assert affected.fetchall() == [("duplicates", D)]  # api_flaky touches no file


@pytest.mark.db
def test_simulate_day_stale_data_suppresses_source_and_records_affected(db_conn, settings_tmp):
    seed(db_conn, settings_tmp, real_today=REAL_TODAY)
    enable(db_conn, "stale_data")
    simulate_day(db_conn, settings_tmp)
    landing = settings_tmp.landing_dir
    assert not (landing / "payments" / f"payments_{D}.parquet").exists()
    assert (landing / "sales" / f"sales_{D}.csv").exists()
    assert reset(db_conn) == [D]


@pytest.mark.db
def test_simulate_day_build_failure_leaves_clock_alone(db_conn, settings_tmp, monkeypatch):
    from drct.generator import writer

    seed(db_conn, settings_tmp, real_today=REAL_TODAY)
    before = read_clock(db_conn).now

    def boom(*args, **kwargs):
        raise RuntimeError("generator broke")

    monkeypatch.setattr(writer, "build_day", boom)
    with pytest.raises(RuntimeError, match="generator broke"):
        simulate_day(db_conn, settings_tmp)
    assert read_clock(db_conn).now == before
    assert not (settings_tmp.landing_dir / "sales" / f"sales_{D}.csv").exists()


@pytest.mark.db
def test_chaos_reset_resends_clean_files_for_affected_dates(db_conn, settings_tmp):
    seed(db_conn, settings_tmp, real_today=REAL_TODAY)
    enable(db_conn, "broken_fk")
    simulate_day(db_conn, settings_tmp)
    simulate_day(db_conn, settings_tmp)
    second = D + timedelta(days=1)

    assert chaos_reset(db_conn, settings_tmp) == [D, second]

    assert active(db_conn) == {}
    landing = settings_tmp.landing_dir
    for day in (D, second):
        resent = pd.read_csv(landing / "sales" / "_resend" / f"sales_{day}.csv", dtype=str)
        pd.testing.assert_frame_equal(resent, clean_batch(day).sales)
        for name in (
            f"payments/_resend/payments_{day}.parquet",
            f"customers/_resend/customers_{day}.json",
            f"products/_resend/products_{day}.json",
        ):
            assert (landing / name).is_file(), name
    # The chaotic originals stay where they landed; resend files are never pending.
    assert (landing / "sales" / f"sales_{D}.csv").is_file()


@pytest.mark.db
def test_cli_chaos_enable_casts_params(db_conn, cli_env, capsys):
    code = commands.main(
        ["chaos", "enable", "volume_spike", "--param", "multiplier=0.1", "--param", "note=x"]
    )
    assert code == 0
    assert active(db_conn) == {"volume_spike": {"multiplier": 0.1, "note": "x"}}
    commands.main(["chaos", "enable", "api_flaky", "--param", "fail_attempts=3"])
    assert active(db_conn)["api_flaky"] == {"fail_attempts": 3}
    assert "api_flaky" in capsys.readouterr().out


@pytest.mark.db
def test_cli_chaos_enable_unknown_scenario_exits_1(db_conn, cli_env, capsys):
    assert commands.main(["chaos", "enable", "meteor_strike"]) == 1
    assert "unknown chaos scenario" in capsys.readouterr().out


@pytest.mark.db
def test_cli_chaos_reset_prints_reprocess_hint(db_conn, cli_env, capsys):
    record_affected(db_conn, ["duplicates"], D)
    record_affected(db_conn, ["duplicates"], D + timedelta(days=2))
    assert commands.main(["chaos", "reset"]) == 0
    out = capsys.readouterr().out
    assert f"make reprocess FROM={D} TO={D + timedelta(days=2)}" in out


@pytest.mark.db
def test_cli_chaos_reset_with_nothing_affected(db_conn, cli_env, capsys):
    assert commands.main(["chaos", "reset"]) == 0
    assert "make reprocess" not in capsys.readouterr().out


@pytest.mark.db
def test_cli_replay_quarantine_moves_files_back(db_conn, cli_env, settings_tmp, capsys):
    held = settings_tmp.landing_dir / "sales" / "_quarantine"
    held.mkdir(parents=True)
    (held / f"sales_{D}.csv").write_text("x")
    db_conn.execute(
        "insert into quarantine.files (source, file_name) values ('sales', %s)", (f"sales_{D}.csv",)
    )
    assert commands.main(["replay-quarantine", "--source", "sales"]) == 0
    assert (settings_tmp.landing_dir / "sales" / f"sales_{D}.csv").is_file()
    assert f"sales_{D}.csv" in capsys.readouterr().out
    assert cli_env.triggered_dags() == []


@pytest.mark.db
def test_cli_replay_quarantine_reports_files_it_leaves_behind(
    db_conn, cli_env, settings_tmp, capsys
):
    """Review C2: a quarantined file with no record awaiting replay is left and reported."""
    held = settings_tmp.landing_dir / "sales" / "_quarantine"
    held.mkdir(parents=True)
    (held / "sales_2026-07-28.csv").write_text("x")
    assert commands.main(["replay-quarantine", "--source", "sales"]) == 0
    assert (held / "sales_2026-07-28.csv").is_file()
    out = capsys.readouterr().out
    assert "Replayed" not in out
    assert "Left sales_2026-07-28.csv" in out and "no quarantine record" in out
