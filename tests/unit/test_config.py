import json

import pytest

from drct.common.config import get_settings
from drct.common.logging import get_logger

REQUIRED = {
    "WAREHOUSE_DSN": "postgresql://u:p@h:5432/db",
    "LANDING_DIR": "/data/landing",
    "FX_API_URL": "https://fx.example/v1",
}


@pytest.fixture
def env(monkeypatch):
    for key in (
        "WAREHOUSE_DSN",
        "LANDING_DIR",
        "DRCT_SEED",
        "FX_SOURCE",
        "FX_API_URL",
        "AIRFLOW_API_URL",
        "DBT_PROJECT_DIR",
    ):
        monkeypatch.delenv(key, raising=False)
    for key, value in REQUIRED.items():
        monkeypatch.setenv(key, value)
    return monkeypatch


def test_get_settings_names_missing_env(monkeypatch):
    monkeypatch.delenv("WAREHOUSE_DSN", raising=False)
    with pytest.raises(ValueError, match="WAREHOUSE_DSN"):
        get_settings()


@pytest.mark.parametrize("missing", ["LANDING_DIR", "FX_API_URL"])
def test_get_settings_names_each_missing_required_var(env, missing):
    env.delenv(missing)
    with pytest.raises(ValueError, match=missing):
        get_settings()


def test_get_settings_applies_defaults(env):
    s = get_settings()
    assert s.warehouse_dsn == REQUIRED["WAREHOUSE_DSN"]
    assert str(s.landing_dir) == "/data/landing"
    assert s.seed == 42
    assert s.fx_source == "api"
    assert s.fx_api_url == REQUIRED["FX_API_URL"]
    assert s.airflow_api_url == "http://airflow-apiserver:8080"
    assert str(s.dbt_project_dir) == "/opt/drct/dbt"


def test_get_settings_reads_overrides(env):
    env.setenv("DRCT_SEED", "7")
    env.setenv("FX_SOURCE", "stub")
    env.setenv("AIRFLOW_API_URL", "http://localhost:8080")
    env.setenv("DBT_PROJECT_DIR", "/tmp/dbt")
    s = get_settings()
    assert (s.seed, s.fx_source) == (7, "stub")
    assert s.airflow_api_url == "http://localhost:8080"
    assert str(s.dbt_project_dir) == "/tmp/dbt"


def test_get_settings_rejects_bad_fx_source(env):
    env.setenv("FX_SOURCE", "carrier-pigeon")
    with pytest.raises(ValueError, match="FX_SOURCE"):
        get_settings()


def test_get_settings_rejects_non_integer_seed(env):
    env.setenv("DRCT_SEED", "abc")
    with pytest.raises(ValueError, match="DRCT_SEED"):
        get_settings()


def test_get_logger_emits_json_lines_with_bound_keys(capsys):
    get_logger("drct.test", dag_id="d1").info("hello", rows=3)
    line = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert line["event"] == "hello"
    assert line["level"] == "info"
    assert line["dag_id"] == "d1"
    assert line["rows"] == 3
    assert "timestamp" in line
