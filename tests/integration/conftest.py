import os
from pathlib import Path
from typing import ClassVar

import pytest
from psycopg.conninfo import make_conninfo

from drct.common.config import Settings


@pytest.fixture
def settings_tmp(db_conn, tmp_path: Path) -> Settings:
    """Settings pointing at the throwaway test database and a temporary landing directory."""
    dsn = make_conninfo(os.environ["TEST_ADMIN_DSN"], dbname=db_conn.info.dbname)
    return Settings(
        warehouse_dsn=dsn,
        landing_dir=tmp_path,
        seed=42,
        fx_source="stub",
        fx_api_url="http://fx.invalid",
        airflow_api_url="http://airflow.invalid",
        dbt_project_dir=tmp_path / "dbt",
    )


class FakeAirflowClient:
    """Stands in for ``AirflowClient`` in CLI tests: records triggers, never talks to Airflow."""

    instances: ClassVar[list["FakeAirflowClient"]] = []

    def __init__(self, base_url: str) -> None:
        self.triggered: list[tuple[str, dict | None]] = []
        FakeAirflowClient.instances.append(self)

    def trigger_dag(self, dag_id: str, conf: dict | None = None) -> str:
        self.triggered.append((dag_id, conf))
        return "manual__1"

    def wait_until_idle(self, **kwargs) -> None:
        return None

    @classmethod
    def triggered_dags(cls) -> list[str]:
        return [dag_id for c in cls.instances for dag_id, _ in c.triggered]


@pytest.fixture
def cli_env(settings_tmp: Settings, monkeypatch) -> type[FakeAirflowClient]:
    """Point the ops CLI at the test database and landing dir, with a fake Airflow client.

    Returns the fake client class; its ``triggered_dags()`` lists every DAG the CLI triggered.
    """
    from drct.ops import commands

    FakeAirflowClient.instances.clear()
    monkeypatch.setattr(commands, "AirflowClient", FakeAirflowClient)
    monkeypatch.setenv("WAREHOUSE_DSN", settings_tmp.warehouse_dsn)
    monkeypatch.setenv("LANDING_DIR", str(settings_tmp.landing_dir))
    monkeypatch.setenv("DRCT_SEED", str(settings_tmp.seed))
    monkeypatch.setenv("FX_API_URL", settings_tmp.fx_api_url)
    monkeypatch.setenv("AIRFLOW_API_URL", settings_tmp.airflow_api_url)
    return FakeAirflowClient
