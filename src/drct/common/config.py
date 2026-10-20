"""Process-wide settings, read from environment variables."""

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, cast

FX_SOURCES = ("api", "stub")


@dataclass(frozen=True)
class Settings:
    warehouse_dsn: str
    landing_dir: Path
    seed: int
    fx_source: Literal["api", "stub"]
    fx_api_url: str
    airflow_api_url: str
    dbt_project_dir: Path


def _required(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def get_settings() -> Settings:
    """Build Settings from the environment; raise ValueError naming any missing/invalid var."""
    warehouse_dsn = _required("WAREHOUSE_DSN")
    landing_dir = _required("LANDING_DIR")
    fx_api_url = _required("FX_API_URL")

    raw_seed = os.environ.get("DRCT_SEED", "42")
    try:
        seed = int(raw_seed)
    except ValueError:
        raise ValueError(f"DRCT_SEED must be an integer, got {raw_seed!r}") from None

    fx_source = os.environ.get("FX_SOURCE", "api")
    if fx_source not in FX_SOURCES:
        raise ValueError(f"FX_SOURCE must be one of {FX_SOURCES}, got {fx_source!r}")

    return Settings(
        warehouse_dsn=warehouse_dsn,
        landing_dir=Path(landing_dir),
        seed=seed,
        fx_source=cast(Literal["api", "stub"], fx_source),
        fx_api_url=fx_api_url,
        airflow_api_url=os.environ.get("AIRFLOW_API_URL", "http://airflow-apiserver:8080"),
        dbt_project_dir=Path(os.environ.get("DBT_PROJECT_DIR", "/opt/drct/dbt")),
    )
