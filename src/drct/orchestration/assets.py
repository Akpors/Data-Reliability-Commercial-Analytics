"""The Airflow Assets of the pipeline.

``LANDING_ASSETS`` are watched folders: a ``FileArrivalTrigger`` per source turns file arrivals
into asset events (event-driven scheduling, no cron). ``RAW_ASSETS`` are the warehouse tables the
ingest DAGs produce; ``MARTS_ASSET`` and ``DBT_RESULTS_ASSET`` are produced downstream by dbt.
"""

from airflow.sdk import Asset, AssetWatcher

from drct.orchestration.triggers import FileArrivalTrigger

FILE_SOURCES = ("sales", "payments", "customers", "products")

LANDING_ROOT = "/opt/drct/landing"
WAREHOUSE_URI = "postgres://warehouse:5432/warehouse"
_FILE_EXTENSIONS = {"sales": "csv", "payments": "parquet", "customers": "json", "products": "json"}

RAW_ASSETS: dict[str, Asset] = {
    s: Asset(name=f"raw.{s}", uri=f"{WAREHOUSE_URI}/raw/{s}") for s in (*FILE_SOURCES, "fx_rates")
}

LANDING_ASSETS: dict[str, Asset] = {
    s: Asset(
        name=f"landing.{s}",
        uri=f"file://{LANDING_ROOT}/{s}",
        watchers=[
            AssetWatcher(
                name=f"watch_{s}",
                trigger=FileArrivalTrigger(
                    directory=f"{LANDING_ROOT}/{s}", pattern=f"{s}_*.{_FILE_EXTENSIONS[s]}"
                ),
            )
        ],
    )
    for s in FILE_SOURCES
}

MARTS_ASSET = Asset(name="marts", uri=f"{WAREHOUSE_URI}/marts/all")
DBT_RESULTS_ASSET = Asset(
    name="monitoring.dbt_results", uri=f"{WAREHOUSE_URI}/monitoring/dbt_results"
)
