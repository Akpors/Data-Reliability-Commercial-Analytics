"""Parse dbt's JSON artifacts and publish them into the ``monitoring`` schema.

``publish_dbt_artifacts`` runs after every ``dbt build`` (pass or fail) and
``publish_freshness`` after ``dbt source freshness``. Both tolerate missing or half-written
artifacts (published as 0 rows with a warning), because dbt can fail before writing them or be
rewriting them.
"""

import json
from dataclasses import astuple, dataclass
from datetime import datetime
from pathlib import Path
from typing import Literal

import psycopg

from drct.common.logging import get_logger

__all__ = [
    "FreshnessResult",
    "TestResult",
    "parse_run_results",
    "parse_sources",
    "publish_dbt_artifacts",
    "publish_freshness",
]

log = get_logger("drct.monitoring.dbt_artifacts")

TestStatus = Literal["pass", "fail", "warn", "error", "skipped"]
FreshnessStatus = Literal["pass", "warn", "error", "runtime error"]


@dataclass(frozen=True)
class TestResult:
    __test__ = False  # not a pytest test class despite the name

    unique_id: str
    test_name: str
    test_type: str
    attached_node: str | None
    status: TestStatus
    severity: str
    failures: int | None
    message: str | None
    compiled_sql: str | None


@dataclass(frozen=True)
class FreshnessResult:
    source: str
    max_loaded_at: datetime | None
    snapshotted_at: datetime | None
    age_seconds: float | None
    status: FreshnessStatus


def _parse_ts(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value else None


def parse_run_results(run_results: dict, manifest: dict) -> list[TestResult]:
    """Return one ``TestResult`` per dbt test in ``run_results`` (models, exposures etc. are skipped)."""
    nodes = manifest.get("nodes", {})
    out = []
    for result in run_results.get("results", []):
        unique_id = result["unique_id"]
        if not unique_id.startswith("test."):
            continue
        node = nodes.get(unique_id, {})
        depends_on = node.get("depends_on", {}).get("nodes", [])
        # dbt lists the referenced model first for `relationships` tests, so prefer the
        # manifest's explicit attached_node (absent for singular tests) over depends_on order.
        attached = node.get("attached_node") or (depends_on[0] if depends_on else None)
        out.append(
            TestResult(
                unique_id=unique_id,
                test_name=node.get("name") or unique_id.split(".")[2],
                test_type=(node.get("test_metadata") or {}).get("name") or "singular",
                attached_node=attached,
                status=result["status"],
                severity=(node.get("config", {}).get("severity") or "error").lower(),
                failures=result.get("failures"),
                message=result.get("message"),
                compiled_sql=result.get("compiled_code"),
            )
        )
    return out


def parse_sources(sources: dict) -> list[FreshnessResult]:
    """Return one ``FreshnessResult`` per source in ``sources.json`` (``raw.sales`` style names)."""
    out = []
    for result in sources.get("results", []):
        _, _, source_name, table_name = result["unique_id"].split(".", 3)
        max_loaded_at = _parse_ts(result.get("max_loaded_at"))
        snapshotted_at = _parse_ts(result.get("snapshotted_at"))
        age = result.get("max_loaded_at_time_ago_in_s")
        if age is None and max_loaded_at and snapshotted_at:
            age = (snapshotted_at - max_loaded_at).total_seconds()
        out.append(
            FreshnessResult(
                source=f"{source_name}.{table_name}",
                max_loaded_at=max_loaded_at,
                snapshotted_at=snapshotted_at,
                age_seconds=None if age is None else float(age),
                status=result["status"],
            )
        )
    return out


def _load(path: Path) -> dict | None:
    """The artifact, or None when it is missing or not valid JSON (half-written)."""
    if not path.is_file():
        return None
    try:
        return json.loads(path.read_text())
    except json.JSONDecodeError:
        log.warning("dbt_artifact_unreadable", path=str(path))
        return None


def publish_dbt_artifacts(conn: psycopg.Connection, target_dir: Path, sim_now: datetime) -> int:
    """Insert every dbt test result of the latest invocation into ``monitoring.test_results``.

    Returns the number of rows inserted, or 0 (with a warning) if ``run_results.json`` or
    ``manifest.json`` is missing or half-written.
    """
    run_results = _load(target_dir / "run_results.json")
    manifest = _load(target_dir / "manifest.json")
    if run_results is None or manifest is None:
        log.warning("dbt_artifacts_missing", target_dir=str(target_dir), published=0)
        return 0
    metadata = run_results["metadata"]
    generated_at = _parse_ts(metadata["generated_at"])
    rows = [
        (metadata["invocation_id"], *astuple(r), sim_now, generated_at)
        for r in parse_run_results(run_results, manifest)
    ]
    with conn.transaction(), conn.cursor() as cur:
        cur.executemany(
            "insert into monitoring.test_results (invocation_id, unique_id, test_name, test_type,"
            " attached_node, status, severity, failures, message, compiled_sql, sim_now,"
            " generated_at) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            rows,
        )
    log.info("dbt_test_results_published", invocation_id=metadata["invocation_id"], rows=len(rows))
    return len(rows)


def publish_freshness(conn: psycopg.Connection, target_dir: Path) -> int:
    """Insert one ``monitoring.freshness`` row per source in ``sources.json`` (``checked_at=now()``).

    Returns the number of rows inserted, or 0 (with a warning) if ``sources.json`` is missing or
    half-written.
    """
    sources = _load(target_dir / "sources.json")
    if sources is None:
        log.warning("dbt_sources_missing", target_dir=str(target_dir), published=0)
        return 0
    rows = [astuple(r) for r in parse_sources(sources)]
    with conn.transaction(), conn.cursor() as cur:
        cur.executemany(
            "insert into monitoring.freshness (source, max_loaded_at, snapshotted_at, age_seconds,"
            " status, checked_at) values (%s, %s, %s, %s, %s, now())",
            rows,
        )
    log.info("dbt_freshness_published", rows=len(rows))
    return len(rows)
