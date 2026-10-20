"""Volume anomaly detection: daily raw row counts scored with a robust (median/MAD) z-score.

Generic by design: every source is scored the same way from its own history, and nothing here
knows about injected chaos.
"""

import statistics
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime

import psycopg
from psycopg import sql

from drct.common.logging import get_logger

__all__ = ["AnomalyResult", "compute_volume_metrics", "detect_anomaly", "evaluate_volume"]

log = get_logger("drct.monitoring.volume")

SOURCES = ("sales", "payments")
_Z_SCALE = 0.6745  # makes MAD comparable to a standard deviation for normal data
# Floor on the MAD as a share of the median, used by evaluate_volume. Eight same-weekday samples sometimes give a MAD of
# ~2% of the median, so ordinary +-12% days scored |z| > 3.5: on 400 generated clean days the bare
# MAD flagged 7/351 sales and 8/351 payments days (worst |z| 10.6); with this floor, none (worst
# 2.3). An x8 spike still scores z ~ 90.
MIN_MAD_RATIO = 0.05


@dataclass(frozen=True)
class AnomalyResult:
    value: int
    baseline_median: float | None
    mad: float | None
    robust_z: float | None
    is_anomaly: bool
    reason: str  # insufficient_history | spike | drop | normal


def detect_anomaly(
    history: Sequence[int],
    value: int,
    threshold: float = 3.5,
    min_history: int = 7,
    min_mad_ratio: float = 0.0,
) -> AnomalyResult:
    """Flag ``value`` when ``|0.6745 * (value - median) / MAD| > threshold``.

    A zero MAD (flat history) falls back to ``max(1.0, 0.05 * median)`` so it neither divides
    by zero nor flags every tiny wobble.
    ``min_mad_ratio`` also floors a non-zero MAD at that share of the median, for small samples
    whose MAD comes out implausibly tight.
    """
    if len(history) < min_history:
        return AnomalyResult(value, None, None, None, False, "insufficient_history")
    median = float(statistics.median(history))
    mad = float(statistics.median(abs(x - median) for x in history))
    if mad == 0:
        mad = max(1.0, 0.05 * median)
    mad = max(mad, min_mad_ratio * median)
    z = _Z_SCALE * (value - median) / mad
    if abs(z) > threshold:
        return AnomalyResult(value, median, mad, z, True, "spike" if z > 0 else "drop")
    return AnomalyResult(value, median, mad, z, False, "normal")


def compute_volume_metrics(conn: psycopg.Connection, sources: Sequence[str] = SOURCES) -> int:
    """Upsert ``count(*)`` per business date for every date in each raw table.

    All dates are recomputed, so a reprocess that adds rows to an old day is picked up.
    Returns the number of (source, business_date) rows upserted.
    """
    total = 0
    with conn.transaction():
        for source in sources:
            query = sql.SQL(
                """
                insert into monitoring.volume_metrics (source, business_date, row_count, computed_at)
                select %s, business_date, count(*), now()
                  from {table}
                 where business_date is not null
                 group by business_date
                on conflict (source, business_date) do update
                    set row_count = excluded.row_count,
                        computed_at = excluded.computed_at
                """
            ).format(table=sql.Identifier("raw", source))
            total += conn.execute(query, (source,)).rowcount
    log.info("volume_metrics_computed", rows=total)
    return total


def evaluate_volume(
    conn: psycopg.Connection, sim_now: datetime, window: int = 8
) -> list[tuple[str, date, AnomalyResult]]:
    """Score each source's latest business date against the previous ``window`` same-weekday dates.

    Comparing like with like (R23) deseasonalises the check: the generator has weekday seasonality
    (Saturday runs ~1.3x), which a plain 14-day median/MAD window flags as a spike about 5% of the
    time on clean data.

    One row per source goes into ``monitoring.volume_anomalies`` (normal results included).
    """
    results: list[tuple[str, date, AnomalyResult]] = []
    sources = [
        r[0]
        for r in conn.execute(
            "select distinct source from monitoring.volume_metrics order by source"
        ).fetchall()
    ]
    with conn.transaction():
        for source in sources:
            latest = conn.execute(
                "select business_date, row_count from monitoring.volume_metrics"
                " where source = %s order by business_date desc limit 1",
                (source,),
            ).fetchone()
            business_date, row_count = latest
            history = [
                r[0]
                for r in conn.execute(
                    "select row_count from monitoring.volume_metrics"
                    " where source = %s and business_date < %s"
                    " and extract(isodow from business_date) = extract(isodow from %s::date)"
                    " order by business_date desc limit %s",
                    (source, business_date, business_date, window),
                ).fetchall()
            ]
            result = detect_anomaly(history, int(row_count), min_mad_ratio=MIN_MAD_RATIO)
            conn.execute(
                """
                insert into monitoring.volume_anomalies
                    (source, business_date, row_count, baseline_median, mad, robust_z,
                     is_anomaly, reason, sim_now, evaluated_at)
                values (%s, %s, %s, %s, %s, %s, %s, %s, %s, now())
                """,
                (
                    source,
                    business_date,
                    result.value,
                    result.baseline_median,
                    result.mad,
                    result.robust_z,
                    result.is_anomaly,
                    result.reason,
                    sim_now,
                ),
            )
            results.append((source, business_date, result))
    return results
