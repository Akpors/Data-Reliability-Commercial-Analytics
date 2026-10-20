"""Idempotent raw loader.

``load_file`` validates one landing file against its contract, records a schema event and, unless
the diff is breaking, replaces that business date's rows in ``raw.<source>``. It never moves
files: ``load_pending`` and ``reprocess_files`` do, and quarantine goes through ``quarantine_file``.
"""

import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Literal

import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb

from drct.common.logging import get_logger
from drct.ingestion.contracts import SchemaDiff, canonicalise, diff_schema, load_contract
from drct.ingestion.quarantine import move_file, quarantine_file
from drct.ingestion.readers import parse_business_date, read_landing_file

__all__ = [
    "SOURCES",
    "LoadResult",
    "load_file",
    "load_pending",
    "parse_business_date",
    "pending_files",
    "reprocess_files",
]

SOURCES = ("sales", "payments", "customers", "products")
PROCESSED_DIR = "_processed"
RESEND_DIR = "_resend"
_EXTENSIONS = ("csv", "json", "parquet")

log = get_logger("drct.ingestion.loader")


@dataclass
class LoadResult:
    source: str
    path: Path
    business_date: date
    status: Literal["loaded", "quarantined"]
    rows_in: int
    rows_loaded: int
    diff: SchemaDiff


def _record_schema_event(
    conn: psycopg.Connection,
    source: str,
    file_name: str,
    contract_version: int,
    diff: SchemaDiff,
    sim_now: datetime,
) -> None:
    conn.execute(
        """
        insert into monitoring.schema_events
            (source, file_name, contract_version, added, missing, type_mismatches, aliased,
             is_breaking, sim_now)
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            source,
            file_name,
            contract_version,
            diff.added,
            diff.missing,
            Jsonb(diff.type_mismatches),
            Jsonb(diff.aliased),
            diff.is_breaking,
            sim_now,
        ),
    )


def load_file(
    conn: psycopg.Connection, source: str, path: Path, sim_now: datetime, batch_id: str
) -> LoadResult:
    """Validate ``path`` against the ``source`` contract and load it into ``raw.<source>``.

    A schema event is always recorded, clean or not. A breaking diff returns ``quarantined``
    without touching the table (the caller moves the file). Otherwise the rows of the file's
    business date are replaced, in one transaction, so reloading a file is idempotent.
    """
    contract = load_contract(source)
    business_date = parse_business_date(path)
    df = read_landing_file(path)
    diff = diff_schema(contract, df)
    _record_schema_event(conn, source, path.name, contract.version, diff, sim_now)
    if diff.is_breaking:
        return LoadResult(source, path, business_date, "quarantined", len(df), 0, diff)

    frame = canonicalise(contract, df)
    source_columns = [c.name for c in contract.columns]
    columns = [
        *source_columns,
        "business_date",
        "_sim_loaded_at",
        "_source_file",
        "_batch_id",
        "_extra",
    ]
    table = sql.Identifier("raw", source)
    with conn.transaction():
        conn.execute(
            sql.SQL("delete from {} where business_date = %s").format(table), (business_date,)
        )
        copy_sql = sql.SQL("copy {} ({}) from stdin").format(
            table, sql.SQL(", ").join(map(sql.Identifier, columns))
        )
        # _loaded_at is left to its column default, now().
        rows_loaded = 0
        with conn.cursor() as cur, cur.copy(copy_sql) as copy:
            for row in frame.itertuples(index=False, name=None):
                copy.write_row((*row[:-1], business_date, sim_now, path.name, batch_id, row[-1]))
                rows_loaded += 1
    log.info("file_loaded", source=source, file=path.name, rows=rows_loaded, batch_id=batch_id)
    return LoadResult(source, path, business_date, "loaded", len(df), rows_loaded, diff)


def pending_files(landing_dir: Path, source: str) -> list[Path]:
    """Top-level landing files of ``source``, oldest business date first.

    Dotfiles, temp files, other names and sub-folders (``_processed`` etc.) are ignored.
    """
    folder = landing_dir / source
    if not folder.is_dir():
        return []
    name = re.compile(rf"{re.escape(source)}_\d{{4}}-\d{{2}}-\d{{2}}\.({'|'.join(_EXTENSIONS)})")
    found = []
    for path in folder.iterdir():
        if path.is_file() and name.fullmatch(path.name):
            try:
                found.append((parse_business_date(path), path))
            except ValueError:  # matches the shape but is not a calendar date
                continue
    return [path for _, path in sorted(found)]


def _settle(
    conn: psycopg.Connection, result: LoadResult, sim_now: datetime, landing_dir: Path
) -> None:
    """File the outcome: loaded files go to ``_processed/``, breaking ones to quarantine."""
    source, path = result.source, result.path
    if result.status == "loaded":
        move_file(path, landing_dir / source / PROCESSED_DIR)
    else:
        quarantine_file(
            conn, source, path, result.diff, load_contract(source).version, sim_now, landing_dir
        )


def load_pending(
    conn: psycopg.Connection, source: str, landing_dir: Path, sim_now: datetime, batch_id: str
) -> list[LoadResult]:
    """Load every pending file of ``source``, oldest first, and file each away by its outcome."""
    results = []
    for path in pending_files(landing_dir, source):
        result = load_file(conn, source, path, sim_now, batch_id)
        _settle(conn, result, sim_now, landing_dir)
        results.append(result)
    return results


def reprocess_files(
    conn: psycopg.Connection,
    landing_dir: Path,
    start: date,
    end: date,
    sim_now: datetime,
    batch_id: str,
    sources: tuple[str, ...] = SOURCES,
) -> list[LoadResult]:
    """Reload the business dates ``start``..``end`` (inclusive) from ``_resend/`` or ``_processed/``.

    A ``_resend/`` file wins over the ``_processed/`` one, and once used replaces it. Dates with
    no file are skipped. A breaking file is quarantined like any other.
    """
    results = []
    day = start
    while day <= end:
        for source in sources:
            name = f"{source}_{day.isoformat()}.{load_contract(source).file_format}"
            folder = landing_dir / source
            path = next(
                (
                    p
                    for p in (folder / RESEND_DIR / name, folder / PROCESSED_DIR / name)
                    if p.is_file()
                ),
                None,
            )
            if path is None:
                continue
            result = load_file(conn, source, path, sim_now, batch_id)
            _settle(conn, result, sim_now, landing_dir)
            results.append(result)
        day += timedelta(days=1)
    return results
