"""Quarantine: set breaking files aside, and put them back for another attempt."""

import dataclasses
import os
from datetime import datetime
from pathlib import Path

import psycopg
from psycopg.types.json import Jsonb

from drct.common.logging import get_logger
from drct.ingestion.contracts import SchemaDiff

QUARANTINE_DIR = "_quarantine"

log = get_logger("drct.ingestion.quarantine")


def move_file(path: Path, target_dir: Path) -> Path:
    """Move ``path`` into ``target_dir`` (created if needed), replacing a file of the same name."""
    target_dir.mkdir(parents=True, exist_ok=True)
    target = target_dir / path.name
    if target != path:
        os.replace(path, target)
    return target


def quarantine_file(
    conn: psycopg.Connection,
    source: str,
    path: Path,
    diff: SchemaDiff,
    contract_version: int,
    sim_now: datetime,
    landing_dir: Path,
) -> Path:
    """Move ``path`` to ``landing/<source>/_quarantine/`` and record it in ``quarantine.files``."""
    target_dir = landing_dir / source / QUARANTINE_DIR
    # Record first, then move, in one transaction: a failed insert never leaves an untracked file
    # in _quarantine/, and a failed move rolls the row back.
    with conn.transaction():
        conn.execute(
            """
            insert into quarantine.files
                (source, file_name, stored_path, contract_version, diff, sim_now)
            values (%s, %s, %s, %s, %s, %s)
            """,
            (
                source,
                path.name,
                str(target_dir / path.name),
                contract_version,
                Jsonb(dataclasses.asdict(diff)),
                sim_now,
            ),
        )
        stored = move_file(path, target_dir)
    log.warning("file_quarantined", source=source, file=path.name, missing=diff.missing)
    return stored


@dataclasses.dataclass(frozen=True)
class Replay:
    """What ``replay_quarantine`` did: files moved back to landing, and files it left in place."""

    moved: list[Path]
    untracked: list[Path]


def replay_quarantine(conn: psycopg.Connection, source: str, landing_dir: Path) -> Replay:
    """Move the quarantined files of ``source`` that await replay back to ``landing/<source>/``.

    A file awaits replay when it has a ``quarantine.files`` row with no ``replayed_at``; those rows
    are marked replayed. Any other file in ``_quarantine/`` (left by an earlier stack, or already
    replayed) stays where it is and is reported in ``untracked``, so it is never re-ingested by
    accident.
    """
    folder = landing_dir / source / QUARANTINE_DIR
    if not folder.is_dir():
        return Replay([], [])
    files = sorted(p for p in folder.iterdir() if p.is_file() and not p.name.startswith("."))
    with conn.transaction():
        awaiting = {
            name
            for (name,) in conn.execute(
                "select file_name from quarantine.files where source = %s and replayed_at is null",
                (source,),
            ).fetchall()
        }
        moved = [move_file(p, landing_dir / source) for p in files if p.name in awaiting]
        for path in moved:
            conn.execute(
                """
                update quarantine.files set replayed_at = now()
                where source = %s and file_name = %s and replayed_at is null
                """,
                (source, path.name),
            )
    untracked = [p for p in files if p.name not in awaiting]
    if untracked:
        log.warning("quarantine_files_untracked", source=source, files=[p.name for p in untracked])
    return Replay(moved, untracked)
