"""Warehouse connection helper."""

import psycopg

from drct.common.config import get_settings


def connect(dsn: str | None = None) -> psycopg.Connection:
    """Open an autocommit connection (default: the configured warehouse DSN).

    Callers that perform multi-statement writes wrap them in ``with conn.transaction():``.
    """
    return psycopg.connect(dsn or get_settings().warehouse_dsn, autocommit=True)
