import os
import uuid
from pathlib import Path

import psycopg
import pytest
from psycopg.conninfo import make_conninfo

INIT_DIR = Path(
    os.environ.get("WAREHOUSE_INIT_DIR", Path(__file__).resolve().parents[1] / "warehouse" / "init")
)


@pytest.fixture
def db_conn():
    """Fresh, throwaway database with all warehouse init SQL applied (autocommit connection)."""
    admin_dsn = os.environ["TEST_ADMIN_DSN"]
    db_name = f"drct_test_{uuid.uuid4().hex}"
    with psycopg.connect(admin_dsn, autocommit=True) as admin:
        admin.execute(f'create database "{db_name}"')
    conn = psycopg.connect(make_conninfo(admin_dsn, dbname=db_name), autocommit=True)
    try:
        for sql_file in sorted(INIT_DIR.glob("*.sql")):
            conn.execute(sql_file.read_text())
        yield conn
    finally:
        conn.close()
        with psycopg.connect(admin_dsn, autocommit=True) as admin:
            admin.execute(f'drop database if exists "{db_name}" with (force)')
