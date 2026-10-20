"""The Control Tower renders on a fresh install, without a database behind it."""

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Self

import psycopg

from drct.control_tower.queries import readable_node

# AppTest resolves relative paths against the calling file, so anchor at the repo root.
APP = str(Path(__file__).resolve().parents[2] / "src" / "drct" / "control_tower" / "app.py")


class _EmptyCursor:
    def fetchall(self) -> list:
        return []

    def fetchone(self) -> None:
        return None


class _EmptyConnection:
    """Answers every query the way an empty warehouse with no sim clock would."""

    @contextmanager
    def transaction(self) -> Iterator[None]:
        yield

    def execute(self, query, params=None) -> _EmptyCursor:
        if "sim_now()" in str(query):
            raise psycopg.errors.RaiseException("control.sim_clock is not initialised")
        return _EmptyCursor()

    def close(self) -> None:
        pass

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc) -> None:
        self.close()


def empty_db_connection(dsn: str | None = None) -> _EmptyConnection:
    return _EmptyConnection()


def test_app_renders_without_exceptions_on_empty_db(monkeypatch):
    from streamlit.testing.v1 import AppTest

    monkeypatch.setenv("CONTROL_TOWER_DSN", "postgresql://unused")
    monkeypatch.setattr("drct.control_tower.queries.connect", empty_db_connection)
    at = AppTest.from_file(APP).run()
    assert not at.exception


def test_app_shows_empty_state_on_every_panel(monkeypatch):
    from streamlit.testing.v1 import AppTest

    monkeypatch.setenv("CONTROL_TOWER_DSN", "postgresql://unused")
    monkeypatch.setattr("drct.control_tower.queries.connect", empty_db_connection)
    at = AppTest.from_file(APP).run()
    empty_states = [m for m in at.markdown if "No data yet" in m.value]
    assert len(empty_states) == 6


def test_readable_node_names():
    assert readable_node("model.drct.fct_payments") == "fct_payments"
    assert readable_node("exposure.drct.control_tower") == "Control Tower (dashboard)"
    assert readable_node("source.drct.raw.sales") == "raw.sales"
