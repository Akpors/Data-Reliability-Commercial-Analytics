"""Minimal Airflow 3 REST client for the ops CLI: trigger a DAG and wait for the pipeline to drain."""

import time
from pathlib import Path
from typing import Any
from urllib.parse import quote

import httpx

_DAG_RUNS = "/api/v2/dags/{dag_id}/dagRuns"
_ACTIVE_STATES = ("queued", "running")
_TERMINAL_STATES = ("success", "failed")


class AirflowClient:
    """Talks to the Airflow api-server (``http://airflow-apiserver:8080`` inside the compose network)."""

    def __init__(self, base_url: str, transport: httpx.BaseTransport | None = None) -> None:
        self._http = httpx.Client(base_url=base_url, transport=transport, timeout=30.0)
        self._token: str | None = None

    def token(self) -> str:
        """A JWT for the REST API.

        With the SimpleAuthManager in all-admins mode ``GET /auth/token`` issues an admin token
        without credentials (the ``POST`` form insists on a username and password body).
        """
        response = self._http.get("/auth/token")
        response.raise_for_status()
        self._token = response.json()["access_token"]
        return self._token

    def _request(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        if self._token is None:
            self.token()
        for attempt in (1, 2):
            response = self._http.request(
                method, url, headers={"Authorization": f"Bearer {self._token}"}, **kwargs
            )
            if response.status_code != 401 or attempt == 2:
                break
            self.token()  # expired token: fetch a fresh one and retry once
        response.raise_for_status()
        return response

    def trigger_dag(
        self,
        dag_id: str,
        conf: dict | None = None,
        not_found_timeout_s: float = 60.0,
        poll_interval_s: float = 2.0,
    ) -> str:
        """Start a run of ``dag_id`` with ``conf``; returns its ``dag_run_id``.

        Right after ``make up`` the api-server may not be listening yet (connection refused) and the
        dag-processor may not have parsed the DAG yet (404 DagNotFound); a busy api-server may time
        out or answer 5xx. All of these are retried for up to ``not_found_timeout_s``; other errors
        raise at once. A timed-out request may still have created its run, so a retry can start a
        second one: every DAG triggered this way runs one at a time and reloads idempotently.
        """
        deadline = time.monotonic() + not_found_timeout_s
        while True:
            try:
                response = self._request(
                    "POST",
                    _DAG_RUNS.format(dag_id=dag_id),
                    json={"logical_date": None, "conf": conf or {}},
                )
            except httpx.HTTPStatusError as exc:
                status = exc.response.status_code
                if not (status == 404 or status >= 500) or time.monotonic() >= deadline:
                    raise
                time.sleep(poll_interval_s)
                continue
            except (httpx.ConnectError, httpx.TimeoutException):
                if time.monotonic() >= deadline:
                    raise
                time.sleep(poll_interval_s)
                continue
            return response.json()["dag_run_id"]

    def get_dag_run_state(self, dag_id: str, run_id: str) -> str:
        """The state of one DAG run ('queued', 'running', 'success', 'failed'...)."""
        url = _DAG_RUNS.format(dag_id=dag_id) + "/" + quote(run_id, safe="")
        return str(self._request("GET", url).json()["state"])

    def wait_for_run(
        self, dag_id: str, run_id: str, timeout_s: float = 900, poll_interval_s: float = 5.0
    ) -> str:
        """Block until the run reaches a terminal state and return it. Raises ``TimeoutError``."""
        deadline = time.monotonic() + timeout_s
        while True:
            state = self.get_dag_run_state(dag_id, run_id)
            if state in _TERMINAL_STATES:
                return state
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(f"{dag_id} run {run_id} still {state} after {timeout_s:g}s")
            time.sleep(min(poll_interval_s, remaining))

    def active_run_count(self) -> int:
        """Queued plus running DAG runs across every DAG."""
        response = self._request(
            "GET",
            _DAG_RUNS.format(dag_id="~"),
            params=[("state", s) for s in _ACTIVE_STATES] + [("limit", 1)],
        )
        return int(response.json()["total_entries"])

    def wait_until_idle(
        self,
        timeout_s: float = 600,
        settle_polls: int = 3,
        landing_dir: Path | None = None,
        poll_interval_s: float = 5.0,
    ) -> None:
        """Block until no DAG run is queued/running (and, with ``landing_dir``, no landing file
        is pending) for ``settle_polls`` consecutive polls. Raises ``TimeoutError``.

        Asset-triggered DAGs start a moment after their upstream finishes, so a single idle poll
        can fall in the gap between two runs; requiring consecutive idle polls bridges it.
        """
        deadline = time.monotonic() + timeout_s
        idle_polls = 0
        while True:
            busy = self.active_run_count() > 0 or (
                landing_dir is not None and _pending_landing_files(landing_dir)
            )
            idle_polls = 0 if busy else idle_polls + 1
            if idle_polls >= settle_polls:
                return
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(f"Airflow still busy after {timeout_s:g}s")
            time.sleep(min(poll_interval_s, remaining))


def _pending_landing_files(landing_dir: Path) -> bool:
    from drct.ingestion.loader import SOURCES, pending_files

    return any(pending_files(landing_dir, source) for source in SOURCES)
