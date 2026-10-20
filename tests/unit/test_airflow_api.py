import json
from pathlib import Path

import httpx
import pytest

from drct.common.airflow_api import AirflowClient


def fake_airflow(active_runs: list[int], calls: list[httpx.Request] | None = None):
    """Transport whose dagRuns listing reports ``active_runs[i]`` runs on the i-th poll (the last
    value repeats)."""
    polls = iter(active_runs)
    last = {"n": active_runs[-1]}

    def handler(request: httpx.Request) -> httpx.Response:
        if calls is not None:
            calls.append(request)
        path = request.url.path
        if path == "/auth/token":
            return httpx.Response(201, json={"access_token": "tok"})
        if request.headers.get("authorization") != "Bearer tok":
            return httpx.Response(401, json={"detail": "Not authenticated"})
        if request.method == "POST" and path == "/api/v2/dags/reprocess/dagRuns":
            return httpx.Response(200, json={"dag_run_id": "manual__1", "state": "queued"})
        if request.method == "GET" and path == "/api/v2/dags/~/dagRuns":
            last["n"] = next(polls, last["n"])
            return httpx.Response(200, json={"dag_runs": [], "total_entries": last["n"]})
        return httpx.Response(404)

    return httpx.MockTransport(handler)


def client(transport) -> AirflowClient:
    return AirflowClient("http://x", transport=transport)


def test_trigger_dag_sends_conf_and_null_logical_date_and_returns_run_id():
    calls: list[httpx.Request] = []
    run_id = client(fake_airflow([0], calls)).trigger_dag(
        "reprocess", {"start": "2026-07-01", "end": "2026-07-02"}
    )
    assert run_id == "manual__1"
    post = next(c for c in calls if c.method == "POST" and c.url.path.endswith("/dagRuns"))
    assert json.loads(post.content) == {
        "logical_date": None,
        "conf": {"start": "2026-07-01", "end": "2026-07-02"},
    }
    assert post.headers["authorization"] == "Bearer tok"


def test_trigger_dag_without_conf_sends_empty_conf():
    calls: list[httpx.Request] = []
    client(fake_airflow([0], calls)).trigger_dag("reprocess")
    post = next(c for c in calls if c.method == "POST" and c.url.path.endswith("/dagRuns"))
    assert json.loads(post.content)["conf"] == {}


def test_wait_until_idle_times_out_when_runs_stay_active():
    with pytest.raises(TimeoutError):
        client(fake_airflow([1])).wait_until_idle(timeout_s=0.1)


def test_wait_until_idle_queries_queued_and_running_runs_of_all_dags():
    calls: list[httpx.Request] = []
    client(fake_airflow([0], calls)).wait_until_idle(settle_polls=1, poll_interval_s=0)
    listing = next(c for c in calls if c.url.path == "/api/v2/dags/~/dagRuns")
    assert listing.url.params.get_list("state") == ["queued", "running"]


def test_wait_until_idle_needs_consecutive_idle_polls():
    calls: list[httpx.Request] = []
    # idle, busy, idle, idle -> only the last two are consecutive: 4 polls to settle on 2.
    transport = fake_airflow([0, 1, 0, 0], calls)
    client(transport).wait_until_idle(settle_polls=2, poll_interval_s=0)
    polls = [c for c in calls if c.url.path == "/api/v2/dags/~/dagRuns"]
    assert len(polls) == 4


def test_wait_until_idle_treats_pending_landing_files_as_busy(tmp_path: Path):
    (tmp_path / "sales").mkdir()
    (tmp_path / "sales" / "sales_2026-07-01.csv").write_text("x")
    with pytest.raises(TimeoutError):
        client(fake_airflow([0])).wait_until_idle(
            timeout_s=0.1, poll_interval_s=0, landing_dir=tmp_path
        )


def test_wait_until_idle_ignores_processed_landing_files(tmp_path: Path):
    (tmp_path / "sales" / "_processed").mkdir(parents=True)
    (tmp_path / "sales" / "_processed" / "sales_2026-07-01.csv").write_text("x")
    client(fake_airflow([0])).wait_until_idle(
        settle_polls=1, poll_interval_s=0, landing_dir=tmp_path
    )


def dag_not_parsed_yet(not_found_posts: int, calls: list[httpx.Request]):
    """Transport whose first ``not_found_posts`` trigger POSTs get DagNotFound's 404."""

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        if request.url.path == "/auth/token":
            return httpx.Response(201, json={"access_token": "tok"})
        posts = sum(1 for c in calls if c.method == "POST")
        if posts <= not_found_posts:
            return httpx.Response(404, json={"detail": "The Dag with dag_id: `x` was not found"})
        return httpx.Response(200, json={"dag_run_id": "manual__2"})

    return httpx.MockTransport(handler)


def test_trigger_dag_retries_while_dag_is_not_found():
    calls: list[httpx.Request] = []
    run_id = client(dag_not_parsed_yet(2, calls)).trigger_dag("ingest_fx", poll_interval_s=0)
    assert run_id == "manual__2"
    assert sum(1 for c in calls if c.method == "POST") == 3


def test_trigger_dag_gives_up_on_404_after_timeout():
    calls: list[httpx.Request] = []
    with pytest.raises(httpx.HTTPStatusError) as excinfo:
        client(dag_not_parsed_yet(10_000, calls)).trigger_dag(
            "ingest_fx", not_found_timeout_s=0.05, poll_interval_s=0.01
        )
    assert excinfo.value.response.status_code == 404
    assert sum(1 for c in calls if c.method == "POST") >= 2


def test_trigger_dag_does_not_retry_other_errors():
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        if request.url.path == "/auth/token":
            return httpx.Response(201, json={"access_token": "tok"})
        return httpx.Response(400, json={"detail": "bad conf"})

    with pytest.raises(httpx.HTTPStatusError):
        client(httpx.MockTransport(handler)).trigger_dag("ingest_fx", poll_interval_s=0)
    assert sum(1 for c in calls if c.method == "POST") == 1


def test_get_dag_run_state_reads_the_run():
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        if request.url.path == "/auth/token":
            return httpx.Response(201, json={"access_token": "tok"})
        if request.url.path == "/api/v2/dags/reprocess/dagRuns/manual__1":
            return httpx.Response(200, json={"dag_run_id": "manual__1", "state": "failed"})
        return httpx.Response(404)

    state = client(httpx.MockTransport(handler)).get_dag_run_state("reprocess", "manual__1")
    assert state == "failed"
    assert calls[-1].method == "GET"


def test_get_dag_run_state_escapes_the_run_id():
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/auth/token":
            return httpx.Response(201, json={"access_token": "tok"})
        seen.append(request.url.raw_path.decode())
        return httpx.Response(200, json={"state": "running"})

    run_id = "manual__2026-10-20T01:49:13.232476+00:00"
    assert client(httpx.MockTransport(handler)).get_dag_run_state("monitoring", run_id) == "running"
    assert seen == [
        "/api/v2/dags/monitoring/dagRuns/manual__2026-10-20T01%3A49%3A13.232476%2B00%3A00"
    ]


def api_server_starting(refused_requests: int, calls: list[httpx.Request]):
    """Transport that refuses the first ``refused_requests`` connections (uvicorn not listening)."""

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        if len(calls) <= refused_requests:
            raise httpx.ConnectError("[Errno 111] Connection refused", request=request)
        if request.url.path == "/auth/token":
            return httpx.Response(201, json={"access_token": "tok"})
        return httpx.Response(200, json={"dag_run_id": "manual__3"})

    return httpx.MockTransport(handler)


def test_trigger_dag_retries_while_the_api_server_refuses_connections():
    calls: list[httpx.Request] = []
    run_id = client(api_server_starting(3, calls)).trigger_dag("ingest_fx", poll_interval_s=0)
    assert run_id == "manual__3"


def test_trigger_dag_gives_up_on_refused_connections_after_timeout():
    calls: list[httpx.Request] = []
    with pytest.raises(httpx.ConnectError):
        client(api_server_starting(10_000, calls)).trigger_dag(
            "ingest_fx", not_found_timeout_s=0.05, poll_interval_s=0.01
        )
    assert len(calls) >= 2


def run_states(states: list[str], calls: list[httpx.Request] | None = None):
    """Transport whose single DAG run reports ``states[i]`` on the i-th poll (the last repeats)."""
    polls = iter(states)
    last = {"state": states[-1]}

    def handler(request: httpx.Request) -> httpx.Response:
        if calls is not None:
            calls.append(request)
        if request.url.path == "/auth/token":
            return httpx.Response(201, json={"access_token": "tok"})
        last["state"] = next(polls, last["state"])
        return httpx.Response(200, json={"dag_run_id": "manual__1", "state": last["state"]})

    return httpx.MockTransport(handler)


@pytest.mark.parametrize("final", ["success", "failed"])
def test_wait_for_run_polls_until_the_run_is_terminal(final):
    calls: list[httpx.Request] = []
    state = client(run_states(["queued", "running", final], calls)).wait_for_run(
        "reprocess", "manual__1", poll_interval_s=0
    )
    assert state == final
    polls = [c for c in calls if c.url.path == "/api/v2/dags/reprocess/dagRuns/manual__1"]
    assert len(polls) == 3


def test_wait_for_run_times_out_while_the_run_is_active():
    with pytest.raises(TimeoutError, match="still running"):
        client(run_states(["running"])).wait_for_run(
            "monitoring", "manual__1", timeout_s=0.05, poll_interval_s=0.01
        )


def flaky_trigger(failures: list[int | type[Exception]], calls: list[httpx.Request]):
    """Transport whose first trigger POSTs fail with ``failures`` in turn (a status code or an
    exception class), then succeed."""
    outcomes = iter(failures)

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        if request.url.path == "/auth/token":
            return httpx.Response(201, json={"access_token": "tok"})
        failure = next(outcomes, None)
        if isinstance(failure, int):
            return httpx.Response(failure, json={"detail": "boom"})
        if failure is not None:
            raise failure("timed out", request=request)
        return httpx.Response(200, json={"dag_run_id": "manual__4"})

    return httpx.MockTransport(handler)


def test_trigger_dag_retries_server_errors_and_timeouts():
    """Ledger L204: a busy api-server's 5xx or ReadTimeout must not fail `make seed`/`demo`."""
    calls: list[httpx.Request] = []
    transport = flaky_trigger([500, 503, httpx.ReadTimeout, httpx.ConnectTimeout], calls)
    assert client(transport).trigger_dag("ingest_fx", poll_interval_s=0) == "manual__4"
    assert sum(1 for c in calls if c.method == "POST") == 5


def test_trigger_dag_gives_up_on_server_errors_after_timeout():
    calls: list[httpx.Request] = []
    with pytest.raises(httpx.HTTPStatusError) as excinfo:
        client(flaky_trigger([502] * 10_000, calls)).trigger_dag(
            "ingest_fx", not_found_timeout_s=0.05, poll_interval_s=0.01
        )
    assert excinfo.value.response.status_code == 502
