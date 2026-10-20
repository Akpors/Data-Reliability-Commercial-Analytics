from pathlib import Path
from typing import ClassVar

import pytest

from drct.ops import commands


class FakeClient:
    instances: ClassVar[list["FakeClient"]] = []
    final_state: ClassVar[str] = "success"

    def __init__(self, base_url: str) -> None:
        self.base_url = base_url
        self.triggered: list[tuple[str, dict | None]] = []
        self.waited: dict | None = None
        self.calls: list[str] = []
        FakeClient.instances.append(self)

    def trigger_dag(self, dag_id: str, conf: dict | None = None) -> str:
        self.triggered.append((dag_id, conf))
        self.calls.append(f"trigger {dag_id}")
        return "manual__42"

    def wait_for_run(self, dag_id: str, run_id: str, **kwargs) -> str:
        self.calls.append(f"run {dag_id} {run_id}")
        return self.final_state

    def wait_until_idle(self, **kwargs) -> None:
        self.waited = kwargs
        self.calls.append("wait")


@pytest.fixture
def fake_client(monkeypatch, tmp_path):
    FakeClient.instances.clear()
    monkeypatch.setattr(FakeClient, "final_state", "success")
    monkeypatch.setattr(commands, "AirflowClient", FakeClient)
    monkeypatch.setenv("WAREHOUSE_DSN", "postgresql://unused")
    monkeypatch.setenv("LANDING_DIR", str(tmp_path))
    monkeypatch.setenv("FX_API_URL", "http://fx")
    monkeypatch.setenv("AIRFLOW_API_URL", "http://airflow-apiserver:8080")
    return FakeClient


def test_reprocess_triggers_dag_with_range_then_waits_until_idle(fake_client, tmp_path, capsys):
    code = commands.main(["reprocess", "--from", "2026-07-01", "--to", "2026-07-03"])
    assert code == 0
    (client,) = fake_client.instances
    assert client.base_url == "http://airflow-apiserver:8080"
    assert client.triggered == [("reprocess", {"start": "2026-07-01", "end": "2026-07-03"})]
    assert client.waited["landing_dir"] == Path(tmp_path)
    assert "manual__42" in capsys.readouterr().out


def test_reprocess_rejects_inverted_range(fake_client):
    with pytest.raises(SystemExit):
        commands.main(["reprocess", "--from", "2026-07-03", "--to", "2026-07-01"])
    assert fake_client.instances == []


def test_reprocess_rejects_malformed_date(fake_client):
    with pytest.raises(SystemExit):
        commands.main(["reprocess", "--from", "03/07/2026", "--to", "2026-07-04"])


def test_monitor_triggers_monitoring_dag_then_waits_until_idle(fake_client, tmp_path, capsys):
    code = commands.main(["monitor"])
    assert code == 0
    (client,) = fake_client.instances
    assert client.triggered == [("monitoring", None)]
    assert client.waited["landing_dir"] == Path(tmp_path)
    assert "manual__42" in capsys.readouterr().out


def test_monitor_waits_for_pending_work_before_triggering(fake_client):
    """Regression (Task 17): `make demo` ran monitoring while the first ingest was still loading,
    so freshness saw empty raw tables and opened stale_data incidents for every source."""
    commands.main(["monitor"])
    (client,) = fake_client.instances
    assert client.calls == ["wait", "trigger monitoring", "run monitoring manual__42", "wait"]


def test_reprocess_waits_for_its_own_run_then_for_idle(fake_client):
    commands.main(["reprocess", "--from", "2026-07-01", "--to", "2026-07-01"])
    (client,) = fake_client.instances
    assert client.calls == ["trigger reprocess", "run reprocess manual__42", "wait"]


@pytest.mark.parametrize("state", ["failed", "upstream_failed"])
def test_reprocess_exits_non_zero_when_its_run_does_not_succeed(fake_client, capsys, state):
    """Review I3: a failed reprocess must not print "Pipeline idle" and exit 0."""
    fake_client.final_state = state
    code = commands.main(["reprocess", "--from", "2026-07-01", "--to", "2026-07-03"])
    assert code == 1
    out = capsys.readouterr().out
    assert f"reprocess run manual__42 ended {state}" in out
    assert "Pipeline idle" not in out


def test_monitor_exits_non_zero_when_monitoring_fails(fake_client, capsys):
    fake_client.final_state = "failed"
    assert commands.main(["monitor"]) == 1
    out = capsys.readouterr().out
    assert "monitoring run manual__42 ended failed" in out
    assert "Pipeline idle" not in out
