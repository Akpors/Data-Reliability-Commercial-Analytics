import pytest

from drct.chaos.scenarios import SCENARIOS
from drct.ops import verify
from drct.ops.verify import EXPECTATIONS, ScenarioExpectation, ScenarioOutcome, format_table


def test_every_scenario_has_an_expectation():
    assert {e.scenario for e in EXPECTATIONS} == set(SCENARIOS)


def test_resend_fix_used_for_data_corruption():
    fixes = {e.scenario: e.fix for e in EXPECTATIONS}
    assert all(
        fixes[s] == "resend"
        for s in ("duplicates", "missing_dates", "broken_fk", "negative_revenue", "volume_spike")
    )


def test_stale_data_needs_a_second_missed_day():
    extra = {e.scenario: e.extra_days for e in EXPECTATIONS}
    assert extra["stale_data"] == 1
    assert all(days == 0 for scenario, days in extra.items() if scenario != "stale_data")


def test_table_has_one_row_per_outcome_and_a_pass_count():
    outcomes = [
        ScenarioOutcome("duplicates", True, True, 61.2, "companions: pipeline_failure"),
        ScenarioOutcome("api_flaky", True, False, 300.0, "still open: sla_breach"),
    ]
    lines = format_table(outcomes).splitlines()
    assert any("duplicates" in line and "detected=True resolved=True" in line for line in lines)
    assert any("api_flaky" in line and "resolved=False" in line for line in lines)
    assert lines[-1] == "1/2 scenarios detected and resolved"


class FakeAirflow:
    """Every triggered run ends in the state given for its DAG (default success)."""

    def __init__(self, states: dict[str, str]) -> None:
        self.states = states
        self.runs: dict[str, str] = {}
        self.triggered: list[str] = []

    def trigger_dag(self, dag_id: str, conf: dict | None = None) -> str:
        run_id = f"manual__{len(self.triggered)}"
        self.triggered.append(dag_id)
        self.runs[run_id] = dag_id
        return run_id

    def get_dag_run_state(self, dag_id: str, run_id: str) -> str:
        assert self.runs[run_id] == dag_id
        return self.states.get(dag_id, "success")

    def wait_for_run(self, dag_id: str, run_id: str, **kwargs) -> str:
        return self.get_dag_run_state(dag_id, run_id)  # every fake run is already terminal

    def wait_until_idle(self, **kwargs) -> None:
        pass


@pytest.fixture
def no_db(monkeypatch, tmp_path, request):
    """Stub the warehouse side: chaos switches and day writes are no-ops; open incident types
    follow the healthy lifecycle (none, then the expected type, then none again)."""
    expected = getattr(request, "param", None)
    open_sequence = iter([[], [expected] if expected else [], []])
    monkeypatch.setenv("LANDING_DIR", str(tmp_path))
    monkeypatch.setattr(verify, "open_incident_types", lambda conn: next(open_sequence, []))
    monkeypatch.setattr(verify, "enable", lambda conn, scenario: None)
    monkeypatch.setattr(verify, "chaos_reset", lambda conn, settings: [])
    monkeypatch.setattr(verify, "simulate_day", lambda conn, settings: None)


def _exp(scenario: str) -> ScenarioExpectation:
    return next(e for e in EXPECTATIONS if e.scenario == scenario)


@pytest.mark.parametrize(
    ("no_db", "scenario"),
    [("sla_breach", "api_flaky"), ("stale_data", "stale_data")],
    indirect=["no_db"],
)
def test_a_failed_ingest_fx_run_fails_the_scenario(no_db, scenario):
    """Even with the expected incident opening and resolving, a failed FX run fails it."""
    client = FakeAirflow({"ingest_fx": "failed"})
    outcome = verify.run_scenario(_exp(scenario), client, conn=None)
    assert not outcome.passed
    assert "ingest_fx run manual__0 ended failed" in outcome.detail


def test_next_run_fix_requires_the_fx_run_to_succeed(no_db):
    client = FakeAirflow({})
    verify._apply_fix(_exp("api_flaky"), client, None, verify.get_settings())
    assert client.triggered == ["ingest_fx"]
    client.states["ingest_fx"] = "failed"
    with pytest.raises(verify.ScenarioFailed, match="ingest_fx run .* ended failed"):
        verify._apply_fix(_exp("stale_data"), client, None, verify.get_settings())
