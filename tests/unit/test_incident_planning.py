from datetime import UTC, datetime

import pytest

from drct.monitoring.checks import TEST_TYPE_TO_INCIDENT, Check
from drct.monitoring.incidents import Incident, plan_incident_changes

SIM_NOW = datetime(2026, 7, 23, 6, 0, tzinfo=UTC)


def incident(**overrides) -> Incident:
    fields = {
        "id": 1,
        "type": "stale_data",
        "asset": "raw.payments",
        "severity": "critical",
        "status": "open",
        "first_seen": SIM_NOW,
        "last_seen": SIM_NOW,
    }
    return Incident(**{**fields, **overrides})


def test_failed_check_opens_then_updates():
    c = Check(
        "duplicate_records",
        "test.drct.unique_stg_payments_payment_id",
        False,
        "critical",
        "12 dupes",
    )
    assert plan_incident_changes([], [c]).to_open == [c]
    inc = incident(type=c.type, asset=c.asset)
    assert plan_incident_changes([inc], [c]).to_update == [(inc, c)]


def test_passing_check_resolves():
    inc = incident(type="stale_data", asset="raw.payments")
    changes = plan_incident_changes(
        [inc], [Check("stale_data", "raw.payments", True, "critical", "")]
    )
    assert changes.to_resolve == [inc]


def test_skipped_check_does_not_resolve():  # Review Focus 2
    inc = incident(type="broken_reference", asset="test.drct.rel_x")
    changes = plan_incident_changes(
        [inc], [Check("broken_reference", "test.drct.rel_x", None, "critical", "skipped")]
    )
    assert changes.to_resolve == [] and changes.to_update == [] and changes.to_open == []


def test_passing_check_without_open_incident_changes_nothing():
    changes = plan_incident_changes([], [Check("stale_data", "raw.sales", True, "critical", "")])
    assert changes.to_open == [] and changes.to_update == [] and changes.to_resolve == []


def test_identity_is_type_and_asset():
    inc = incident(type="stale_data", asset="raw.sales")
    other_type = Check("volume_anomaly", "raw.sales", False, "warning", "")
    other_asset = Check("stale_data", "raw.payments", False, "critical", "")
    assert plan_incident_changes([inc], [other_type, other_asset]).to_open == [
        other_type,
        other_asset,
    ]


@pytest.mark.parametrize(
    ("test_type", "incident_type"),
    [
        ("unique", "duplicate_records"),
        ("not_null", "missing_values"),
        ("relationships", "broken_reference"),
        ("expect_column_values_to_be_between", "out_of_range"),
        ("singular", "missing_dates"),
    ],
)
def test_test_type_mapping(test_type, incident_type):
    assert TEST_TYPE_TO_INCIDENT[test_type] == incident_type
    assert "accepted_values" not in TEST_TYPE_TO_INCIDENT  # anything else is test_failure


def test_duplicate_failed_checks_open_exactly_one_incident():
    a = Check("duplicate_records", "test.drct.x", False, "critical", "first")
    b = Check("duplicate_records", "test.drct.x", False, "critical", "second")
    assert len(plan_incident_changes([], [a, b]).to_open) == 1


def test_duplicate_failed_checks_update_exactly_once():
    inc = incident(type="duplicate_records", asset="test.drct.x")
    a = Check("duplicate_records", "test.drct.x", False, "critical", "first")
    b = Check("duplicate_records", "test.drct.x", False, "critical", "second")
    assert len(plan_incident_changes([inc], [a, b]).to_update) == 1


def test_failed_check_beats_passed_check_for_the_same_identity():
    fail = Check("duplicate_records", "test.drct.x", False, "critical", "dupes")
    ok = Check("duplicate_records", "test.drct.x", True, "critical", "")
    for checks in ([ok, fail], [fail, ok]):
        assert plan_incident_changes([], checks).to_open == [fail]
        inc = incident(type=fail.type, asset=fail.asset)
        changes = plan_incident_changes([inc], checks)
        assert changes.to_resolve == [] and changes.to_update == [(inc, fail)]


def test_critical_beats_warning_for_the_same_identity():
    warn = Check("out_of_range", "test.drct.y", False, "warning", "w")
    crit = Check("out_of_range", "test.drct.y", False, "critical", "c")
    assert plan_incident_changes([], [warn, crit]).to_open == [crit]
    assert plan_incident_changes([], [crit, warn]).to_open == [crit]


def test_no_signal_is_ignored_unless_every_check_has_none():
    none = Check("missing_values", "test.drct.z", None, "critical", "skipped")
    ok = Check("missing_values", "test.drct.z", True, "critical", "")
    inc = incident(type=none.type, asset=none.asset)
    assert plan_incident_changes([inc], [none, ok]).to_resolve == [inc]
    untouched = plan_incident_changes([inc], [none, none])
    assert untouched.to_resolve == [] and untouched.to_update == [] and untouched.to_open == []
