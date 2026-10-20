import psycopg
import pytest


@pytest.mark.db
def test_all_schemas_exist(db_conn):
    names = {r[0] for r in db_conn.execute("select schema_name from information_schema.schemata")}
    assert {
        "raw",
        "quarantine",
        "staging",
        "intermediate",
        "marts",
        "monitoring",
        "control",
    } <= names


@pytest.mark.db
def test_sim_now_raises_until_clock_initialised(db_conn):
    with pytest.raises(psycopg.errors.RaiseException):
        db_conn.execute("select control.sim_now()")


@pytest.mark.db
def test_only_one_open_incident_per_type_and_asset(db_conn):
    ins = (
        "insert into monitoring.incidents(type, asset, severity, status, first_seen, last_seen, detail) "
        "values ('stale_data','raw.sales','critical','open', now(), now(), '')"
    )
    db_conn.execute(ins)
    with pytest.raises(psycopg.errors.UniqueViolation):
        db_conn.execute(ins)


@pytest.mark.db
def test_control_tower_role_cannot_read_control_schema(db_conn):
    db_conn.execute("set role control_tower_ro")
    with pytest.raises(psycopg.errors.InsufficientPrivilege):
        db_conn.execute("select * from control.chaos_state")


@pytest.mark.db
def test_control_tower_role_reads_monitoring_and_clock_only_via_function(db_conn):
    db_conn.execute(
        "insert into control.sim_clock values (1, '2026-01-01', '2026-04-01 06:00+00', now())"
    )
    db_conn.execute("set role control_tower_ro")
    assert db_conn.execute("select count(*) from monitoring.incidents").fetchone() == (0,)
    assert (
        db_conn.execute("select control.sim_now()").fetchone()[0].isoformat()
        == "2026-04-01T06:00:00+00:00"
    )
    with pytest.raises(psycopg.errors.InsufficientPrivilege):
        db_conn.execute("select * from control.sim_clock")
