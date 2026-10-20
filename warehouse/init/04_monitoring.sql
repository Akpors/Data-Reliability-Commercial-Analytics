create table monitoring.pipeline_runs (
    dag_id        text not null,
    run_id        text not null,
    task_id       text,
    run_type      text,
    status        text,
    try_number    int not null default 1,
    started_at    timestamptz,
    ended_at      timestamptz,
    duration_s    double precision,
    business_date date,
    rows_in       bigint,
    rows_rejected bigint,
    sim_now       timestamptz
);

create unique index pipeline_runs_identity_uq
    on monitoring.pipeline_runs (dag_id, run_id, coalesce(task_id, ''), try_number);

create table monitoring.schema_events (
    id               bigserial primary key,
    source           text not null,
    file_name        text,
    contract_version int,
    added            text[],
    missing          text[],
    type_mismatches  jsonb,
    aliased          jsonb,
    is_breaking      boolean not null default false,
    sim_now          timestamptz,
    recorded_at      timestamptz not null default now()
);

create table monitoring.test_results (
    invocation_id text,
    unique_id     text,
    test_name     text,
    test_type     text,
    attached_node text,
    status        text,
    severity      text,
    failures      bigint,
    message       text,
    compiled_sql  text,
    sim_now       timestamptz,
    generated_at  timestamptz
);

create table monitoring.freshness (
    source         text,
    max_loaded_at  timestamptz,
    snapshotted_at timestamptz,
    age_seconds    double precision,
    status         text,
    checked_at     timestamptz
);

create table monitoring.volume_metrics (
    source        text,
    business_date date,
    row_count     bigint,
    computed_at   timestamptz,
    primary key (source, business_date)
);

create table monitoring.volume_anomalies (
    source          text,
    business_date   date,
    row_count       bigint,
    baseline_median double precision,
    mad             double precision,
    robust_z        double precision,
    is_anomaly      boolean,
    reason          text,
    sim_now         timestamptz,
    evaluated_at    timestamptz
);

create table monitoring.sla_results (
    rule         text,
    status       text,
    expected     text,
    actual       text,
    detail       text,
    sim_now      timestamptz,
    evaluated_at timestamptz
);

create table monitoring.incidents (
    id                    serial primary key,
    type                  text not null,
    asset                 text not null,
    severity              text not null,
    status                text not null check (status in ('open', 'resolved')),
    first_seen            timestamptz not null,
    last_seen             timestamptz not null,
    resolved_at           timestamptz,
    detail                text,
    affected_downstream   text[]
);

create unique index incidents_one_open_per_identity
    on monitoring.incidents (type, asset) where status = 'open';
