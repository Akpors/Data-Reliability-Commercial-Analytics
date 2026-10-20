-- Simulated clock: the single source of "now" for freshness, SLAs and incidents.
create table control.sim_clock (
    id          int primary key check (id = 1),
    day0        date,
    sim_now     timestamptz,
    advanced_at timestamptz
);

create table control.chaos_state (
    scenario   text primary key,
    params     jsonb,
    enabled_at timestamptz
);

create table control.chaos_affected (
    scenario      text,
    business_date date,
    primary key (scenario, business_date)
);

create table control.fx_sync (
    id        int primary key check (id = 1),
    watermark date
);

-- SECURITY DEFINER so the read-only Control Tower role can read the clock without
-- any privilege on control.sim_clock itself.
create function control.sim_now() returns timestamptz
language plpgsql
stable
security definer
set search_path = pg_catalog, pg_temp
as $$
declare
    v_now timestamptz;
begin
    select c.sim_now into v_now from control.sim_clock c where c.id = 1;
    if v_now is null then
        raise exception 'control.sim_clock is not initialised';
    end if;
    return v_now;
end;
$$;

revoke all on function control.sim_now() from public;
