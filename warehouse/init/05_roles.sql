-- Roles are cluster-wide, so creation must be idempotent. Grants are per-database and run every time.
-- The login password is applied from the environment by 06_role_password.sh (first warehouse start).
do $$
begin
    if not exists (select from pg_roles where rolname = 'control_tower_ro') then
        create role control_tower_ro login;
    end if;
end
$$;

grant usage on schema monitoring, marts to control_tower_ro;
grant select on all tables in schema monitoring, marts to control_tower_ro;
alter default privileges in schema monitoring grant select on tables to control_tower_ro;
alter default privileges in schema marts grant select on tables to control_tower_ro;

-- The control schema (chaos state, clock) stays hidden: schema usage and the SECURITY DEFINER clock function only.
grant usage on schema control to control_tower_ro;
grant execute on function control.sim_now() to control_tower_ro;
