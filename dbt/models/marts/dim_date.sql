-- Spine from day0 to the current sim date. The bounds are read from the sim clock at run time, so a
-- generate_series is used rather than dbt_utils.date_spine (which needs compile-time literals).
select
    d::date as date_day,
    extract(year from d)::int as year,
    extract(month from d)::int as month,
    extract(day from d)::int as day_of_month,
    extract(isodow from d)::int as iso_day_of_week,
    to_char(d, 'Dy') as day_name,
    extract(isodow from d) in (6, 7) as is_weekend
from generate_series(
    (select day0 from control.sim_clock where id = 1),
    (control.sim_now() at time zone 'UTC')::date,
    interval '1 day'
) as d
